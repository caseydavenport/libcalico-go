// Copyright (c) 2016 Tigera, Inc. All rights reserved.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package k8s

import (
	goerrors "errors"
	"fmt"
	"net"
	"strings"

	"crypto/sha1"
	"encoding/hex"
	"encoding/json"

	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cnet "github.com/projectcalico/libcalico-go/lib/net"
	"github.com/projectcalico/libcalico-go/lib/numorstring"
	k8sapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/util/intstr"
)

var (
	policyAnnotation = "net.beta.kubernetes.io/network-policy"
)

type namespacePolicy struct {
	Ingress struct {
		Isolation string `json:"isolation"`
	} `json:"ingress"`
}

type converter struct {
}

// parseWorkloadID extracts the Namespace and Pod name from the given workload ID.
func (c converter) parseWorkloadID(workloadID string) (string, string) {
	splits := strings.SplitN(workloadID, ".", 2)
	return splits[0], splits[1]
}

// parsePolicyName extracts the Namespace and NetworkPolicy name from the given Policy name.
func (c converter) parsePolicyName(name string) (string, string) {
	splits := strings.SplitN(name, ".", 2)
	if len(splits) != 2 {
		return "", ""
	}
	return splits[0], splits[1]
}

// parseProfileName extracts the Namespace name from the given Profile name.
func (c converter) parseProfileName(profileName string) (string, error) {
	splits := strings.SplitN(profileName, ".", 2)
	if len(splits) != 2 {
		return "", goerrors.New(fmt.Sprintf("Invalid profile name: %s", profileName))
	}
	return splits[1], nil
}

func (c converter) namespaceToPool(ns *k8sapi.Namespace) (*model.KVPair, error) {
	if ns.ObjectMeta.Name != "kube-system" {
		return nil, goerrors.New("Invalid namespace, must be kube-system")
	}

	// Get the serialized KVPair.
	poolStr, ok := ns.ObjectMeta.Annotations["projectcalico.org/ipPool"]
	if !ok {
		// No pools exist.
		return nil, goerrors.New("No Kubernetes Pod IP Pool configured")
	}
	pool := model.Pool{}
	err := json.Unmarshal([]byte(poolStr), &pool)
	if err != nil {
		return nil, err
	}
	return &model.KVPair{
		Key:   model.PoolKey{CIDR: pool.CIDR},
		Value: pool,
	}, nil
}

func (c converter) namespaceToProfile(ns *k8sapi.Namespace) (*model.KVPair, error) {
	// Determine the ingress action based off the DefaultDeny annotation.
	ingressAction := "allow"
	for k, v := range ns.ObjectMeta.Annotations {
		if k == policyAnnotation {
			np := namespacePolicy{}
			if err := json.Unmarshal([]byte(v), &np); err != nil {
				return nil, err
			}
			if np.Ingress.Isolation == "DefaultDeny" {
				ingressAction = "deny"
			}
		}
	}

	// Generate the labels to apply to the profile.
	labels := map[string]string{}
	for k, v := range ns.ObjectMeta.Labels {
		labels[fmt.Sprintf("k8s_ns/label/%s", k)] = v
	}

	name := fmt.Sprintf("k8s_ns.%s", ns.ObjectMeta.Name)
	kvp := model.KVPair{
		Key: model.ProfileKey{Name: name},
		Value: model.Profile{
			Rules: model.ProfileRules{
				InboundRules:  []model.Rule{model.Rule{Action: ingressAction}},
				OutboundRules: []model.Rule{model.Rule{Action: "allow"}},
			},
			Tags:   []string{name},
			Labels: labels,
		},
		Revision: ns.ObjectMeta.ResourceVersion,
	}
	return &kvp, nil
}

func (c converter) podToWorkloadEndpoint(pod *k8sapi.Pod) (*model.KVPair, error) {
	// If the pod is in host networking, we want nothing to do with it.
	// Return nil to indicate there is no corresponding workload endpoint.
	if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.HostNetwork == true {
		return nil, nil
	}

	// Pull out the profile and workload ID based on pod name and Namespace.
	profile := fmt.Sprintf("k8s_ns.%s", pod.ObjectMeta.Namespace)
	workload := fmt.Sprintf("%s.%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)

	// Parse the pod's IP address, MAC.
	ipNets := []cnet.IPNet{}
	_, ipNet, err := cnet.ParseCIDR(pod.Status.PodIP)
	if err == nil {
		// We parsed the IP address - add it to the ipNets slice.
		// If parsing failed, then this pod likely hasn't been
		// scheduled yet.
		ipNets = append(ipNets, *ipNet)
	}
	// TODO: Get the real mac from somewhere?
	mac, err := net.ParseMAC("ff:ff:ff:ff:ff:ff")
	if err != nil {
		return nil, err
	}

	// Generate the interface name based on identifiers.
	h := sha1.New()
	h.Write([]byte(workload))
	interfaceName := fmt.Sprintf("cali%s", hex.EncodeToString(h.Sum(nil))[0:12])

	// Create the key / value pair to return.
	kvp := model.KVPair{
		Key: model.WorkloadEndpointKey{
			Hostname:       pod.Spec.NodeName, // TODO: Is this always set?
			OrchestratorID: "k8s",
			WorkloadID:     workload,
			EndpointID:     "eth0",
		},
		Value: model.WorkloadEndpoint{
			State:      "active",
			Name:       interfaceName,
			Mac:        cnet.MAC{HardwareAddr: mac},
			ProfileIDs: []string{profile},
			IPv4Nets:   ipNets,
			IPv6Nets:   []cnet.IPNet{},
			Labels:     pod.ObjectMeta.Labels,
		},
		Revision: pod.ObjectMeta.ResourceVersion,
	}
	return &kvp, nil
}

// networkPolicyToPolicy converts a k8s NetworkPolicy to a model.KVPair.
func (c converter) networkPolicyToPolicy(np *extensions.NetworkPolicy) (*model.KVPair, error) {
	// Parse out important fields.
	policyName := fmt.Sprintf("%s.%s", np.ObjectMeta.Namespace, np.ObjectMeta.Name)
	order := float64(1000.0)

	// Generate the inbound rules list.
	inboundRules := []model.Rule{}
	for _, r := range np.Spec.Ingress {
		inboundRules = append(inboundRules, c.parseIngressRule(r, np.ObjectMeta.Namespace)...)
	}

	// Build and return the KVPair.
	return &model.KVPair{
		Key: model.PolicyKey{
			Name: policyName,
			Tier: "k8s-network-policy",
		},
		Value: model.Policy{
			Order:         &order,
			Selector:      c.parseSelector(&np.Spec.PodSelector, &np.ObjectMeta.Namespace),
			InboundRules:  inboundRules,
			OutboundRules: []model.Rule{},
		},
		Revision: np.ObjectMeta.ResourceVersion,
	}, nil
}

// parseSelector takes a namespaced k8s label selector and returns the Calico
// equivalent.
func (c converter) parseSelector(s *unversioned.LabelSelector, ns *string) string {
	// If this is a podSelector, it needs to be namespaced, and it
	// uses a different prefix.  Otherwise, treat this as a NamespaceSelector.
	selectors := []string{}
	prefix := "k8s_ns/label/"
	if ns != nil {
		prefix = ""
		selectors = append(selectors, fmt.Sprintf("calico/k8s_ns == '%s'", *ns))
	}

	// matchLabels is a map key => value, it means match if (label[key] ==
	// value) for all keys.
	for k, v := range s.MatchLabels {
		selectors = append(selectors, fmt.Sprintf("%s%s == '%s'", prefix, k, v))
	}

	// matchExpressions is a list of in/notin/exists/doesnotexist tests.
	for _, e := range s.MatchExpressions {
		valueList := strings.Join(e.Values, ", ")

		// Each selector is formatted differently based on the operator.
		switch e.Operator {
		case unversioned.LabelSelectorOpIn:
			selectors = append(selectors, fmt.Sprintf("%s%s in { %s }", prefix, e.Key, valueList))
		case unversioned.LabelSelectorOpNotIn:
			selectors = append(selectors, fmt.Sprintf("%s%s no int { %s }", prefix, e.Key, valueList))
		case unversioned.LabelSelectorOpExists:
			selectors = append(selectors, fmt.Sprintf("has(%s%s)", prefix, e.Key))
		case unversioned.LabelSelectorOpDoesNotExist:
			selectors = append(selectors, fmt.Sprintf("! has(%s%s)", prefix, e.Key))
		}
	}

	return strings.Join(selectors, " && ")
}

func (c converter) parseIngressRule(r extensions.NetworkPolicyIngressRule, ns string) []model.Rule {
	rules := []model.Rule{}
	peers := []*extensions.NetworkPolicyPeer{}
	ports := []*extensions.NetworkPolicyPort{}

	// Built up a list of the sources and a list of the desintations.
	for _, f := range r.From {
		peers = append(peers, &f)
	}
	for _, p := range r.Ports {
		ports = append(ports, &p)
	}

	// If there no peers, or no ports, represent that as nil.
	if len(peers) == 0 {
		peers = []*extensions.NetworkPolicyPeer{nil}
	}
	if len(ports) == 0 {
		ports = []*extensions.NetworkPolicyPort{nil}
	}

	// Combine desintations with sources to generate rules.
	for _, port := range ports {
		for _, peer := range peers {
			// Build rule and append to list.
			rules = append(rules, c.buildRule(port, peer, ns))
		}
	}
	return rules
}

func (c converter) buildRule(port *extensions.NetworkPolicyPort, peer *extensions.NetworkPolicyPeer, ns string) model.Rule {
	var protocol *numorstring.Protocol
	dstPorts := []numorstring.Port{}
	srcSelector := ""
	if port != nil {
		// Port information available.
		protocol = c.parseProtocol(port.Protocol)
		dstPorts = c.parsePolicyPort(*port)
	}
	if peer != nil {
		// Peer information available.
		srcSelector = c.parsePolicyPeer(*peer, ns)
	}

	// Build the rule.
	return model.Rule{
		Action:      "allow",
		Protocol:    protocol,
		SrcSelector: srcSelector,
		DstPorts:    dstPorts,
	}
}

func (c converter) parseProtocol(protocol *k8sapi.Protocol) *numorstring.Protocol {
	if protocol != nil {
		p := numorstring.ProtocolFromString(strings.ToLower(string(*protocol)))
		return &p
	}
	return nil
}

func (c converter) parsePolicyPeer(peer extensions.NetworkPolicyPeer, ns string) string {
	// Determine the source selector for the rule.
	// Only one of PodSelector / NamespaceSelector can be defined.
	if peer.PodSelector != nil {
		return c.parseSelector(peer.PodSelector, &ns)
	}
	if peer.NamespaceSelector != nil {
		return c.parseSelector(peer.NamespaceSelector, nil)
	}

	// Neither is defined - return an empty selector.
	return ""
}

func (c converter) parsePolicyPort(port extensions.NetworkPolicyPort) []numorstring.Port {
	if port.Port != nil && port.Port.Type == intstr.Int {
		return []numorstring.Port{numorstring.PortFromInt(port.Port.IntVal)}
	} else if port.Port != nil && port.Port.Type == intstr.String {
		return []numorstring.Port{numorstring.PortFromString(port.Port.StrVal)}
	}

	// No ports - return empty list.
	return []numorstring.Port{}
}
