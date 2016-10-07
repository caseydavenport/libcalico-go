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
	"fmt"
	"net"

	"encoding/json"

	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cnet "github.com/projectcalico/libcalico-go/lib/net"
	"k8s.io/kubernetes/pkg/api/v1"
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

func (c converter) namespaceToProfile(ns *v1.Namespace) (*model.KVPair, error) {
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

	name := fmt.Sprintf("k8s_ns.%s", ns.ObjectMeta.Name)
	kvp := model.KVPair{
		Key: model.ProfileKey{Name: name},
		Value: model.Profile{
			Rules: model.ProfileRules{
				InboundRules:  []model.Rule{model.Rule{Action: ingressAction}},
				OutboundRules: []model.Rule{model.Rule{Action: "allow"}},
			},
			Tags:   []string{name},
			Labels: map[string]string{},
		},
	}
	return &kvp, nil
}

func (c converter) podToWorkloadEndpoint(pod *v1.Pod) (*model.KVPair, error) {
	// If the pod is in host networking, we want nothing to do with it.
	// Return nil to indicate there is no corresponding workload endpoint.
	if pod.Spec.HostNetwork == true {
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
			Name:       "eth0",
			Mac:        cnet.MAC{HardwareAddr: mac},
			ProfileIDs: []string{profile},
			IPv4Nets:   ipNets,
			IPv6Nets:   []cnet.IPNet{},
			Labels:     pod.ObjectMeta.Labels,
		},
	}
	return &kvp, nil
}
