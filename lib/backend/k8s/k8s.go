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
	"strings"

	// log "github.com/Sirupsen/logrus"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cnet "github.com/projectcalico/libcalico-go/lib/net"
	k8sapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/v1"
	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_4"
	"k8s.io/kubernetes/pkg/client/unversioned/clientcmd"
	// "github.com/projectcalico/libcalico-go/lib/errors"
)

type KubeClient struct {
	clientSet *clientset.Clientset
	converter converter
}

type KubeConfig struct {
}

func NewKubeClient(kc *KubeConfig) (*KubeClient, error) {

	// Use the kubernetes client code to load the kubeconfig file and combine it with the overrides.
	// TODO: This needs to be configurable, not just using my hardcoded kubeconfig file.
	configOverrides := &clientcmd.ConfigOverrides{}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: "/Users/casey/.kube/config"},
		configOverrides).ClientConfig()
	if err != nil {
		return nil, err
	}
	//logger.Debugf("Kubernetes config %v", config)

	// Create the clientset
	cs, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &KubeClient{clientSet: cs}, nil
}

func (c *KubeClient) Syncer(callbacks api.SyncerCallbacks) api.Syncer {
	return newSyncer(*c, callbacks)
}

// Create an entry in the datastore.  This errors if the entry already exists.
func (c *KubeClient) Create(d *model.KVPair) (*model.KVPair, error) {
	return nil, goerrors.New("Create is not supported for Kubernetes backend")
}

// Update an existing entry in the datastore.  This errors if the entry does
// not exist.
func (c *KubeClient) Update(d *model.KVPair) (*model.KVPair, error) {
	return nil, nil
}

// Set an existing entry in the datastore.  This ignores whether an entry already
// exists.
func (c *KubeClient) Apply(d *model.KVPair) (*model.KVPair, error) {
	return nil, nil
}

// Delete an entry in the datastore.  This errors if the entry does not exists.
func (c *KubeClient) Delete(d *model.KVPair) error {
	return goerrors.New("Delete is not supported for the Kubernetes backend")
}

// Get an entry from the datastore.  This errors if the entry does not exist.
func (c *KubeClient) Get(k model.Key) (*model.KVPair, error) {
	switch k.(type) {
	case model.ProfileKey:
		return c.getProfile(k.(model.ProfileKey))
	case model.WorkloadEndpointKey:
		return c.getWorkloadEndpoint(k.(model.WorkloadEndpointKey))
	default:
		return nil, goerrors.New("Kubernetes backend does not support 'get' for this type")
	}
}

// List entries in the datastore.  This may return an empty list of there are
// no entries matching the request in the ListInterface.
func (c *KubeClient) List(l model.ListInterface) ([]*model.KVPair, error) {
	switch l.(type) {
	case model.ProfileListOptions:
		return c.listProfiles(l.(model.ProfileListOptions))
	case model.WorkloadEndpointListOptions:
		return c.listWorkloadEndpoints(l.(model.WorkloadEndpointListOptions))
	case model.PoolListOptions:
		return c.listPools(l.(model.PoolListOptions))
	default:
		return nil, goerrors.New("Kubernetes backend does not support 'list' for this type")
	}
}

// listProfiles lists Profiles from the k8s API based on existing Namespaces.
func (c *KubeClient) listProfiles(l model.ProfileListOptions) ([]*model.KVPair, error) {
	// If a name is specified, then do an exact lookup.
	if l.Name != "" {
		kvp, err := c.getProfile(model.ProfileKey{Name: l.Name})
		if err != nil {
			return nil, err
		}
		return []*model.KVPair{kvp}, nil
	}

	// Otherwise, enumerate all.
	namespaces, err := c.clientSet.Namespaces().List(k8sapi.ListOptions{})
	if err != nil {
		return nil, err
	}

	// For each Namespace, return a profile.
	ret := []*model.KVPair{}
	for _, ns := range namespaces.Items {
		kvp, err := c.converter.namespaceToProfile(&ns)
		if err != nil {
			return nil, err
		}
		ret = append(ret, kvp)
	}
	return ret, nil
}

// getProfile gets the Profile from the k8s API based on existing Namespaces.
func (c *KubeClient) getProfile(k model.ProfileKey) (*model.KVPair, error) {
	namespace, err := c.clientSet.Namespaces().Get(k.Name)
	if err != nil {
		return nil, err
	}

	return c.converter.namespaceToProfile(namespace)
}

// listWorkloadEndpoints lists WorkloadEndpoints from the k8s API based on existing Pods.
func (c *KubeClient) listWorkloadEndpoints(l model.WorkloadEndpointListOptions) ([]*model.KVPair, error) {
	// Enumerate all pods in all namespaces.
	// TODO: Is this the best way to get all pods?
	namespaces, err := c.clientSet.Namespaces().List(k8sapi.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods := []v1.Pod{}
	for _, ns := range namespaces.Items {
		nsPods, err := c.clientSet.Pods(ns.Name).List(k8sapi.ListOptions{})
		if err != nil {
			return nil, err
		}
		pods = append(pods, nsPods.Items...)
	}

	// For each Pod, return a workload endpoint.
	ret := []*model.KVPair{}
	for _, pod := range pods {
		kvp, err := c.converter.podToWorkloadEndpoint(&pod)
		if err != nil {
			return nil, err
		}

		// Some Pods are invalid - e.g those with hostNetwork.
		// The conversion func returns these as nil, so just
		// skip these.
		if kvp != nil {
			ret = append(ret, kvp)
		}
	}
	return ret, nil
}

// getWorkloadEndpoint gets the WorkloadEndpoint from the k8s API based on existing Pods.
func (c *KubeClient) getWorkloadEndpoint(k model.WorkloadEndpointKey) (*model.KVPair, error) {
	// The workloadID is of the form namespace.podname.  Parse it so we
	// can find the correct namespace to get the pod.
	splits := strings.SplitN(k.WorkloadID, ".", 2)
	namespace := splits[0]
	podName := splits[1]

	pod, err := c.clientSet.Pods(namespace).Get(podName)
	if err != nil {
		return nil, err
	} else if pod == nil {
		return nil, goerrors.New("Pod uses hostNetwork: true")
	}
	return c.converter.podToWorkloadEndpoint(pod)
}

// listPools lists Pools thus the k8s API based on TODO.
func (c *KubeClient) listPools(l model.PoolListOptions) ([]*model.KVPair, error) {
	_, poolCidr, err := cnet.ParseCIDR("192.168.0.0/24")
	if err != nil {
		return nil, err
	}
	kvp, err := c.getPool(model.PoolKey{CIDR: *poolCidr})
	if err != nil {
		return nil, err
	}
	return []*model.KVPair{kvp}, nil
}

// getPool gets the Pool from the k8s API based on TODO.
// TODO: Allow configuration of pools - this currently just returns
// the pool that was asked for, no matter what.
func (c *KubeClient) getPool(k model.PoolKey) (*model.KVPair, error) {
	return &model.KVPair{
		Key: k,
		Value: model.Pool{
			CIDR:          k.CIDR,
			IPIPInterface: "",
			Masquerade:    true,
			IPAM:          true,
			Disabled:      false,
		},
	}, nil
}
