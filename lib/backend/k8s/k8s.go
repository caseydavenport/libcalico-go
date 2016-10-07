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
	// "reflect"
	// "strings"
	"fmt"

	// "time"

	"encoding/json"

	// log "github.com/Sirupsen/logrus"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	k8sapi "k8s.io/kubernetes/pkg/api"
	clientset "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_4"
	"k8s.io/kubernetes/pkg/client/unversioned/clientcmd"
	// "github.com/projectcalico/libcalico-go/lib/errors"
	// "golang.org/x/net/context"
)

var (
	policyAnnotation = "net.beta.kubernetes.io/network-policy"
)

type namespacePolicy struct {
	Ingress struct {
		Isolation string `json:"isolation"`
	} `json:"ingress"`
}

type KubeClient struct {
	clientSet *clientset.Clientset
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
	return nil, nil
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
	return nil
}

// Get an entry from the datastore.  This errors if the entry does not exist.
func (c *KubeClient) Get(k model.Key) (*model.KVPair, error) {
	return nil, nil
}

// List entries in the datastore.  This may return an empty list of there are
// no entries matching the request in the ListInterface.
func (c *KubeClient) List(l model.ListInterface) ([]*model.KVPair, error) {
	switch l.(type) {
	case model.ProfileListOptions:
		return c.listProfiles(l)
	default:
		return nil, goerrors.New("Kubernetes backend does not support listing this type")
	}
}

// listProfiles lists Profiles from the k8s API based on existing Namespaces.
func (c *KubeClient) listProfiles(l model.ListInterface) ([]*model.KVPair, error) {
	namespaces, err := c.clientSet.Namespaces().List(k8sapi.ListOptions{})
	if err != nil {
		return nil, err
	}

	// For each Namespace, return a profile.
	ret := []*model.KVPair{}
	for _, ns := range namespaces.Items {
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
		ret = append(ret, &kvp)
	}
	return ret, nil
}
