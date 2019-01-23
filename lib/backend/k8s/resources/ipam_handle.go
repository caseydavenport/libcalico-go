// Copyright (c) 2016-2017 Tigera, Inc. All rights reserved.

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

package resources

import (
	"context"
	"reflect"
	"strings"

	apiv3 "github.com/projectcalico/libcalico-go/lib/apis/v3"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cerrors "github.com/projectcalico/libcalico-go/lib/errors"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	IPAMHandleResourceName = "IPAMHandles"
	IPAMHandleCRDName      = "ipamhandles.crd.projectcalico.org"
)

func NewIPAMHandleClient(c *kubernetes.Clientset, r *rest.RESTClient) K8sResourceClient {
	// Create a resource client which manages k8s CRDs.
	rc := customK8sResourceClient{
		clientSet:       c,
		restClient:      r,
		name:            IPAMHandleCRDName,
		resource:        IPAMHandleResourceName,
		description:     "Calico IPAM handles",
		k8sResourceType: reflect.TypeOf(apiv3.IPAMHandle{}),
		k8sResourceTypeMeta: metav1.TypeMeta{
			Kind:       apiv3.KindIPAMHandle,
			APIVersion: apiv3.GroupVersionCurrent,
		},
		k8sListType:  reflect.TypeOf(apiv3.IPAMHandleList{}),
		resourceKind: apiv3.KindIPAMHandle,
	}

	return &ipamHandleClient{rc: rc}
}

// Implements the api.Client interface for IPAMHandles.
type ipamHandleClient struct {
	rc customK8sResourceClient
}

func (c ipamHandleClient) toV1(kvpv3 *model.KVPair) *model.KVPair {
	handle := kvpv3.Value.(*apiv3.IPAMHandle).Spec.HandleID
	block := kvpv3.Value.(*apiv3.IPAMHandle).Spec.Block
	return &model.KVPair{
		Key: model.IPAMHandleKey{
			HandleID: handle,
		},
		Value: &model.IPAMHandle{
			HandleID: handle,
			Block:    block,
		},
		Revision: kvpv3.Revision,
	}
}

func (c ipamHandleClient) v3Fields(k model.Key) string {
	return strings.ToLower(k.(model.IPAMHandleKey).HandleID)
}

func (c ipamHandleClient) toV3(kvpv1 *model.KVPair) *model.KVPair {
	name := c.v3Fields(kvpv1.Key)
	handle := kvpv1.Key.(model.IPAMHandleKey).HandleID
	block := kvpv1.Value.(*model.IPAMHandle).Block
	return &model.KVPair{
		Key: model.ResourceKey{
			Name: name,
			Kind: apiv3.KindIPAMHandle,
		},
		Value: &apiv3.IPAMHandle{
			TypeMeta: metav1.TypeMeta{
				Kind:       apiv3.KindIPAMHandle,
				APIVersion: "crd.projectcalico.org/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				ResourceVersion: kvpv1.Revision,
			},
			Spec: apiv3.IPAMHandleSpec{
				HandleID: handle,
				Block:    block,
			},
		},
		Revision: kvpv1.Revision,
	}
}

func (c *ipamHandleClient) Create(ctx context.Context, kvp *model.KVPair) (*model.KVPair, error) {
	nkvp := c.toV3(kvp)
	kvp, err := c.rc.Create(ctx, nkvp)
	if err != nil {
		return nil, err
	}
	return c.toV1(kvp), nil
}

func (c *ipamHandleClient) Update(ctx context.Context, kvp *model.KVPair) (*model.KVPair, error) {
	nkvp := c.toV3(kvp)
	kvp, err := c.rc.Update(ctx, nkvp)
	if err != nil {
		return nil, err
	}
	return c.toV1(kvp), nil
}

func (c *ipamHandleClient) Delete(ctx context.Context, key model.Key, revision string) (*model.KVPair, error) {
	name := c.v3Fields(key)
	k := model.ResourceKey{
		Name: name,
		Kind: apiv3.KindIPAMHandle,
	}
	kvp, err := c.rc.Delete(ctx, k, revision)
	if err != nil {
		return nil, err
	}
	return c.toV1(kvp), nil
}

func (c *ipamHandleClient) Get(ctx context.Context, key model.Key, revision string) (*model.KVPair, error) {
	name := c.v3Fields(key)
	k := model.ResourceKey{
		Name: name,
		Kind: apiv3.KindIPAMHandle,
	}
	kvp, err := c.rc.Get(ctx, k, revision)
	if err != nil {
		return nil, err
	}
	return c.toV1(kvp), nil
}

func (c *ipamHandleClient) List(ctx context.Context, list model.ListInterface, revision string) (*model.KVPairList, error) {
	l := model.ResourceListOptions{Kind: apiv3.KindIPAMHandle}
	v3list, err := c.rc.List(ctx, l, revision)
	if err != nil {
		return nil, err
	}

	kvpl := &model.KVPairList{KVPairs: []*model.KVPair{}}
	for _, i := range v3list.KVPairs {
		v1kvp := c.toV1(i)
		kvpl.KVPairs = append(kvpl.KVPairs, v1kvp)
	}
	return kvpl, nil
}

func (c *ipamHandleClient) Watch(ctx context.Context, list model.ListInterface, revision string) (api.WatchInterface, error) {
	log.Warn("Operation Watch is not supported on IPAMHandle type")
	return nil, cerrors.ErrorOperationNotSupported{
		Identifier: list,
		Operation:  "Watch",
	}
}

func (c *ipamHandleClient) EnsureInitialized() error {
	return nil
}