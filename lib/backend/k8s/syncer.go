// Copyright (c) 2016 Tigera, Inc. All rights reserved.
//
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

	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	k8sapi "k8s.io/kubernetes/pkg/api"
)

func newSyncer(kc KubeClient, callbacks api.SyncerCallbacks) *kubeSyncer {
	return &kubeSyncer{
		kc:        kc,
		callbacks: callbacks,
	}
}

type kubeSyncer struct {
	kc        KubeClient
	callbacks api.SyncerCallbacks
	OneShot   bool
}

func (syn *kubeSyncer) Start() {
	c := make(chan model.KVPair)
	go syn.watchNamespaces(c)
	for x := range c {
		fmt.Printf("Recieved KVPAIR: %+v", x)
	}
}

func (syn *kubeSyncer) watchNamespaces(c chan model.KVPair) {
	nsWatch, err := syn.kc.clientSet.Namespaces().Watch(k8sapi.ListOptions{})
	if err != nil {
		panic(err)
	}
	resultChan := nsWatch.ResultChan()
	for e := range resultChan {
		fmt.Printf("Recieved update.  Type=%s\n", e.Type)
		ns, ok := e.Object.(*k8sapi.Namespace)
		if !ok {
			panic(ok)
		}
		fmt.Printf("Received update.  Object: %+v\n", *ns)
		c <- model.KVPair{
			Key: model.PolicyKey{
				Name: ns.ObjectMeta.Name,
			},
			Value: model.Policy{},
		}
	}
}
