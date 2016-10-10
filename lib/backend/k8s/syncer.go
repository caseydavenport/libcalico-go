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
	log "github.com/Sirupsen/logrus"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	k8sapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/watch"
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

// Holds resource version information.
type resourceVersions struct {
	podVersion           string
	namespaceVersion     string
	networkPolicyVersion string
}

func (syn *kubeSyncer) Start() {
	// Channel for receiving updates from a snapshot.
	snapshotUpdates := make(chan *model.KVPair)

	// Channel for receiving updates from the watcher.
	watchUpdates := make(chan *model.KVPair)

	// Channel used by the watcher to trigger a re-sync.
	triggerResync := make(chan *resourceVersions, 5)

	// Channel to send the index from which to start the snapshot.
	initialSnapshotIndex := make(chan *resourceVersions)

	// If we're not in one-shot mode, start the API watcher to
	// gather updates.
	if !syn.OneShot {
		go syn.watchKubeAPI(watchUpdates, triggerResync, initialSnapshotIndex)
	}

	// Start a background thread to read snapshots from etcd.  It will
	// read a start-of-day snapshot and then wait to be signalled on the
	// resyncIndex channel.
	go syn.readSnapshot(snapshotUpdates, triggerResync, initialSnapshotIndex)

	// Start a routine to merge updates from the snapshot routine and the
	// watch routine (if running), and pass information to callbacks.
	go syn.mergeUpdates(snapshotUpdates, watchUpdates)
}

// TODO: Make this smarter!
func (syn *kubeSyncer) mergeUpdates(snapshotUpdates, watchUpdates chan *model.KVPair) {

	syn.callbacks.OnStatusUpdated(api.WaitForDatastore)
	var sUpdate, wUpdate *model.KVPair
	for {
		select {
		case sUpdate = <-snapshotUpdates:
			log.Debugf("Snapshot update: %+v", sUpdate)
			syn.callbacks.OnUpdates([]model.KVPair{*sUpdate})
		case wUpdate = <-watchUpdates:
			log.Debugf("Watch update: %+v", sUpdate)
			syn.callbacks.OnUpdates([]model.KVPair{*wUpdate})
		}
	}
}

func (syn *kubeSyncer) readSnapshot(updateChan chan *model.KVPair,
	resyncChan chan *resourceVersions, initialVersionChan chan *resourceVersions) {

	log.Info("Starting readSnapshot worker")
	// Perform an initial snapshot, and send the latest versions to the
	// watcher routine.
	initialVersions := resourceVersions{}
	snap := syn.performSnapshot(&initialVersions)
	for _, u := range snap {
		// TODO: We're looping over this snapshot a lot.
		updateChan <- u
	}

	// Trigger the watcher routine to start watching at the
	// provided versions.
	syn.callbacks.OnStatusUpdated(api.InSync)
	initialVersionChan <- &initialVersions

	for {
		// Wait for an event on the resync request channel.
		log.Debug("Initial snapshot complete - waiting for resnc trigger")
		newVersions := <-resyncChan
		log.Warnf("Received snapshot trigger for versions %+v", newVersions)

		// We've received an event - perform a resync.
		snap = syn.performSnapshot(newVersions)
		for _, u := range snap {
			// TODO: We're looping over this snapshot a lot.
			updateChan <- u
		}
		syn.callbacks.OnStatusUpdated(api.InSync)

		// Send new resource versions back to watcher thread so
		// it can restart its watch.
		initialVersionChan <- newVersions
	}
}

func (syn *kubeSyncer) performSnapshot(versions *resourceVersions) []*model.KVPair {
	snap := []*model.KVPair{}
	syn.callbacks.OnStatusUpdated(api.ResyncInProgress)
	opts := k8sapi.ListOptions{}

	// Get Namespaces (Profiles)
	log.Info("Syncing Namespaces")
	nsList, _ := syn.kc.clientSet.Namespaces().List(opts)
	versions.namespaceVersion = nsList.ListMeta.ResourceVersion
	for _, ns := range nsList.Items {
		prof, _ := syn.kc.converter.namespaceToProfile(&ns)
		snap = append(snap, prof)

		// If this is the kube-system Namespace, also send
		// the pool through. // TODO: Hacky.
		if ns.ObjectMeta.Name == "kube-system" {
			pool, _ := syn.kc.converter.namespaceToPool(&ns)
			if pool != nil {
				snap = append(snap, pool)
			}
		}
	}

	// Get NetworkPolicies (Policies)
	log.Info("Syncing NetworkPolicy")
	npList, _ := syn.kc.clientSet.NetworkPolicies("").List(opts)
	versions.networkPolicyVersion = npList.ListMeta.ResourceVersion
	for _, np := range npList.Items {
		pol, _ := syn.kc.converter.networkPolicyToPolicy(&np)
		snap = append(snap, pol)
	}

	// Get Pods (WorkloadEndpoints)
	log.Info("Syncing Pods")
	poList, _ := syn.kc.clientSet.Pods("").List(opts)
	versions.podVersion = poList.ListMeta.ResourceVersion
	for _, po := range poList.Items {
		wep, _ := syn.kc.converter.podToWorkloadEndpoint(&po)
		if wep != nil {
			snap = append(snap, wep)
		}
	}
	log.Debugf("Created snapshot: %+v", snap)

	log.Infof("Snapshot resourceVersions: %+v", versions)
	return snap
}

// TODO: Handle case where we fall behind, trigger another snapshot.
// TODO: Find out a way to filter out the many duplicate updates we get for Pods.
func (syn *kubeSyncer) watchKubeAPI(updateChan chan *model.KVPair,
	resyncChan chan *resourceVersions, initialVersionSource chan *resourceVersions) {

	log.Info("Starting Kubernetes API watch worker")

	// Wait for the initial resourceVersions to watch for.
	initialVersions := <-initialVersionSource
	log.Infof("Received initialVersions: %+v", initialVersions)

	// Get watch channels for each resource.
	opts := k8sapi.ListOptions{ResourceVersion: initialVersions.namespaceVersion}
	nsWatch, err := syn.kc.clientSet.Namespaces().Watch(opts)
	if err != nil {
		panic(err)
	}
	opts = k8sapi.ListOptions{ResourceVersion: initialVersions.podVersion}
	poWatch, err := syn.kc.clientSet.Pods("").Watch(opts)
	if err != nil {
		panic(err)
	}
	opts = k8sapi.ListOptions{ResourceVersion: initialVersions.networkPolicyVersion}
	npWatch, err := syn.kc.clientSet.NetworkPolicies("").Watch(opts)
	if err != nil {
		panic(err)
	}

	// Keep track of the latest resource versions.
	latestVersions := initialVersions

	nsChan := nsWatch.ResultChan()
	poChan := poWatch.ResultChan()
	npChan := npWatch.ResultChan()
	var event watch.Event
	var kvp, poolKVP *model.KVPair
	for {
		select {
		case event = <-nsChan:
			// TODO: Check for errors which would cause a re-sync.
			kvp, poolKVP = syn.parseNamespaceEvent(event)
			latestVersions.namespaceVersion = kvp.Revision.(string)
		case event = <-poChan:
			kvp = syn.parsePodEvent(event)
			if kvp != nil {
				latestVersions.podVersion = kvp.Revision.(string)
			}
		case event = <-npChan:
			kvp = syn.parseNetworkPolicyEvent(event)
			latestVersions.networkPolicyVersion = kvp.Revision.(string)
		}

		// If we encounter a k8s resource
		// we don't care about, the KVPair will be nil.
		if kvp != nil {
			// Send the KVPair to the update channel.
			updateChan <- kvp
		}

		// If there was a pool update, send that as well.
		if poolKVP != nil {
			updateChan <- poolKVP
		}
	}
}

func (syn *kubeSyncer) parseNamespaceEvent(e watch.Event) (*model.KVPair, *model.KVPair) {
	ns, ok := e.Object.(*k8sapi.Namespace)
	if !ok {
		panic(ok)
	}

	// Convert the received Namespace into a profile KVPair.
	kvp, err := syn.kc.converter.namespaceToProfile(ns)
	if err != nil {
		panic(err)
	}

	// If this is the kube-system Namespace, it also houses Pool
	// information, so send a pool update. FIXME: Make this better.
	var poolKVP *model.KVPair
	if ns.ObjectMeta.Name == "kube-system" {
		poolKVP, err = syn.kc.converter.namespaceToPool(ns)
		if err != nil {
			panic(err)
		}
	}

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return kvp, poolKVP
}

func (syn *kubeSyncer) parsePodEvent(e watch.Event) *model.KVPair {
	pod, ok := e.Object.(*k8sapi.Pod)
	if !ok {
		panic(ok)
	}

	// Convert the received Namespace into a profile KVPair.
	kvp, err := syn.kc.converter.podToWorkloadEndpoint(pod)
	if err != nil {
		panic(err)
	}

	// Don't care about hostNetworked pods.
	if kvp == nil {
		return nil
	}

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return kvp
}

func (syn *kubeSyncer) parseNetworkPolicyEvent(e watch.Event) *model.KVPair {
	log.Debug("Parsing NetworkPolicy watch event")
	// First, check the event type.
	np, ok := e.Object.(*extensions.NetworkPolicy)
	if !ok {
		panic(ok)
	}

	// Convert the received NetworkPolicy into a profile KVPair.
	kvp, err := syn.kc.converter.networkPolicyToPolicy(np)
	if err != nil {
		panic(err)
	}

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return kvp
}
