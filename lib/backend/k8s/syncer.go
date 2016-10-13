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
	snapshotUpdates := make(chan *[]model.KVPair)

	// Channel for receiving updates from the watcher.
	watchUpdates := make(chan *model.KVPair)

	// Channel used by the watcher to trigger a re-sync.
	triggerResync := make(chan *resourceVersions, 5)

	// Channel to send the index from which to start the snapshot.
	initialSnapshotIndex := make(chan *resourceVersions)

	// Status channel
	statusUpdates := make(chan api.SyncStatus)

	// If we're not in one-shot mode, start the API watcher to
	// gather updates.
	if !syn.OneShot {
		go syn.watchKubeAPI(watchUpdates, triggerResync, initialSnapshotIndex)
	}

	// Start a background thread to read snapshots from etcd.  It will
	// read a start-of-day snapshot and then wait to be signalled on the
	// resyncIndex channel.
	go syn.readSnapshot(snapshotUpdates, triggerResync, initialSnapshotIndex, statusUpdates)

	// Start a routine to merge updates from the snapshot routine and the
	// watch routine (if running), and pass information to callbacks.
	// TODO: We're never actually running both the snapshot thread and the watch thread at the same time
	// so what's the point?
	go syn.mergeUpdates(snapshotUpdates, watchUpdates, statusUpdates)
}

// TODO: Make this smarter!
func (syn *kubeSyncer) mergeUpdates(snapshotUpdates chan *[]model.KVPair, watchUpdates chan *model.KVPair, statusUpdates chan api.SyncStatus) {
	var update *model.KVPair
	var updates *[]model.KVPair
	currentStatus := api.WaitForDatastore
	newStatus := api.WaitForDatastore
	syn.callbacks.OnStatusUpdated(api.WaitForDatastore)
	for {
		select {
		case updates = <-snapshotUpdates:
			log.Debugf("Snapshot update: %+v", updates)
			syn.callbacks.OnUpdates(*updates)
		case update = <-watchUpdates:
			log.Debugf("Watch update: %+v", update)
			syn.callbacks.OnUpdates([]model.KVPair{*update})
		case newStatus = <-statusUpdates:
			if newStatus != currentStatus {
				syn.callbacks.OnStatusUpdated(newStatus)
				currentStatus = newStatus
			}
			continue
		}
	}
}

func (syn *kubeSyncer) readSnapshot(updateChan chan *[]model.KVPair,
	resyncChan chan *resourceVersions, initialVersionChan chan *resourceVersions, statusUpdates chan api.SyncStatus) {

	log.Info("Starting readSnapshot worker")
	// Indicate we're starting a resync.
	statusUpdates <- api.ResyncInProgress

	// Perform an initial snapshot, and send the latest versions to the
	// watcher routine.
	initialVersions := resourceVersions{}
	updateChan <- syn.performSnapshot(&initialVersions)

	// Indicate we're in sync for the first time.
	statusUpdates <- api.InSync

	// Trigger the watcher routine to start watching at the
	// provided versions.
	initialVersionChan <- &initialVersions

	for {
		// Wait for an event on the resync request channel.
		log.Debug("Initial snapshot complete - waiting for resnc trigger")
		newVersions := <-resyncChan
		statusUpdates <- api.ResyncInProgress
		log.Warnf("Received snapshot trigger for versions %+v", newVersions)

		// We've received an event - perform a resync.
		updateChan <- syn.performSnapshot(newVersions)
		statusUpdates <- api.InSync

		// Send new resource versions back to watcher thread so
		// it can restart its watch.
		initialVersionChan <- newVersions
	}
}

// performSnapshot returns a list of existing objects in the datastore, and
// populates the provided resourceVersions with the latest k8s resource version
// for each.
func (syn *kubeSyncer) performSnapshot(versions *resourceVersions) *[]model.KVPair {
	snap := []model.KVPair{}
	opts := k8sapi.ListOptions{}

	// Get Namespaces (Profiles)
	log.Info("Syncing Namespaces")
	nsList, _ := syn.kc.clientSet.Namespaces().List(opts)
	versions.namespaceVersion = nsList.ListMeta.ResourceVersion
	for _, ns := range nsList.Items {
		// The Syncer API expects a profile to be broken into its underlying
		// components - rules, tags, labels. TODO: Why?
		rules, tags, labels, _ := syn.kc.converter.namespaceToProfileComponents(&ns)
		snap = append(snap, *rules, *tags, *labels)

		// If this is the kube-system Namespace, also send
		// the pool through. // TODO: Hacky.
		if ns.ObjectMeta.Name == "kube-system" {
			pool, _ := syn.kc.converter.namespaceToPool(&ns)
			if pool != nil {
				snap = append(snap, *syn.convertValueToPointer(pool))
			}
		}
	}

	// Get NetworkPolicies (Policies)
	log.Info("Syncing NetworkPolicy")
	npList, _ := syn.kc.clientSet.NetworkPolicies("").List(opts)
	versions.networkPolicyVersion = npList.ListMeta.ResourceVersion
	for _, np := range npList.Items {
		pol, _ := syn.kc.converter.networkPolicyToPolicy(&np)
		snap = append(snap, *syn.convertValueToPointer(pol))
	}

	// Get Pods (WorkloadEndpoints)
	log.Info("Syncing Pods")
	poList, _ := syn.kc.clientSet.Pods("").List(opts)
	versions.podVersion = poList.ListMeta.ResourceVersion
	for _, po := range poList.Items {
		wep, _ := syn.kc.converter.podToWorkloadEndpoint(&po)
		if wep != nil {
			snap = append(snap, *syn.convertValueToPointer(wep))
		}
	}

	// Sync GlobalConfig
	confList, _ := syn.kc.listGlobalConfig(model.GlobalConfigListOptions{})
	for _, c := range confList {
		snap = append(snap, *syn.convertValueToPointer(c))
	}

	log.Infof("Snapshot resourceVersions: %+v", versions)
	log.Debugf("Created snapshot: %+v", snap)
	return &snap
}

// convertValueToPointer converts the Value of a KVPair to a typed pointer.
func (sync *kubeSyncer) convertValueToPointer(kvp *model.KVPair) *model.KVPair {
	switch kvp.Key.(type) {
	case model.ProfileKey:
		v := kvp.Value.(model.Profile)
		return &model.KVPair{Key: kvp.Key, Value: &v, Revision: kvp.Revision}
	case model.PoolKey:
		v := kvp.Value.(model.Pool)
		return &model.KVPair{Key: kvp.Key, Value: &v, Revision: kvp.Revision}
	case model.PolicyKey:
		v := kvp.Value.(model.Policy)
		return &model.KVPair{Key: kvp.Key, Value: &v, Revision: kvp.Revision}
	case model.WorkloadEndpointKey:
		v := kvp.Value.(model.WorkloadEndpoint)
		return &model.KVPair{Key: kvp.Key, Value: &v, Revision: kvp.Revision}
	default:
		// Otherwise, just return the object as it is.  Felix expects some objects
		// to _not_ be pointers.
		return kvp
	}
}

// watchKubeAPI watches the Kubernetes API and sends updates to the merge thread.
// If it encounters an error or falls behind, it triggers a new snapshot.
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
	var kvp *model.KVPair
	needsResync := false

	for {
		select {
		case event = <-nsChan:
			log.Debugf("Incoming Namespace watch event. Type=%s, Object=%+v", event.Type, event.Object)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the inner for loop, back
				// into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				break
			}

			// Event is OK - parse it.
			kvps := syn.parseNamespaceEvent(event)
			for _, k := range kvps {
				if k == nil {
					// This can return nil when the event is not
					// one we care about.
					continue
				}
				updateChan <- syn.convertValueToPointer(k)
				latestVersions.namespaceVersion = k.Revision.(string)
			}
			continue
		case event = <-poChan:
			log.Debugf("Incoming Pod watch event. Type=%s, Object=%+v", event.Type, event.Object)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the inner for loop, back
				// into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				break
			}

			// Event is OK - parse it.
			kvp = syn.parsePodEvent(event)
			if kvp == nil {
				// This can return nil when the event is not one
				// we care about.
				continue
			}
			latestVersions.podVersion = kvp.Revision.(string)
			updateChan <- syn.convertValueToPointer(kvp)
		case event = <-npChan:
			log.Debugf("Incoming NetworkPolicy watch event. Type=%s, Object=%+v", event.Type, event.Object)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the inner for loop, back
				// into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				break
			}

			// Event is OK - parse it.
			kvp = syn.parseNetworkPolicyEvent(event)
			updateChan <- syn.convertValueToPointer(kvp)
			latestVersions.networkPolicyVersion = kvp.Revision.(string)
		}

		if needsResync {
			// We broke out of the watch loop - trigger a resync.
			log.Warnf("Resync required - sending latest versions: %+v", latestVersions)
			resyncChan <- latestVersions

			// Wait to be told the new versions to watch.
			log.Warn("Waiting for snapshot to complete")
			latestVersions = <-initialVersionSource
			log.Warnf("Snapshot complete after resync - start watch from %+v", latestVersions)

			// Reset the flag.
			needsResync = false
		}
	}
}

// eventTriggersResync returns true of the given event requires a
// full datastore resync to occur, and false otherwise.
func (syn *kubeSyncer) eventTriggersResync(e watch.Event) bool {
	// If we encounter an error, or if the event is nil (which can indicate
	// an unexpected connection close).
	if e.Type == watch.Error || e.Object == nil {
		return true
	}
	return false
}

func (syn *kubeSyncer) parseNamespaceEvent(e watch.Event) []*model.KVPair {
	ns, ok := e.Object.(*k8sapi.Namespace)
	if !ok {
		panic(fmt.Sprintf("Invalid namespace event: %+v", e.Object))
	}

	// Convert the received Namespace into a profile KVPair.
	rules, tags, labels, err := syn.kc.converter.namespaceToProfileComponents(ns)
	if err != nil {
		panic(err)
	}

	// If this is the kube-system Namespace, it also houses Pool
	// information, so send a pool update. FIXME: Make this better.
	var pool *model.KVPair
	if ns.ObjectMeta.Name == "kube-system" {
		pool, err = syn.kc.converter.namespaceToPool(ns)
		if err != nil {
			panic(err)
		}
	}

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		rules.Value = nil
		tags.Value = nil
		labels.Value = nil
	}
	return []*model.KVPair{rules, tags, labels, pool}
}

func (syn *kubeSyncer) parsePodEvent(e watch.Event) *model.KVPair {
	pod, ok := e.Object.(*k8sapi.Pod)
	if !ok {
		panic(fmt.Sprintf("Invalid pod event. Type: %s, Object: %+v", e.Type, e.Object))
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
		log.Debugf("Delete for pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
		kvp.Value = nil
	} else if len(kvp.Value.(model.WorkloadEndpoint).IPv4Nets) == 0 {
		// Don't care about adds/modifies with no IP address.  This means the pod
		// hasn't been started yet.
		return nil
	}

	return kvp
}

func (syn *kubeSyncer) parseNetworkPolicyEvent(e watch.Event) *model.KVPair {
	log.Debug("Parsing NetworkPolicy watch event")
	// First, check the event type.
	np, ok := e.Object.(*extensions.NetworkPolicy)
	if !ok {
		panic(fmt.Sprintf("Invalid NetworkPolicy event. Type: %s, Object: %+v", e.Type, e.Object))
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
