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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/compat"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	k8sapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/watch"
	"reflect"
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

func getUpdateType(e watch.Event) model.UpdateType {
	// Determine the update type.
	updateType := model.UpdateTypeKVUnknown
	switch e.Type {
	case watch.Added:
		updateType = model.UpdateTypeKVNew
	case watch.Deleted:
		updateType = model.UpdateTypeKVDeleted
	case watch.Modified:
		updateType = model.UpdateTypeKVUpdated
	}
	return updateType
}

func (syn *kubeSyncer) Start() {
	// Channel for receiving updates from a snapshot.
	snapshotUpdates := make(chan *[]model.Update)

	// Channel for receiving updates from the watcher.
	watchUpdates := make(chan *model.Update)

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

// mergeUpdates ensures that callbacks are all executed from the same thread.
func (syn *kubeSyncer) mergeUpdates(snapshotUpdates chan *[]model.Update, watchUpdates chan *model.Update, statusUpdates chan api.SyncStatus) {
	var update *model.Update
	var updates *[]model.Update
	currentStatus := api.WaitForDatastore
	newStatus := api.WaitForDatastore
	syn.callbacks.OnStatusUpdated(api.WaitForDatastore)
	for {
		select {
		case updates = <-snapshotUpdates:
			log.Debugf("Snapshot update: %+v", updates)
			if currentStatus != api.ResyncInProgress {
				panic("Recieved snapshot update while not resyncing")
			}
			syn.callbacks.OnUpdates(*updates)
		case update = <-watchUpdates:
			log.Debugf("Watch update: %+v", update)
			if currentStatus != api.InSync {
				panic("Recieved watch update while not in sync")
			}
			syn.callbacks.OnUpdates([]model.Update{*update})
		case newStatus = <-statusUpdates:
			if newStatus != currentStatus {
				log.Debugf("Status update. %s -> %s", currentStatus, newStatus)
				syn.callbacks.OnStatusUpdated(newStatus)
				currentStatus = newStatus
			}
		}
	}
}

func (syn *kubeSyncer) readSnapshot(updateChan chan *[]model.Update,
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
func (syn *kubeSyncer) performSnapshot(versions *resourceVersions) *[]model.Update {
	snap := []model.Update{}
	opts := k8sapi.ListOptions{}

	// Loop until we successfully are able to accesss the API.
	for {
		// Get Namespaces (Profiles)
		log.Info("Syncing Namespaces")
		nsList, err := syn.kc.clientSet.Namespaces().List(opts)
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		versions.namespaceVersion = nsList.ListMeta.ResourceVersion
		for _, ns := range nsList.Items {
			// The Syncer API expects a profile to be broken into its underlying
			// components - rules, tags, labels.
			profile, err := syn.kc.converter.namespaceToProfile(&ns)
			if err != nil {
				panic(err)
			}
			rules, tags, labels := compat.ToTagsLabelsRules(profile)
			snap = append(snap,
				model.Update{KVPair: *rules, UpdateType: model.UpdateTypeKVNew},
				model.Update{KVPair: *tags, UpdateType: model.UpdateTypeKVNew},
				model.Update{KVPair: *labels, UpdateType: model.UpdateTypeKVNew},
			)

			// If this is the kube-system Namespace, also send
			// the pool through. // TODO: Hacky.
			if ns.ObjectMeta.Name == "kube-system" {
				pool, _ := syn.kc.converter.namespaceToPool(&ns)
				if pool != nil {
					snap = append(snap, model.Update{KVPair: *pool, UpdateType: model.UpdateTypeKVNew})
				}
			}
		}

		// Get NetworkPolicies (Policies)
		log.Info("Syncing NetworkPolicy")
		npList, err := syn.kc.clientSet.NetworkPolicies("").List(opts)
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		versions.networkPolicyVersion = npList.ListMeta.ResourceVersion
		for _, np := range npList.Items {
			pol, _ := syn.kc.converter.networkPolicyToPolicy(&np)
			snap = append(snap, model.Update{KVPair: *pol, UpdateType: model.UpdateTypeKVNew})
		}

		// Get Pods (WorkloadEndpoints)
		log.Info("Syncing Pods")
		poList, err := syn.kc.clientSet.Pods("").List(opts)
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		versions.podVersion = poList.ListMeta.ResourceVersion
		for _, po := range poList.Items {
			wep, _ := syn.kc.converter.podToWorkloadEndpoint(&po)
			if wep != nil {
				snap = append(snap, model.Update{KVPair: *wep, UpdateType: model.UpdateTypeKVNew})
			}
		}

		// Sync GlobalConfig
		confList, err := syn.kc.listGlobalConfig(model.GlobalConfigListOptions{})
		if err != nil {
			log.Warnf("Error accessing Kubernetes API, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, c := range confList {
			snap = append(snap, model.Update{KVPair: *c, UpdateType: model.UpdateTypeKVNew})
		}

		log.Infof("Snapshot resourceVersions: %+v", versions)
		log.Debugf("Created snapshot: %+v", snap)
		return &snap
	}
}

// watchKubeAPI watches the Kubernetes API and sends updates to the merge thread.
// If it encounters an error or falls behind, it triggers a new snapshot.
func (syn *kubeSyncer) watchKubeAPI(updateChan chan *model.Update,
	resyncChan chan *resourceVersions, initialVersionSource chan *resourceVersions) {

	log.Info("Starting Kubernetes API watch worker")

	// Wait for the initial resourceVersions to watch for.
	log.Info("Waiting for initial snapshot to complete")
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
	var upd *model.Update
	needsResync := false

	log.Info("Starting Kubernetes API watch loop")
	for {
		// If we need to resync, do so.
		if needsResync {
			log.Warnf("Resync required - sending latest versions: %+v", latestVersions)
			resyncChan <- latestVersions

			// Wait to be told the new versions to watch.
			log.Warn("Waiting for snapshot to complete")
			latestVersions = <-initialVersionSource
			log.Warnf("Snapshot complete after resync - start watch from %+v", latestVersions)

			// Re-create the watches.
			opts = k8sapi.ListOptions{ResourceVersion: initialVersions.namespaceVersion}
			nsWatch, err = syn.kc.clientSet.Namespaces().Watch(opts)
			if err != nil {
				log.Warn("Failed to connect to API, retrying")
				time.Sleep(1 * time.Second)
				continue
			}
			opts = k8sapi.ListOptions{ResourceVersion: initialVersions.podVersion}
			poWatch, err = syn.kc.clientSet.Pods("").Watch(opts)
			if err != nil {
				log.Warn("Failed to connect to API, retrying")
				time.Sleep(1 * time.Second)
				continue
			}
			opts = k8sapi.ListOptions{ResourceVersion: initialVersions.networkPolicyVersion}
			npWatch, err = syn.kc.clientSet.NetworkPolicies("").Watch(opts)
			if err != nil {
				log.Warn("Failed to connect to API, retrying")
				time.Sleep(1 * time.Second)
				continue
			}

			nsChan = nsWatch.ResultChan()
			poChan = poWatch.ResultChan()
			npChan = npWatch.ResultChan()

			// Successfull resync - reset the flag.
			needsResync = false
		}

		// Select on the various watch channels.
		select {
		case event = <-nsChan:
			log.Debugf("Incoming Namespace watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the inner for loop, back
				// into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				break
			}

			// Event is OK - parse it.
			upds := syn.parseNamespaceEvent(event)
			for _, k := range upds {
				if k == nil {
					// This can return nil when the event is not
					// one we care about.
					continue
				}
				updateChan <- k
				latestVersions.namespaceVersion = k.KVPair.Revision.(string)
			}
			continue
		case event = <-poChan:
			log.Debugf("Incoming Pod watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the inner for loop, back
				// into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				break
			}

			// Event is OK - parse it.
			if upd = syn.parsePodEvent(event); upd != nil {
				// Only send the update if we care about it.  We filter
				// out a number of events that aren't useful for us.
				latestVersions.podVersion = upd.KVPair.Revision.(string)
				updateChan <- upd
			}
		case event = <-npChan:
			log.Debugf("Incoming NetworkPolicy watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the inner for loop, back
				// into the sync loop.
				log.Warn("Event triggered resync: %+v", event)
				break
			}

			// Event is OK - parse it.
			updateChan <- upd
			latestVersions.networkPolicyVersion = upd.KVPair.Revision.(string)
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

func (syn *kubeSyncer) parseNamespaceEvent(e watch.Event) []*model.Update {
	ns, ok := e.Object.(*k8sapi.Namespace)
	if !ok {
		panic(fmt.Sprintf("Invalid namespace event: %+v", e.Object))
	}

	// Convert the received Namespace into a profile KVPair.
	profile, err := syn.kc.converter.namespaceToProfile(ns)
	if err != nil {
		panic(err)
	}
	rules, tags, labels := compat.ToTagsLabelsRules(profile)

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

	// Return the updates.
	updateType := getUpdateType(e)
	updates := []*model.Update{
		&model.Update{KVPair: *rules, UpdateType: updateType},
		&model.Update{KVPair: *tags, UpdateType: updateType},
		&model.Update{KVPair: *labels, UpdateType: updateType},
	}
	if pool != nil {
		updates = append(updates, &model.Update{KVPair: *pool, UpdateType: updateType})
	}
	return updates
}

// labelCache stores labels for a given pod, so we can determine if
// we need to send an update on the syncer API for a given event.
var labelCache map[string]map[string]string = map[string]map[string]string{}

// parsePodEvent returns a KVPair for the given event.  If the event isn't
// useful, parsePodEvent returns nil to indicate that there is nothing to do.
func (syn *kubeSyncer) parsePodEvent(e watch.Event) *model.Update {
	pod, ok := e.Object.(*k8sapi.Pod)
	if !ok {
		panic(fmt.Sprintf("Invalid pod event. Type: %s, Object: %+v", e.Type, e.Object))
	}

	// Ignore any updates for host networked pods.
	if syn.kc.converter.isHostNetworked(pod) {
		log.Debugf("Skipping host networked pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
		return nil
	}

	// Convert the received Namespace into a KVPair.
	kvp, err := syn.kc.converter.podToWorkloadEndpoint(pod)
	if err != nil {
		panic(err)
	}

	// We behave differently based on the event type.
	switch e.Type {
	case watch.Deleted:
		// For deletes, we need to nil out the Value part of the KVPair.
		log.Debugf("Delete for pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
		kvp.Value = nil

		// Remove it from the cache, if it is there.
		workload := kvp.Key.(model.WorkloadEndpointKey).WorkloadID
		delete(labelCache, workload)
	default:
		// Adds and modifies are treated the same.  First, if the pod doesn't have an
		// IP address, we ignore it until it does.
		if !syn.kc.converter.hasIPAddress(pod) {
			log.Debugf("Skipping pod with no IP: %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			return nil
		}

		// If it does have an address, we only send updates if the labels have changed.
		workload := kvp.Key.(model.WorkloadEndpointKey).WorkloadID
		labels := kvp.Value.(*model.WorkloadEndpoint).Labels
		if reflect.DeepEqual(labelCache[workload], labels) {
			// Labels haven't changed - no need to send an update for this add/modify.
			log.Debugf("Skipping pod event - labels didn't change: %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			return nil
		}

		// Labels have changed on a running pod - update the label cache.
		labelCache[workload] = labels
	}

	return &model.Update{KVPair: *kvp, UpdateType: getUpdateType(e)}
}

func (syn *kubeSyncer) parseNetworkPolicyEvent(e watch.Event) *model.Update {
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
	return &model.Update{KVPair: *kvp, UpdateType: getUpdateType(e)}
}
