// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apidApigeeSync

import (
	"encoding/json"
	"github.com/apid/apid-core"
)

const (
	LISTENER_TABLE_APID_CLUSTER = "edgex.apid_cluster"
	LISTENER_TABLE_DATA_SCOPE   = "edgex.data_scope"
)

type listenerManager struct {
	changeMan     changeManager
	snapMan       snapshotManager
	tokenMan      tokenManager
	isOfflineMode bool
}

func (l *listenerManager) init() {
	/* This callback function will get called once all the plugins are
	 * initialized (not just this plugin). This is needed because,
	 * downloadSnapshots/changes etc have to begin to be processed only
	 * after all the plugins are initialized
	 */
	eventService.ListenOnceFunc(apid.SystemEventsSelector, l.postInitPlugins)
}

// Plugins have all initialized, gather their info and start the ApigeeSync downloads
func (l *listenerManager) postInitPlugins(event apid.Event) {
	var plinfoDetails []pluginDetail
	if pie, ok := event.(apid.PluginsInitializedEvent); ok {
		/*
		 * Store the plugin details in the heap. Needed during
		 * Bearer token generation request.
		 */
		for _, plugin := range pie.Plugins {
			name := plugin.Name
			version := plugin.Version
			if schemaVersion, ok := plugin.ExtraData["schemaVersion"].(string); ok {
				inf := pluginDetail{
					Name:          name,
					SchemaVersion: schemaVersion}
				plinfoDetails = append(plinfoDetails, inf)
				log.Debugf("plugin %s is version %s, schemaVersion: %s", name, version, schemaVersion)
			}
		}
		if plinfoDetails == nil {
			log.Panic("No Plugins registered!")
		}

		pgInfo, err := json.Marshal(plinfoDetails)
		if err != nil {
			log.Panicf("Unable to marshal plugin data: %v", err)
		}
		apidPluginDetails = string(pgInfo[:])

		log.Debug("start post plugin init")

		l.tokenMan.start()
		go l.bootstrap(apidInfo.LastSnapshot)

		log.Debug("Done post plugin init")
	}
}

/*
 *  Start from existing snapshot if possible
 *  If an existing snapshot does not exist, use the apid scope to fetch
 *  all data scopes, then get a snapshot for those data scopes
 *
 *  Then, poll for changes
 */
func (l *listenerManager) bootstrap(lastSnap string) {
	if l.isOfflineMode && lastSnap == "" {
		log.Panic("Diagnostic mode requires existent snapshot info in default DB.")
	}

	if lastSnap != "" {
		if err := l.snapMan.startOnDataSnapshot(lastSnap); err == nil {
			log.Infof("Started on local snapshot: %s", lastSnap)
			l.changeMan.pollChangeWithBackoff()
			return
		} else {
			log.Errorf("Failed to bootstrap on local snapshot: %v", err)
			log.Warn("Will get new snapshots.")
		}
	}

	l.snapMan.downloadBootSnapshot()
	if err := l.snapMan.downloadDataSnapshot(); err != nil {
		log.Panicf("Error downloading data snapshot: %v", err)
	}
	l.changeMan.pollChangeWithBackoff()
}
