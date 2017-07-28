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
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	LISTENER_TABLE_APID_CLUSTER = "edgex.apid_cluster"
	LISTENER_TABLE_DATA_SCOPE   = "edgex.data_scope"
)

type listenerManager struct {
	dbm dbManagerInterface
}

func (lm *listenerManager) processSnapshot(snapshot *common.Snapshot) {
	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := dataService.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	lm.processSqliteSnapshot(db)

	//update apid instance info
	apidInfo.LastSnapshot = snapshot.SnapshotInfo
	err = lm.dbm.updateApidInstanceInfo()
	if err != nil {
		log.Panicf("Unable to update instance info: %v", err)
	}

	lm.dbm.setDb(db)
	log.Debugf("Snapshot processed: %s", snapshot.SnapshotInfo)

}

func (lm *listenerManager) processSqliteSnapshot(db apid.DB) {

	if count, err := lm.dbm.getClusterCount(); err != nil || count != 1 {
		log.Panicf("Illegal state for apid_cluster. Must be a single row. %v", err)
	}

	if err := lm.dbm.alterClusterTable(); err != nil {
		log.Panicf("Unable to create last_sequence column on DB.  Unrecoverable error %v", err)
	}
}

func (lm *listenerManager) processChangeList(changes *common.ChangeList) bool {

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))

	for _, change := range changes.Changes {
		if change.Table == LISTENER_TABLE_APID_CLUSTER {
			log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
		}
		if change.Operation == common.Update && change.Table == LISTENER_TABLE_DATA_SCOPE {
			log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
		}
	}

	return lm.dbm.writeTransaction(changes)
}
