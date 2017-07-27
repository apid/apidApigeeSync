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

func processSnapshot(snapshot *common.Snapshot) {
	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := dataService.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	processSqliteSnapshot(db)

	//update apid instance info
	apidInfo.LastSnapshot = snapshot.SnapshotInfo
	err = updateApidInstanceInfo()
	if err != nil {
		log.Panicf("Unable to update instance info: %v", err)
	}

	setDB(db)
	log.Debugf("Snapshot processed: %s", snapshot.SnapshotInfo)

}

func processSqliteSnapshot(db apid.DB) {

	var numApidClusters int
	apidClusters, err := db.Query("SELECT COUNT(*) FROM edgex_apid_cluster")
	if err != nil {
		log.Panicf("Unable to read database: %s", err.Error())
	}
	apidClusters.Next()
	err = apidClusters.Scan(&numApidClusters)
	if err != nil {
		log.Panicf("Unable to read database: %s", err.Error())
	}

	if numApidClusters != 1 {
		log.Panic("Illegal state for apid_cluster. Must be a single row.")
	}

	_, err = db.Exec("ALTER TABLE edgex_apid_cluster ADD COLUMN last_sequence text DEFAULT ''")
	if err != nil {
		if err.Error() == "duplicate column name: last_sequence" {
			return
		} else {
			log.Error("[[" + err.Error() + "]]")
			log.Panicf("Unable to create last_sequence column on DB.  Unrecoverable error ", err)
		}
	}
}

func processChangeList(changes *common.ChangeList) bool {

	ok := false

	tx, err := getDB().Begin()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
		return ok
	}
	defer tx.Rollback()

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))

	for _, change := range changes.Changes {
		if change.Table == LISTENER_TABLE_APID_CLUSTER {
			log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
		}
		switch change.Operation {
		case common.Insert:
			ok = insert(change.Table, []common.Row{change.NewRow}, tx)
		case common.Update:
			if change.Table == LISTENER_TABLE_DATA_SCOPE {
				log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
			}
			ok = update(change.Table, []common.Row{change.OldRow}, []common.Row{change.NewRow}, tx)
		case common.Delete:
			ok = _delete(change.Table, []common.Row{change.OldRow}, tx)
		}
		if !ok {
			log.Error("Sql Operation error. Operation rollbacked")
			return ok
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
		return false
	}

	return ok
}
