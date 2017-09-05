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

	var prevDb string
	if apidInfo.LastSnapshot != "" && apidInfo.LastSnapshot != snapshot.SnapshotInfo {
		log.Debugf("Release snapshot for {%s}. Switching to version {%s}",
			apidInfo.LastSnapshot , snapshot.SnapshotInfo)
		prevDb = apidInfo.LastSnapshot
	} else {
		log.Debugf("Process snapshot for version {%s}",
			snapshot.SnapshotInfo)
	}
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

	// Releases the DB, when the Connection reference count reaches 0.
	if prevDb != "" {
		dataService.ReleaseDB(prevDb)
	}
}


func completeTxn (tx apid.Tx, err error) {
	if err == nil {
		err = tx.Commit()
		if err == nil {
			log.Debugf("Transaction committed successfully")
			return
		}
		log.Errorf("Transaction commit failed with error : {%v}", err)
	}
	err = tx.Rollback()
	if err != nil {
		log.Panicf("Unable to rollback Transaction. DB in inconsistent state. Err {%v}", err)
	}
}

func processSqliteSnapshot(db apid.DB) {

	var numApidClusters int
	tx, err := db.Begin()
	if err != nil {
		log.Panicf("Unable to open DB txn: {%v}", err.Error())
	}
	defer completeTxn(tx, err)
	err = tx.QueryRow("SELECT COUNT(*) FROM edgex_apid_cluster").Scan(&numApidClusters)
	if err != nil {
		log.Panicf("Unable to read database: {%s}", err.Error())
	}

	if numApidClusters != 1 {
		log.Panic("Illegal state for apid_cluster. Must be a single row.")
	}

	_, err = tx.Exec("ALTER TABLE edgex_apid_cluster ADD COLUMN last_sequence text DEFAULT ''")
	if err != nil {
		if err.Error() == "duplicate column name: last_sequence" {
			return
		} else {
			log.Panicf("Unable to create last_sequence column on DB.  Error {%v}", err.Error())
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
	defer completeTxn(tx, err)

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

	return ok
}
