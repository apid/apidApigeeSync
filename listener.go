package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	LISTENER_TABLE_APID_CLUSTER = "edgex.apid_cluster"
	LISTENER_TABLE_DATA_SCOPE   = "edgex.data_scope"
)

type handler struct {
}

func (h *handler) String() string {
	return "ApigeeSync"
}

func (h *handler) Handle(e apid.Event) {

	if changeSet, ok := e.(*common.ChangeList); ok {
		processChangeList(changeSet)
	} else if snapShot, ok := e.(*common.Snapshot); ok {
		processSnapshot(snapShot)
	} else {
		log.Debugf("Received invalid event. Ignoring. %v", e)
	}
}

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
	apidClusters.Scan(&numApidClusters)

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
