package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	"time"
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
		log.Errorf("Received invalid event. Ignoring. %v", e)
	}
}

func processSnapshot(snapshot *common.Snapshot) {
	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := dataService.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	if config.GetString(configSnapshotProtocol) == "json" {
		processJsonSnapshot(snapshot, db)
	} else if config.GetString(configSnapshotProtocol) == "sqlite" {
		processSqliteSnapshot(snapshot, db)
	}

	//update apid instance info
	apidInfo.LastSnapshot = snapshot.SnapshotInfo
	err = updateApidInstanceInfo()
	if err != nil {
		log.Panicf("Unable to update instance info: %v", err)
	}

	setDB(db)
	log.Debugf("Snapshot processed: %s", snapshot.SnapshotInfo)

}

func processSqliteSnapshot(snapshot *common.Snapshot, db apid.DB) {
	//nothing to do as of now, here as a placeholder
}

func processJsonSnapshot(snapshot *common.Snapshot, db apid.DB) {

	err := initDB(db)
	if err != nil {
		log.Panicf("Unable to initialize database: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Panicf("Error starting transaction: %v", err)
	}
	defer tx.Rollback()

	for _, table := range snapshot.Tables {

		switch table.Name {
		case LISTENER_TABLE_APID_CLUSTER:
			if len(table.Rows) > 1 {
				log.Panic("Illegal state for apid_cluster. Must be a single row.")
			}
			for _, row := range table.Rows {
				ac := makeApidClusterFromRow(row)
				err := insertApidCluster(ac, tx)
				if err != nil {
					log.Panicf("Snapshot update failed: %v", err)
				}
			}

		case LISTENER_TABLE_DATA_SCOPE:
			for _, row := range table.Rows {
				ds := makeDataScopeFromRow(row)
				err := insertDataScope(ds, tx)
				if err != nil {
					log.Panicf("Snapshot update failed: %v", err)
				}
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Panicf("Error committing Snapshot change: %v", err)
	}
}

func processChangeList(changes *common.ChangeList) {

	tx, err := getDB().Begin()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
	}
	defer tx.Rollback()

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))

	for _, change := range changes.Changes {
		switch change.Table {
		case "edgex.apid_cluster":
			switch change.Operation {
			case common.Delete:
				// todo: shut down apid, delete databases, scorch the earth!
				log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
			default:
				log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
			}
		case "edgex.data_scope":
			switch change.Operation {
			case common.Insert:
				ds := makeDataScopeFromRow(change.NewRow)
				err = insertDataScope(ds, tx)
			case common.Delete:
				ds := makeDataScopeFromRow(change.OldRow)
				err = deleteDataScope(ds, tx)
			default:
				// common.Update is not allowed
				log.Panicf("illegal operation: %s for %s", change.Operation, change.Table)
			}
		}
		if err != nil {
			log.Panicf("Error processing ChangeList: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Panicf("Error processing ChangeList: %v", err)
	}
}

func makeApidClusterFromRow(row common.Row) dataApidCluster {

	dac := dataApidCluster{}

	row.Get("id", &dac.ID)
	row.Get("name", &dac.Name)
	row.Get("umbrella_org_app_name", &dac.OrgAppName)
	row.Get("created", &dac.Created)
	row.Get("created_by", &dac.CreatedBy)
	row.Get("updated", &dac.Updated)
	row.Get("updated_by", &dac.UpdatedBy)
	row.Get("description", &dac.Description)

	// convert timestamp to ISO8601
	if dac.Created != "" {
		created, err := time.Parse(sqlTimeFormat, dac.Created)
		if err != nil {
			log.Panic(err)
		}
		dac.Created = created.Format(iso8601)
	}

	if dac.Updated != "" {
		updated, err := time.Parse(sqlTimeFormat, dac.Updated)
		if err != nil {
			log.Panic(err)
		}
		dac.Updated = updated.Format(iso8601)
	}

	return dac
}

func makeDataScopeFromRow(row common.Row) dataDataScope {

	ds := dataDataScope{}

	row.Get("id", &ds.ID)
	row.Get("apid_cluster_id", &ds.ClusterID)
	row.Get("scope", &ds.Scope)
	row.Get("org", &ds.Org)
	row.Get("env", &ds.Env)
	row.Get("created", &ds.Created)
	row.Get("created_by", &ds.CreatedBy)
	row.Get("updated", &ds.Updated)
	row.Get("updated_by", &ds.UpdatedBy)

	// convert timestamp to ISO8601
	if ds.Created != "" {
		created, err := time.Parse(sqlTimeFormat, ds.Created)
		if err != nil {
			log.Panic(err)
		}
		ds.Created = created.Format(iso8601)
	}

	if ds.Updated != "" {
		updated, err := time.Parse(sqlTimeFormat, ds.Updated)
		if err != nil {
			log.Panic(err)
		}
		ds.Updated = updated.Format(iso8601)
	}
	return ds
}
