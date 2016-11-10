package apidApigeeSync

import (
	"database/sql"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
)

type handler struct {
}

func (h *handler) String() string {
	return "ApigeeSync"
}

func (h *handler) Handle(e apid.Event) {

	res := true

	db, err := data.DB()
	if err != nil {
		panic("Unable to access Sqlite DB")
	}
	txn, err := db.Begin()
	if err != nil {
		log.Error("Unable to create Sqlite transaction")
		return
	}

	snapData, ok := e.(*common.Snapshot)
	if ok {
		res = processSnapshot(snapData, txn)
	} else {
		changeSet, ok := e.(*common.ChangeList)
		if ok {
			res = processChange(changeSet, txn)
		} else {
			log.Fatal("Received Invalid event. This shouldn't happen!")
		}
	}
	if res == true {
		txn.Commit()
	} else {
		txn.Rollback()
	}
	return
}

func processSnapshot(snapshot *common.Snapshot, txn *sql.Tx) bool {

	log.Debugf("Process Snapshot data")
	res := true

	for _, payload := range snapshot.Tables {

		switch payload.Name {
		case "edgex.apid_config":
			res = insertApidConfig(payload.Rows, txn, snapshot.SnapshotInfo)
		case "edgex.apid_config_scope":
			res = insertApidConfigScopes(payload.Rows, txn)
		}
		if res == false {
			log.Error("Error encountered in Downloading Snapshot for ApidApigeeSync")
			return res
		}
	}
	return res
}

func processChange(changes *common.ChangeList, txn *sql.Tx) bool {

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))
	var rows []common.Row
	res := true

	for _, payload := range changes.Changes {
		rows = nil
		switch payload.Table {
		case "edgex.apid_config_scope":
			switch payload.Operation {
			case common.Insert:
				rows = append(rows, payload.NewRow)
				res = insertApidConfigScopes(rows, txn)
			}
		}
		if res == false {
			log.Error("Sql Operation error. Operation rollbacked")
			return res
		}
	}
	return res
}

/*
 * INSERT INTO APP_CREDENTIAL op
 */
func insertApidConfig(rows []common.Row, txn *sql.Tx, snapInfo string) bool {

	var scope, id, name, orgAppName, createdBy, updatedBy, Description string
	var updated, created int64

	prep, err := txn.Prepare("INSERT INTO APID_CONFIG (id, _apid_scope, name, umbrella_org_app_name, created, created_by, updated, updated_by, snapshotInfo)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9);")
	if err != nil {
		log.Error("INSERT APID_CONFIG Failed: ", err)
		return false
	}
	defer prep.Close()

	for _, ele := range rows {
		ele.Get("id", &id)
		ele.Get("_apid_scope", &scope)
		ele.Get("name", &name)
		ele.Get("umbrella_org_app_name", &orgAppName)
		ele.Get("created", &created)
		ele.Get("created_by", &createdBy)
		ele.Get("updated", &updated)
		ele.Get("updated_by", &updatedBy)
		ele.Get("description", &Description)

		s := txn.Stmt(prep)
		_, err = s.Exec(
			id,
			scope,
			name,
			orgAppName,
			created,
			createdBy,
			updated,
			updatedBy,
			snapInfo)
		s.Close()
		if err != nil {
			log.Error("INSERT APID_CONFIG Failed: ", id, ", ", scope, ")", err)
			return false
		} else {
			log.Info("INSERT APID_CONFIG Success: (", id, ", ", scope, ")")
		}
	}
	return true
}

/*
 * INSERT INTO APP_CREDENTIAL op
 */
func insertApidConfigScopes(rows []common.Row, txn *sql.Tx) bool {

	var id, scopeId, apiConfigId, scope, createdBy, updatedBy string
	var created, updated int64

	prep, err := txn.Prepare("INSERT INTO APID_CONFIG_SCOPE (id, _apid_scope, apid_config_id, scope, created, created_by, updated, updated_by)VALUES($1,$2,$3,$4,$5,$6,$7,$8);")
	if err != nil {
		log.Error("INSERT APID_CONFIG_SCOPE Failed: ", err)
		return false
	}
	defer prep.Close()

	for _, ele := range rows {

		ele.Get("id", &id)
		ele.Get("_apid_scope", &scopeId)
		ele.Get("apid_config_id", &apiConfigId)
		ele.Get("scope", &scope)
		ele.Get("created", &created)
		ele.Get("created_by", &createdBy)
		ele.Get("updated", &updated)
		ele.Get("updated_by", &updatedBy)

		s := txn.Stmt(prep)
		_, err = s.Exec(
			id,
			scopeId,
			apiConfigId,
			scope,
			created,
			createdBy,
			updated,
			updatedBy)
		s.Close()

		if err != nil {
			log.Error("INSERT APID_CONFIG_SCOPE Failed: ", id, ", ", scope, ")", err)
			return false
		} else {
			log.Info("INSERT APID_CONFIG_SCOPE Success: (", id, ", ", scope, ")")
		}
	}
	return true
}
