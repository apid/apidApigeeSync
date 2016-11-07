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

// todo: The following was basically just copied from old APID - needs review.

func (h *handler) Handle(e apid.Event) {

	snapData, ok := e.(*common.Snapshot)
	if ok {
		processSnapshot(snapData)
	} else {
		changeSet, ok := e.(*common.ChangeList)
		if ok {
			processChange(changeSet)
		} else {
			log.Errorf("Received Invalid event. This shouldn't happen!")
		}
	}
	return
}

func processSnapshot(snapshot *common.Snapshot) {

	log.Debugf("Process Snapshot data")

	db, err := data.DB()
	if err != nil {
		panic("Unable to access Sqlite DB")
	}

	for _, payload := range snapshot.Tables {

		switch payload.Name {
		case "edgex.apid_config":
			for _, row := range payload.Rows {
				insertApidConfig(row, db, snapshot.SnapshotInfo)
			}
		case "edgex.apid_config_scope":
			insertApidConfigScopes(payload.Rows, db)
		}
	}
}

func processChange(changes *common.ChangeList) {

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))
	var rows []common.Row
	db, err := data.DB()
	if err != nil {
		panic("Unable to access Sqlite DB")
	}

	for _, payload := range changes.Changes {
		rows = nil
		switch payload.Table {
		case "edgex.apid_config_scope":
			switch payload.Operation {
			case 1:
				rows = append(rows, payload.NewRow)
				insertApidConfigScopes(rows, db)
			}
		}
	}
}

/*
 * INSERT INTO APP_CREDENTIAL op
 */
func insertApidConfig(ele common.Row, db *sql.DB, snapInfo string) bool {

	var scope, id, name, orgAppName, createdBy, updatedBy, Description string
	var updated, created int64

	prep, err := db.Prepare("INSERT INTO APID_CONFIG (id, _apid_scope, name, umbrella_org_app_name, created, created_by, updated, updated_by, snapshotInfo)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9);")
	if err != nil {
		log.Error("INSERT APID_CONFIG Failed: ", err)
		return false
	}

	txn, err := db.Begin()

	ele.Get("id", &id)
	ele.Get("_apid_scope", &scope)
	ele.Get("name", &name)
	ele.Get("umbrella_org_app_name", &orgAppName)
	ele.Get("created", &created)
	ele.Get("created_by", &createdBy)
	ele.Get("updated", &updated)
	ele.Get("updated_by", &updatedBy)
	ele.Get("description", &Description)

	_, err = txn.Stmt(prep).Exec(
		id,
		scope,
		name,
		orgAppName,
		created,
		createdBy,
		updated,
		updatedBy,
		snapInfo)

	if err != nil {
		log.Error("INSERT APID_CONFIG Failed: ", id, ", ", scope, ")", err)
		txn.Rollback()
		return false
	} else {
		log.Info("INSERT APID_CONFIG Success: (", id, ", ", scope, ")")
		txn.Commit()
		return true
	}

}

/*
 * INSERT INTO APP_CREDENTIAL op
 */
func insertApidConfigScopes(rows []common.Row, db *sql.DB) bool {

	var id, scopeId, apiConfigId, scope, createdBy, updatedBy string
	var created, updated int64

	prep, err := db.Prepare("INSERT INTO APID_CONFIG_SCOPE (id, _apid_scope, apid_config_id, scope, created, created_by, updated, updated_by)VALUES($1,$2,$3,$4,$5,$6,$7,$8);")
	if err != nil {
		log.Error("INSERT APID_CONFIG_SCOPE Failed: ", err)
		return false
	}

	txn, err := db.Begin()
	for _, ele := range rows {

		ele.Get("id", &id)
		ele.Get("_apid_scope", &scopeId)
		ele.Get("apid_config_id", &apiConfigId)
		ele.Get("scope", &scope)
		ele.Get("created", &created)
		ele.Get("created_by", &createdBy)
		ele.Get("updated", &updated)
		ele.Get("updated_by", &updatedBy)

		_, err = txn.Stmt(prep).Exec(
			id,
			scopeId,
			apiConfigId,
			scope,
			created,
			createdBy,
			updated,
			updatedBy)

		if err != nil {
			log.Error("INSERT APID_CONFIG_SCOPE Failed: ", id, ", ", scope, ")", err)
			txn.Rollback()
			return false
		} else {
			log.Info("INSERT APID_CONFIG_SCOPE Success: (", id, ", ", scope, ")")
		}
	}
	txn.Commit()
	return true
}
