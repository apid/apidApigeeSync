package apidApigeeSync

import (
	"database/sql"
	"github.com/30x/apid"
	"github.com/30x/transicator/common"
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
		case "apid_config":
			for _, row := range payload.Rows {
				insertApidConfig(row, db)
			}
		case "apid_config_scope":
			for _, row := range payload.Rows {
				insertApidConfigScope(row, db)
			}
		}
	}
}

func processChange(changes *common.ChangeList) {

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))

	db, err := data.DB()
	if err != nil {
		panic("Unable to access Sqlite DB")
	}

	for _, payload := range changes.Changes {

		switch payload.Table {
		case "public.apid_config":
		case "edgex.apid_config":
			switch payload.Operation {
			case 1:
				insertApidConfig(payload.NewRow, db)
			}

		case "public.apid_config_scope":
		case "edgex.apid_config_scope":
			switch payload.Operation {
			case 1:
				insertApidConfigScope(payload.NewRow, db)
			}
		}
	}
}

/*
 * INSERT INTO APP_CREDENTIAL op
 */
func insertApidConfig(ele common.Row, db *sql.DB) {

	var scope, id, name, orgAppName, createdBy, updatedBy, Description string
	var updated, created int64

	txn, _ := db.Begin()
	err := ele.Get("id", &id)
	err = ele.Get("_apid_scope", &scope)
	err = ele.Get("name", &name)
	err = ele.Get("umbrella_org_app_name", &orgAppName)
	err = ele.Get("created", &created)
	err = ele.Get("created_by", &createdBy)
	err = ele.Get("updated", &updated)
	err = ele.Get("updated_by", &updatedBy)
	err = ele.Get("description", &Description)

	_, err = txn.Exec("INSERT INTO apid_config (id, _apid_scope, name, umbrella_org_app_name, created, created_by, updated, updated_by)VALUES(?,?,?,?,?,?,?,?);",
		id,
		scope,
		name,
		orgAppName,
		created,
		createdBy,
		updated,
		updatedBy,
		Description)

	if err != nil {
		log.Error("INSERT  Failed: ", id, ", ", scope, ")", err)
		txn.Rollback()
	} else {
		log.Info("INSERT  Success: (", id, ", ", scope, ")")
		txn.Commit()
	}

}

/*
 * INSERT INTO APP_CREDENTIAL op
 */
func insertApidConfigScope(ele common.Row, db *sql.DB) {

	var id, scopeId, apiConfigId, scope, createdBy, updatedBy string
	var created, updated int64

	txn, _ := db.Begin()
	err := ele.Get("id", &id)
	err = ele.Get("_apid_scope", &scopeId)
	err = ele.Get("apid_config_id", &apiConfigId)
	err = ele.Get("scope", &scope)
	err = ele.Get("created", &created)
	err = ele.Get("created_by", &createdBy)
	err = ele.Get("updated", &updated)
	err = ele.Get("updated_by", &updatedBy)

	_, err = txn.Exec("INSERT INTO apid_config_scope (id, _apid_scope, apid_config_id, scope, created, created_by, updated, updated_by)VALUES(?,?,?,?,?,?,?,?);",
		id,
		scopeId,
		apiConfigId,
		scope,
		created,
		createdBy,
		updated,
		updatedBy)

	if err != nil {
		log.Error("INSERT CRED Failed: ", id, ", ", scope, ")", err)
		txn.Rollback()
	} else {
		log.Info("INSERT CRED Success: (", id, ", ", scope, ")")
		txn.Commit()
	}

}
