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
	"database/sql"
	"errors"
	"fmt"
	"github.com/apid/apid-core/util"
	"sync"

	"github.com/apid/apid-core"
	"github.com/apigee-labs/transicator/common"
	"sort"
	"strings"
)

var (
	dbMux sync.RWMutex
)

/*
This plugin uses 2 databases:
1. The default DB is used for APID table.
2. The versioned DB is used for APID_CLUSTER & DATA_SCOPE
(Currently, the snapshot never changes, but this is future-proof)
*/

func creatDbManager() *dbManager {
	return &dbManager{
		DbMux:       &sync.RWMutex{},
		knownTables: make(map[string]bool),
	}
}

type dbManager struct {
	Db          apid.DB
	DbMux       *sync.RWMutex
	dbVersion   string
	knownTables map[string]bool
}

// idempotent call to initialize default DB
func (dbMan *dbManager) initDB() error {
	db, err := dataService.DB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Errorf("initDB(): Unable to get DB tx err: {%v}", err)
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS APID (
	    instance_id text,
	    apid_cluster_id text,
	    last_snapshot_info text,
	    PRIMARY KEY (instance_id)
	);
	`)
	if err != nil {
		log.Errorf("initDB(): Unable to tx exec err: {%v}", err)
		return err
	}
	if err = tx.Commit(); err != nil {
		log.Errorf("Error when initDb: %v", err)
		return err
	}
	log.Debug("Database tables created.")
	return nil
}

func (dbMan *dbManager) getDB() apid.DB {
	dbMux.RLock()
	defer dbMux.RUnlock()
	return dbMan.Db
}

func (dbMan *dbManager) setDB(db apid.DB) {
	dbMux.Lock()
	defer dbMux.Unlock()
	dbMan.Db = db
}

//TODO if len(rows) > 1000, chunk it up and exec multiple inserts in the txn
func (dbMan *dbManager) insert(tableName string, rows []common.Row, txn apid.Tx) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows")
	}

	var orderedColumns []string
	for column := range rows[0] {
		orderedColumns = append(orderedColumns, column)
	}
	sort.Strings(orderedColumns)

	sql := dbMan.buildInsertSql(tableName, orderedColumns, rows)

	prep, err := txn.Prepare(sql)
	if err != nil {
		log.Errorf("INSERT Fail to prepare statement [%s] error=[%v]", sql, err)
		return err
	}
	defer prep.Close()

	var values []interface{}

	for _, row := range rows {
		for _, columnName := range orderedColumns {
			//use Value so that stmt exec does not complain about common.ColumnVal being a struct
			values = append(values, row[columnName].Value)
		}
	}

	//create prepared statement from existing template statement
	_, err = prep.Exec(values...)

	if err != nil {
		log.Errorf("INSERT Fail [%s] values=%v error=[%v]", sql, values, err)
		return err
	}
	log.Debugf("INSERT Success [%s] values=%v", sql, values)

	return nil
}

func (dbMan *dbManager) getValueListFromKeys(row common.Row, pkeys []string) []interface{} {
	var values = make([]interface{}, len(pkeys))
	for i, pkey := range pkeys {
		if row[pkey] == nil {
			values[i] = nil
		} else {
			values[i] = row[pkey].Value
		}
	}
	return values
}

func (dbMan *dbManager) delete(tableName string, rows []common.Row, txn apid.Tx) error {
	pkeys, err := dbMan.getPkeysForTable(tableName)
	sort.Strings(pkeys)
	if len(pkeys) == 0 || err != nil {
		return fmt.Errorf("DELETE No primary keys found for table. %s", tableName)
	}

	if len(rows) == 0 {
		return fmt.Errorf("No rows found for table.", tableName)
	}

	sql := dbMan.buildDeleteSql(tableName, rows[0], pkeys)
	prep, err := txn.Prepare(sql)
	if err != nil {
		return fmt.Errorf("DELETE Fail to prep statement [%s] error=[%v]", sql, err)
	}
	defer prep.Close()
	for _, row := range rows {
		values := dbMan.getValueListFromKeys(row, pkeys)
		// delete prepared statement from existing template statement
		res, err := txn.Stmt(prep).Exec(values...)
		if err != nil {
			return fmt.Errorf("DELETE Fail [%s] values=%v error=[%v]", sql, values, err)
		}
		affected, err := res.RowsAffected()
		if err == nil && affected != 0 {
			log.Debugf("DELETE Success [%s] values=%v", sql, values)
		} else if err == nil && affected == 0 {
			return fmt.Errorf("Entry not found [%s] values=%v. Nothing to delete.", sql, values)
		} else {
			return fmt.Errorf("DELETE Failed [%s] values=%v error=[%v]", sql, values, err)
		}

	}
	return nil

}

// Syntax "DELETE FROM Obj WHERE key1=$1 AND key2=$2 ... ;"
func (dbMan *dbManager) buildDeleteSql(tableName string, row common.Row, pkeys []string) string {

	var wherePlaceholders []string
	i := 1
	if row == nil {
		return ""
	}
	normalizedTableName := normalizeTableName(tableName)

	for _, pk := range pkeys {
		wherePlaceholders = append(wherePlaceholders, fmt.Sprintf("%s=$%v", pk, i))
		i++
	}

	sql := "DELETE FROM " + normalizedTableName
	sql += " WHERE "
	sql += strings.Join(wherePlaceholders, " AND ")

	return sql

}

func (dbMan *dbManager) update(tableName string, oldRows, newRows []common.Row, txn apid.Tx) error {
	pkeys, err := dbMan.getPkeysForTable(tableName)
	if len(pkeys) == 0 || err != nil {
		return fmt.Errorf("UPDATE No primary keys found for table: %v, %v", tableName, err)
	}
	if len(oldRows) == 0 || len(newRows) == 0 {
		return fmt.Errorf("UPDATE No old or new rows, table: %v, %v, %v", tableName, oldRows, newRows)
	}

	var orderedColumns []string

	//extract sorted orderedColumns
	for columnName := range newRows[0] {
		orderedColumns = append(orderedColumns, columnName)
	}
	sort.Strings(orderedColumns)

	//build update statement, use arbitrary row as template
	sql := dbMan.buildUpdateSql(tableName, orderedColumns, newRows[0], pkeys)
	prep, err := txn.Prepare(sql)
	if err != nil {
		return fmt.Errorf("UPDATE Fail to prep statement [%s] error=[%v]", sql, err)
	}
	defer prep.Close()

	for i, row := range newRows {
		var values []interface{}

		for _, columnName := range orderedColumns {
			//use Value so that stmt exec does not complain about common.ColumnVal being a struct
			//TODO will need to convert the Value (which is a string) to the appropriate field, using type for mapping
			//TODO right now this will only work when the column type is a string
			if row[columnName] != nil {
				values = append(values, row[columnName].Value)
			} else {
				values = append(values, nil)
			}
		}

		//add values for where clause, use PKs of old row
		for _, pk := range pkeys {
			if oldRows[i][pk] != nil {
				values = append(values, oldRows[i][pk].Value)
			} else {
				values = append(values, nil)
			}

		}

		//create prepared statement from existing template statement
		res, err := txn.Stmt(prep).Exec(values...)

		if err != nil {
			return fmt.Errorf("UPDATE Fail [%s] values=%v error=[%v]", sql, values, err)
		}
		numRowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("UPDATE Fail [%s] values=%v error=[%v]", sql, values, err)
		}
		//delete this once we figure out why tests are failing/not updating
		log.Debugf("NUM ROWS AFFECTED BY UPDATE: %d", numRowsAffected)
		log.Debugf("UPDATE Success [%s] values=%v", sql, values)

	}

	return nil

}

func (dbMan *dbManager) buildUpdateSql(tableName string, orderedColumns []string, row common.Row, pkeys []string) string {
	if row == nil {
		return ""
	}
	normalizedTableName := normalizeTableName(tableName)

	var setPlaceholders, wherePlaceholders []string
	i := 1

	for _, columnName := range orderedColumns {
		setPlaceholders = append(setPlaceholders, fmt.Sprintf("%s=$%v", columnName, i))
		i++
	}

	for _, pk := range pkeys {
		wherePlaceholders = append(wherePlaceholders, fmt.Sprintf("%s=$%v", pk, i))
		i++
	}

	sql := "UPDATE " + normalizedTableName + " SET "
	sql += strings.Join(setPlaceholders, ", ")
	sql += " WHERE "
	sql += strings.Join(wherePlaceholders, " AND ")

	return sql
}

//precondition: rows.length > 1000, max number of entities for sqlite
func (dbMan *dbManager) buildInsertSql(tableName string, orderedColumns []string, rows []common.Row) string {
	if len(rows) == 0 {
		return ""
	}
	normalizedTableName := normalizeTableName(tableName)
	var values string = ""

	var i, j int
	k := 1
	for i = 0; i < len(rows)-1; i++ {
		values += "("
		for j = 0; j < len(orderedColumns)-1; j++ {
			values += fmt.Sprintf("$%d,", k)
			k++
		}
		values += fmt.Sprintf("$%d),", k)
		k++
	}
	values += "("
	for j = 0; j < len(orderedColumns)-1; j++ {
		values += fmt.Sprintf("$%d,", k)
		k++
	}
	values += fmt.Sprintf("$%d)", k)

	sql := "INSERT INTO " + normalizedTableName
	sql += "(" + strings.Join(orderedColumns, ",") + ") "
	sql += "VALUES " + values

	return sql
}

func (dbMan *dbManager) getPkeysForTable(tableName string) ([]string, error) {
	db := dbMan.getDB()
	normalizedTableName := normalizeTableName(tableName)
	sql := "SELECT columnName FROM _transicator_tables WHERE tableName=$1 AND primaryKey ORDER BY columnName;"
	rows, err := db.Query(sql, normalizedTableName)
	if err != nil {
		log.Errorf("Failed [%s] values=[s%] Error: %v", sql, normalizedTableName, err)
		return nil, err
	}
	var columnNames []string
	defer rows.Close()
	for rows.Next() {
		var value string
		err := rows.Scan(&value)
		if err != nil {
			log.Errorf("failed to scan column names: %v", err)
			return nil, err
		}
		columnNames = append(columnNames, value)
	}
	err = rows.Err()
	if err != nil {
		log.Errorf("failed to scan column names: %v", err)
		return nil, err
	}
	return columnNames, nil
}

func normalizeTableName(tableName string) string {
	return strings.Replace(tableName, ".", "_", 1)
}

/*
 * For the given apidConfigId, this function will retrieve all the distinch scopes
 * associated with it. Distinct, because scope is already a collection of the tenants.
 */
func (dbMan *dbManager) findScopesForId(configId string) (scopes []string, err error) {

	log.Debugf("findScopesForId: %s", configId)
	var scope sql.NullString
	db := dbMan.getDB()
	query := `
		SELECT scope FROM edgex_data_scope WHERE apid_cluster_id = $1
		UNION
		SELECT org_scope FROM edgex_data_scope WHERE apid_cluster_id = $2
		UNION
		SELECT env_scope FROM edgex_data_scope WHERE apid_cluster_id = $3
	`
	rows, err := db.Query(query, configId, configId, configId)
	if err != nil {
		log.Errorf("Failed to query EDGEX_DATA_SCOPE: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&scope)
		if err != nil {
			log.Errorf("Failed to get scopes from EDGEX_DATA_SCOPE: %v", err)
			return
		}
		if scope.Valid && scope.String != "" {
			scopes = append(scopes, scope.String)
		}
	}

	log.Debugf("scopes: %v", scopes)
	return
}

/*
 * Retrieve SnapshotInfo for the given apidConfigId from apid_config table
 */
func (dbMan *dbManager) getLastSequence() (lastSequence string) {

	err := dbMan.getDB().QueryRow("select last_sequence from EDGEX_APID_CLUSTER LIMIT 1").Scan(&lastSequence)
	if err != nil && err != sql.ErrNoRows {
		log.Panicf("Failed to query EDGEX_APID_CLUSTER: %v", err)
		return
	}

	log.Debugf("lastSequence: %s", lastSequence)
	return
}

/*
 * Persist the last change Id each time a change has been successfully
 * processed by the plugin(s)
 */
func (dbMan *dbManager) updateLastSequence(lastSequence string) error {

	log.Debugf("updateLastSequence: %s", lastSequence)

	tx, err := dbMan.getDB().Begin()
	if err != nil {
		log.Errorf("getApidInstanceInfo: Unable to get DB tx Err: {%v}", err)
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec("UPDATE EDGEX_APID_CLUSTER SET last_sequence=?;", lastSequence)
	if err != nil {
		log.Errorf("UPDATE EDGEX_APID_CLUSTER Failed: %v", err)
		return err
	}
	log.Debugf("UPDATE EDGEX_APID_CLUSTER Success: %s", lastSequence)
	if err = tx.Commit(); err != nil {
		log.Errorf("Commit error in updateLastSequence: %v", err)
	}
	return err
}

func (dbMan *dbManager) getApidInstanceInfo() (info apidInstanceInfo, err error) {
	info.InstanceName = config.GetString(configName)
	info.ClusterID = config.GetString(configApidClusterId)
	var savedClusterId string

	// always use default database for this
	var db apid.DB
	db, err = dataService.DB()
	if err != nil {
		return
	}
	tx, err := db.Begin()
	if err != nil {
		log.Errorf("getApidInstanceInfo: Unable to get DB tx Err: {%v}", err)
		return
	}
	defer tx.Rollback()
	err = tx.QueryRow("SELECT instance_id, apid_cluster_id, last_snapshot_info FROM APID LIMIT 1").
		Scan(&info.InstanceID, &savedClusterId, &info.LastSnapshot)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Errorf("Unable to retrieve apidInstanceInfo: %v", err)
			return
		} else {
			// first start - no row, generate a UUID and store it
			err = nil
			info.IsNewInstance = true
			info.InstanceID = util.GenerateUUID()

			log.Debugf("Inserting new apid instance id %s", info.InstanceID)
			_, err = tx.Exec("INSERT INTO APID (instance_id, apid_cluster_id, last_snapshot_info) VALUES (?,?,?)",
				info.InstanceID, info.ClusterID, "")
		}
	} else if savedClusterId != info.ClusterID {
		log.Warn("Detected apid cluster id change in config.  Apid will start clean")
		err = nil
		info.IsNewInstance = true
		info.InstanceID = util.GenerateUUID()

		_, err = tx.Exec("DELETE FROM APID;")

		info.LastSnapshot = ""
	}
	if err = tx.Commit(); err != nil {
		log.Errorf("Commit error in getApidInstanceInfo: %v", err)
	}
	return
}

func (dbMan *dbManager) updateApidInstanceInfo(instanceId, clusterId, lastSnap string) error {

	// always use default database for this
	db, err := dataService.DB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Errorf("updateApidInstanceInfo: Unable to get DB tx Err: {%v}", err)
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Exec(`
		REPLACE
		INTO APID (instance_id, apid_cluster_id, last_snapshot_info)
		VALUES (?,?,?)`,
		instanceId, clusterId, lastSnap)
	if err != nil {
		log.Errorf("updateApidInstanceInfo: Tx Exec Err: {%v}", err)
		return err
	}
	n, err := rows.RowsAffected()
	if err == nil && n == 0 {
		err = errors.New("no rows affected")
	}
	if err = tx.Commit(); err != nil {
		log.Errorf("Commit error in updateApidInstanceInfo: %v", err)
	}

	return err
}

func (dbMan *dbManager) extractTables() (map[string]bool, error) {
	tables := make(map[string]bool)
	db := dbMan.getDB()
	rows, err := db.Query("SELECT DISTINCT tableName FROM _transicator_tables;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var table sql.NullString
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		log.Debugf("Table %v found in existing db", table)
		if table.Valid {
			tables[table.String] = true
		}
	}
	log.Debugf("Extracting table names from existing DB %v", tables)
	return tables, nil
}

func (dbMan *dbManager) getKnowTables() map[string]bool {
	return dbMan.knownTables
}

func (dbMan *dbManager) processChangeList(changes *common.ChangeList) error {

	tx, err := dbMan.getDB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))

	for _, change := range changes.Changes {
		if change.Table == LISTENER_TABLE_APID_CLUSTER {
			return fmt.Errorf("illegal operation: %s for %s", change.Operation, change.Table)
		}
		switch change.Operation {
		case common.Insert:
			err = dbMan.insert(change.Table, []common.Row{change.NewRow}, tx)
		case common.Update:
			if change.Table == LISTENER_TABLE_DATA_SCOPE {
				return fmt.Errorf("illegal operation: %s for %s", change.Operation, change.Table)
			}
			err = dbMan.update(change.Table, []common.Row{change.OldRow}, []common.Row{change.NewRow}, tx)
		case common.Delete:
			err = dbMan.delete(change.Table, []common.Row{change.OldRow}, tx)
		}
		if err != nil {
			return err
		}

	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("Commit error in processChangeList: %v", err)
	}
	return nil
}

func (dbMan *dbManager) processSnapshot(snapshot *common.Snapshot, isDataSnapshot bool) error {

	var prevDb string
	if apidInfo.LastSnapshot != "" && apidInfo.LastSnapshot != snapshot.SnapshotInfo {
		log.Debugf("Release snapshot for {%s}. Switching to version {%s}",
			apidInfo.LastSnapshot, snapshot.SnapshotInfo)
		prevDb = apidInfo.LastSnapshot
	} else {
		log.Debugf("Process snapshot for version {%s}",
			snapshot.SnapshotInfo)
	}
	db, err := dataService.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		return fmt.Errorf("Unable to access database: %v", err)
	}

	var numApidClusters int
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Unable to open DB txn: {%v}", err.Error())
	}
	defer tx.Rollback()
	err = tx.QueryRow("SELECT COUNT(*) FROM edgex_apid_cluster").Scan(&numApidClusters)
	if err != nil {
		return fmt.Errorf("Unable to read database: {%s}", err.Error())
	}

	if numApidClusters != 1 {
		return fmt.Errorf("Illegal state for apid_cluster. Must be a single row.")
	}

	_, err = tx.Exec("ALTER TABLE edgex_apid_cluster ADD COLUMN last_sequence text DEFAULT ''")
	if err != nil && err.Error() != "duplicate column name: last_sequence" {
		return fmt.Errorf("Unable to create last_sequence column on DB.  Error {%v}", err.Error())
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("Error when commit in processSqliteSnapshot: %v", err)
	}

	//update apid instance info
	apidInfo.LastSnapshot = snapshot.SnapshotInfo
	err = dbMan.updateApidInstanceInfo(apidInfo.InstanceID, apidInfo.ClusterID, apidInfo.LastSnapshot)
	if err != nil {
		return fmt.Errorf("Unable to update instance info: %v", err)
	}

	dbMan.setDB(db)
	if isDataSnapshot {
		dbMan.knownTables, err = dbMan.extractTables()
		if err != nil {
			return fmt.Errorf("Unable to extract tables: %v", err)
		}
	}
	log.Debugf("Snapshot processed: %s", snapshot.SnapshotInfo)

	// Releases the DB, when the Connection reference count reaches 0.
	if prevDb != "" {
		dataService.ReleaseDB(prevDb)
	}
	return nil
}
