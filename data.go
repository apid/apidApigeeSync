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
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	"sort"
	"strings"
)

var (
	unsafeDB apid.DB
	dbMux    sync.RWMutex
)

type dataApidCluster struct {
	ID, Name, OrgAppName, CreatedBy, UpdatedBy, Description string
	Updated, Created                                        string
}

type dataDataScope struct {
	ID, ClusterID, Scope, Org, Env, CreatedBy, UpdatedBy string
	Updated, Created                                     string
}

/*
This plugin uses 2 databases:
1. The default DB is used for APID table.
2. The versioned DB is used for APID_CLUSTER & DATA_SCOPE
(Currently, the snapshot never changes, but this is future-proof)
*/
func initDB(db apid.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS APID (
	    instance_id text,
	    apid_cluster_id text,
	    last_snapshot_info text,
	    PRIMARY KEY (instance_id)
	);
	`)
	if err != nil {
		return err
	}

	log.Debug("Database tables created.")
	return nil
}

func getDB() apid.DB {
	dbMux.RLock()
	db := unsafeDB
	dbMux.RUnlock()
	return db
}

func setDB(db apid.DB) {
	dbMux.Lock()
	unsafeDB = db
	dbMux.Unlock()
}

//TODO if len(rows) > 1000, chunk it up and exec multiple inserts in the txn
func insert(tableName string, rows []common.Row, txn apid.Tx) bool {

	if len(rows) == 0 {
		return false
	}

	var orderedColumns []string
	for column := range rows[0] {
		orderedColumns = append(orderedColumns, column)
	}
	sort.Strings(orderedColumns)

	sql := buildInsertSql(tableName, orderedColumns, rows)

	prep, err := txn.Prepare(sql)
	if err != nil {
		log.Errorf("INSERT Fail to prepare statement [%s] error=[%v]", sql, err)
		return false
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
		return false
	}
	log.Debugf("INSERT Success [%s] values=%v", sql, values)

	return true
}

func getValueListFromKeys(row common.Row, pkeys []string) []interface{} {
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

func _delete(tableName string, rows []common.Row, txn apid.Tx) bool {
	pkeys, err := getPkeysForTable(tableName)
	sort.Strings(pkeys)
	if len(pkeys) == 0 || err != nil {
		log.Errorf("DELETE No primary keys found for table. %s", tableName)
		return false
	}

	if len(rows) == 0 {
		log.Errorf("No rows found for table.", tableName)
		return false
	}

	sql := buildDeleteSql(tableName, rows[0], pkeys)
	prep, err := txn.Prepare(sql)
	if err != nil {
		log.Errorf("DELETE Fail to prep statement [%s] error=[%v]", sql, err)
		return false
	}
	defer prep.Close()
	for _, row := range rows {
		values := getValueListFromKeys(row, pkeys)
		// delete prepared statement from existing template statement
		res, err := txn.Stmt(prep).Exec(values...)
		if err != nil {
			log.Errorf("DELETE Fail [%s] values=%v error=[%v]", sql, values, err)
			return false
		}
		affected, err := res.RowsAffected()
		if err == nil && affected != 0 {
			log.Debugf("DELETE Success [%s] values=%v", sql, values)
		} else if err == nil && affected == 0 {
			log.Errorf("Entry not found [%s] values=%v. Nothing to delete.", sql, values)
			return false
		} else {
			log.Errorf("DELETE Failed [%s] values=%v error=[%v]", sql, values, err)
			return false
		}

	}
	return true

}

// Syntax "DELETE FROM Obj WHERE key1=$1 AND key2=$2 ... ;"
func buildDeleteSql(tableName string, row common.Row, pkeys []string) string {

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

func update(tableName string, oldRows, newRows []common.Row, txn apid.Tx) bool {
	pkeys, err := getPkeysForTable(tableName)
	if len(pkeys) == 0 || err != nil {
		log.Errorf("UPDATE No primary keys found for table.", tableName)
		return false
	}
	if len(oldRows) == 0 || len(newRows) == 0 {
		return false
	}

	var orderedColumns []string

	//extract sorted orderedColumns
	for columnName := range newRows[0] {
		orderedColumns = append(orderedColumns, columnName)
	}
	sort.Strings(orderedColumns)

	//build update statement, use arbitrary row as template
	sql := buildUpdateSql(tableName, orderedColumns, newRows[0], pkeys)
	prep, err := txn.Prepare(sql)
	if err != nil {
		log.Errorf("UPDATE Fail to prep statement [%s] error=[%v]", sql, err)
		return false
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
			log.Errorf("UPDATE Fail [%s] values=%v error=[%v]", sql, values, err)
			return false
		}
		numRowsAffected, err := res.RowsAffected()
		if err != nil {
			log.Errorf("UPDATE Fail [%s] values=%v error=[%v]", sql, values, err)
			return false
		}
		//delete this once we figure out why tests are failing/not updating
		log.Debugf("NUM ROWS AFFECTED BY UPDATE: %d", numRowsAffected)
		log.Debugf("UPDATE Success [%s] values=%v", sql, values)

	}

	return true

}

func buildUpdateSql(tableName string, orderedColumns []string, row common.Row, pkeys []string) string {
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
func buildInsertSql(tableName string, orderedColumns []string, rows []common.Row) string {
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

func getPkeysForTable(tableName string) ([]string, error) {
	db := getDB()
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
			log.Fatal(err)
		}
		columnNames = append(columnNames, value)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
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
func findScopesForId(configId string) (scopes []string) {

	log.Debugf("findScopesForId: %s", configId)

	var scope sql.NullString
	db := getDB()

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
func getLastSequence() (lastSequence string) {

	err := getDB().QueryRow("select last_sequence from EDGEX_APID_CLUSTER LIMIT 1").Scan(&lastSequence)
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
func updateLastSequence(lastSequence string) error {

	log.Debugf("updateLastSequence: %s", lastSequence)

	stmt, err := getDB().Prepare("UPDATE EDGEX_APID_CLUSTER SET last_sequence=$1;")
	if err != nil {
		log.Errorf("UPDATE EDGEX_APID_CLUSTER Failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(lastSequence)
	if err != nil {
		log.Errorf("UPDATE EDGEX_APID_CLUSTER Failed: %v", err)
		return err
	}

	log.Debugf("UPDATE EDGEX_APID_CLUSTER Success: %s", lastSequence)
	log.Infof("Replication lastSequence=%s", lastSequence)
	return nil
}

func getApidInstanceInfo() (info apidInstanceInfo, err error) {
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

	err = tx.QueryRow("SELECT instance_id, apid_cluster_id, last_snapshot_info FROM APID LIMIT 1").
		Scan(&info.InstanceID, &savedClusterId, &info.LastSnapshot)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Errorf("Unable to retrieve apidInstanceInfo: %v", err)
			tx.Rollback()
			return
		} else {
			// first start - no row, generate a UUID and store it
			err = nil
			newInstanceID = true
			info.InstanceID = GenerateUUID()

			log.Debugf("Inserting new apid instance id %s", info.InstanceID)
			_, err = tx.Exec("INSERT INTO APID (instance_id, apid_cluster_id, last_snapshot_info) VALUES (?,?,?)",
				info.InstanceID, info.ClusterID, "")
		}
	} else if savedClusterId != info.ClusterID {
		log.Debug("Detected apid cluster id change in config.  Apid will start clean")
		err = nil
		newInstanceID = true
		info.InstanceID = GenerateUUID()

		_, err = tx.Exec("REPLACE INTO APID (instance_id, apid_cluster_id, last_snapshot_info) VALUES (?,?,?)",
			info.InstanceID, info.ClusterID, "")
		info.LastSnapshot = ""
	}
	if err != nil {
		err = tx.Commit()
		if err != nil {
			rollbackTxn(tx)
		}
	} else {
		rollbackTxn(tx)
	}
	return
}

func updateApidInstanceInfo() error {

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
	rows, err := tx.Exec(`
		REPLACE
		INTO APID (instance_id, apid_cluster_id, last_snapshot_info)
		VALUES (?,?,?)`,
		apidInfo.InstanceID, apidInfo.ClusterID, apidInfo.LastSnapshot)
	if err != nil {
		log.Errorf("updateApidInstanceInfo: Tx Exec Err: {%v}", err)
		rollbackTxn(tx)
		return err
	}
	n, err := rows.RowsAffected()
	if err == nil && n == 0 {
		err = errors.New("no rows affected")
		rollbackTxn(tx)
	} else if err == nil {
		err = tx.Commit()
		if err != nil {
			rollbackTxn(tx)
		}
	} else {
		rollbackTxn(tx)
	}

	return err
}

/*
 * generates a random uuid (mix of timestamp & crypto random string)
 */

//TODO: Change to https://tools.ietf.org/html/rfc4122 based implementation such as https://github.com/google/uuid
func GenerateUUID() string {

	buff := make([]byte, 16)
	numRead, err := rand.Read(buff)
	if numRead != len(buff) || err != nil {
		panic(err)
	}
	/* uuid v4 spec */
	buff[6] = (buff[6] | 0x40) & 0x4F
	buff[8] = (buff[8] | 0x80) & 0xBF
	return fmt.Sprintf("%x-%x-%x-%x-%x", buff[0:4], buff[4:6], buff[6:8], buff[8:10], buff[10:])
}
