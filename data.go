package apidApigeeSync

import (
	"database/sql"
	"github.com/30x/apid"
	"sync"
	"time"
	"fmt"
	"crypto/rand"
	"errors"
)

var (
	unsafeDB apid.DB
	dbMux    sync.RWMutex
)

type dataApidCluster struct {
	ChangeSelector, ID, Name, OrgAppName, CreatedBy, UpdatedBy, Description string
	Updated, Created string
}

type dataDataScope struct {
	ChangeSelector, ID, ClusterID, Scope, Org, Env, CreatedBy, UpdatedBy string
	Updated, Created string
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
	    last_snapshot_info text,
	    PRIMARY KEY (instance_id)
	);
	CREATE TABLE IF NOT EXISTS APID_CLUSTER (
	    id text,
	    name text,
	    description text,
	    umbrella_org_app_name text,
	    created text,
	    created_by text,
	    updated text,
	    updated_by text,
	    _change_selector text,
	    last_sequence text,
	    PRIMARY KEY (id)
	);
	CREATE TABLE IF NOT EXISTS DATA_SCOPE (
	    id text,
	    apid_cluster_id text,
	    scope text,
	    org text,
	    env text,
	    created text,
	    created_by text,
	    updated text,
	    updated_by text,
	    _change_selector text,
	    PRIMARY KEY (id, apid_cluster_id)
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

func insertApidCluster(dac dataApidCluster, txn *sql.Tx) error {

	log.Debugf("inserting into APID_CLUSTER: %v", dac)

	stmt, err := txn.Prepare(`
	INSERT INTO APID_CLUSTER
		(id, _change_selector, name, umbrella_org_app_name,
		created, created_by, updated, updated_by,
		description)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);
	`)
	if err != nil {
		log.Errorf("prepare insert into APID_CLUSTER transaction Failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		dac.ID, dac.ChangeSelector, dac.Name, dac.OrgAppName,
		dac.Created, dac.CreatedBy, dac.Updated, dac.UpdatedBy,
		dac.Description)

	if err != nil {
		log.Errorf("insert APID_CLUSTER failed: %v", err)
	}

	return err
}

func insertDataScope(ds dataDataScope, txn *sql.Tx) error {

	log.Debugf("insert DATA_SCOPE: %v", ds)

	stmt, err := txn.Prepare(`
	INSERT INTO DATA_SCOPE
		(id, apid_cluster_id, scope, org,
		env, created, created_by, updated,
		updated_by, _change_selector)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);
	`)
	if err != nil {
		log.Errorf("insert DATA_SCOPE failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		ds.ID, ds.ClusterID, ds.Scope, ds.Org,
		ds.Env, ds.Created, ds.CreatedBy, ds.Updated,
		ds.UpdatedBy, ds.ChangeSelector)

	if err != nil {
		log.Errorf("insert DATA_SCOPE failed: %v", err)
		return err
	}

	return nil
}

func deleteDataScope(ds dataDataScope, txn *sql.Tx) error {

	log.Debugf("delete DATA_SCOPE: %v", ds)

	stmt, err := txn.Prepare("DELETE FROM DATA_SCOPE WHERE id=$1 and apid_cluster_id=$2")
	if err != nil {
		log.Errorf("update DATA_SCOPE failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(ds.ID, ds.ClusterID)

	if err != nil {
		log.Errorf("delete DATA_SCOPE failed: %v", err)
		return err
	}

	return nil
}

/*
 * For the given apidConfigId, this function will retrieve all the scopes
 * associated with it
 */
func findScopesForId(configId string) (scopes []string) {

	log.Debugf("findScopesForId: %s", configId)

	var scope string
	db := getDB()

	rows, err := db.Query("select scope from DATA_SCOPE where apid_cluster_id = $1", configId)
	if err != nil {
		log.Errorf("Failed to query DATA_SCOPE: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&scope)
		scopes = append(scopes, scope)
	}

	log.Debugf("scopes: %v", scopes)
	return
}

/*
 * Retrieve SnapshotInfo for the given apidConfigId from apid_config table
 */
func findApidConfigInfo(qparam string) (info string) {

	log.Debugf("findApidConfigInfo: %s", qparam)

	db := getDB()

	rows, err := db.Query("select ? from APID_CLUSTER", qparam)
	if err != nil {
		log.Errorf("Failed to query APID_CLUSTER: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&info)
	}

	log.Debugf("info: %s", info)

	return
}

/*
 * Persist the last change Id each time a change has been successfully
 * processed by the plugin(s)
 */
func persistChange(lastChange string) error {

	log.Debugf("persistChange: %s", lastChange)

	db := getDB()

	stmt, err := db.Prepare("UPDATE APID_CLUSTER SET last_sequence=$1;")
	if err != nil {
		log.Errorf("UPDATE APID_CLUSTER Failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(lastChange)
	if err != nil {
		log.Errorf("UPDATE DATA_SCOPE Failed: %v", err)
		return err
	}

	log.Infof("UPDATE DATA_SCOPE Success: %s", lastChange)

	return nil
}

func getApidInstanceInfo() (info apidInstanceInfo, err error) {

	// always use default database for this
	var db apid.DB
	db, err = data.DB()
	if err != nil {
		return
	}

	err = db.QueryRow("SELECT instance_id, last_snapshot_info FROM APID LIMIT 1").
		Scan(&info.InstanceID, &info.LastSnapshot)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Errorf("Unable to retrieve apidInstanceInfo: %v", err)
			return
		} else {
			// first start - no row, generate a UUID and store it
			err = nil
			info.InstanceID = generateUUID()

			db.Exec("INSERT INTO APID (instance_id) VALUES (?)", info.InstanceID)
		}
	}

	// if name not explicitly configured, just use InstanceID
	config.SetDefault(configName, info.InstanceID)
	info.InstanceName = config.GetString(configName)

	// not stored in DB
	info.ClusterID = config.GetString(configApidClusterId)

	return
}

func updateApidInstanceInfo() error {

	// always use default database for this
	db, err := data.DB()
	if err != nil {
		return err
	}

	rows, err := db.Exec(`
		INSERT OR REPLACE
		INTO APID (instance_id, last_snapshot_info)
		VALUES (?, ?)`,
		apidInfo.InstanceID, apidInfo.LastSnapshot)
	if err != nil {
		return err
	}
	n, err := rows.RowsAffected()
	if err == nil && n == 0 {
		err = errors.New("no rows affected")
	}

	return err
}

/*
 * generates a random uuid (mix of timestamp & crypto random string)
 */
func generateUUID() string {

	unix32bits := uint32(time.Now().UTC().Unix())
	buff := make([]byte, 12)
	numRead, err := rand.Read(buff)
	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}
