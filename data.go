package apidApigeeSync

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/30x/apid-core"
)

const (
	sqlTimeFormat = "2006-01-02 15:04:05.999 -0700 MST"
	iso8601       = "2006-01-02T15:04:05.999Z07:00"
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

	//replace to accomodate same snapshot txid
	stmt, err := txn.Prepare(`
	REPLACE INTO APID_CLUSTER
		(id, description, name, umbrella_org_app_name,
		created, created_by, updated, updated_by,
		last_sequence)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);
	`)
	if err != nil {
		log.Errorf("prepare insert into APID_CLUSTER transaction Failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		dac.ID, dac.Description, dac.Name, dac.OrgAppName,
		dac.Created, dac.CreatedBy, dac.Updated, dac.UpdatedBy,
		"")

	if err != nil {
		log.Errorf("insert APID_CLUSTER failed: %v", err)
	}

	return err
}

func insertDataScope(ds dataDataScope, txn *sql.Tx) error {

	log.Debugf("insert DATA_SCOPE: %v", ds)

	//replace to accomodate same snapshot txid
	stmt, err := txn.Prepare(`
	REPLACE INTO DATA_SCOPE
		(id, apid_cluster_id, scope, org,
		env, created, created_by, updated,
		updated_by)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9);
	`)
	if err != nil {
		log.Errorf("insert DATA_SCOPE failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		ds.ID, ds.ClusterID, ds.Scope, ds.Org,
		ds.Env, ds.Created, ds.CreatedBy, ds.Updated,
		ds.UpdatedBy)

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
 * For the given apidConfigId, this function will retrieve all the distinch scopes
 * associated with it. Distinct, because scope is already a collection of the tenants.
 */
func findScopesForId(configId string) (scopes []string) {

	log.Debugf("findScopesForId: %s", configId)

	var scope string
	db := getDB()

	rows, err := db.Query("select DISTINCT scope from DATA_SCOPE where apid_cluster_id = $1", configId)
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
func getLastSequence() (lastSequence string) {

	err := getDB().QueryRow("select last_sequence from APID_CLUSTER LIMIT 1").Scan(&lastSequence)
	if err != nil && err != sql.ErrNoRows {
		log.Panicf("Failed to query APID_CLUSTER: %v", err)
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

	stmt, err := getDB().Prepare("UPDATE APID_CLUSTER SET last_sequence=$1;")
	if err != nil {
		log.Errorf("UPDATE APID_CLUSTER Failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(lastSequence)
	if err != nil {
		log.Errorf("UPDATE APID_CLUSTER Failed: %v", err)
		return err
	}

	log.Infof("UPDATE APID_CLUSTER Success: %s", lastSequence)

	return nil
}

func getApidInstanceInfo() (info apidInstanceInfo, err error) {
	info.InstanceName = config.GetString(configName)
	info.ClusterID = config.GetString(configApidClusterId)

	// always use default database for this
	var db apid.DB
	db, err = dataService.DB()
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
			newInstanceID = true
			info.InstanceID = generateUUID()

			log.Debugf("Inserting new apid instance id %s", info.InstanceID)
			db.Exec("INSERT INTO APID (instance_id, last_snapshot_info) VALUES (?,?)",
				info.InstanceID, "")
		}
	}
	return
}

func updateApidInstanceInfo() error {

	// always use default database for this
	db, err := dataService.DB()
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

//TODO: Change to https://tools.ietf.org/html/rfc4122 based implementation such as https://github.com/google/uuid
func generateUUID() string {

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
