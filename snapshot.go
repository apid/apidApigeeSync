package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"net/http"
	"encoding/json"
	"os"
	"github.com/30x/apid-core/data"
	"github.com/apigee-labs/transicator/common"

	"io"
	"time"
	"net/url"
	"path"
	"io/ioutil"
)

// retrieve boot information: apid_config and apid_config_scope
func downloadBootSnapshot(quitPolling chan bool) {
	log.Debug("download Snapshot for boot data")

	scopes := []string{apidInfo.ClusterID}
	snapshot := &common.Snapshot{}
	downloadSnapshot(scopes, snapshot, quitPolling)
	// note that for boot snapshot case, we don't need to inform plugins as they'll get the data snapshot
	processSnapshot(snapshot)
}

// use the scope IDs from the boot snapshot to get all the data associated with the scopes
func downloadDataSnapshot(quitPolling chan bool) {
	log.Debug("download Snapshot for data scopes")

	var scopes = findScopesForId(apidInfo.ClusterID)
	scopes = append(scopes, apidInfo.ClusterID)
	snapshot := &common.Snapshot{}
	downloadSnapshot(scopes, snapshot, quitPolling)

	knownTables = extractTablesFromSnapshot(snapshot)

	db, err := dataService.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Database inaccessible: %v", err)
	}
	persistKnownTablesToDB(knownTables, db)

	log.Info("Emitting Snapshot to plugins")

	select {
	case <-time.After(pluginTimeout):
		log.Panic("Timeout. Plugins failed to respond to snapshot.")
	case <-events.Emit(ApigeeSyncEventSelector, snapshot):
	}
}

func extractTablesFromSnapshot(snapshot *common.Snapshot) (tables map[string]bool) {

	tables = make(map[string]bool)

	log.Debug("Extracting table names from snapshot")
	if snapshot.Tables == nil {
		//if this panic ever fires, it's a bug
		log.Panicf("Attempt to extract known tables from snapshot without tables failed")
	}

	for _, table := range snapshot.Tables {
		tables[table.Name] = true
	}

	return tables
}

func extractTablesFromDB(db apid.DB) (tables map[string]bool) {

	tables = make(map[string]bool)

	log.Debug("Extracting table names from existing DB")
	rows, err := db.Query("SELECT name FROM _known_tables;")
	defer rows.Close()

	if err != nil {
		log.Panicf("Error reading current set of tables: %v", err)
	}

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			log.Panicf("Error reading current set of tables: %v", err)
		}
		log.Debugf("Table %s found in existing db", table)

		tables[table] = true
	}
	return tables
}

// Skip Downloading snapshot if there is already a snapshot available from previous run
func startOnLocalSnapshot(snapshot string) *common.Snapshot{
	log.Infof("Starting on local snapshot: %s", snapshot)

	// ensure DB version will be accessible on behalf of dependant plugins
	db, err := dataService.DBVersion(snapshot)
	if err != nil {
		log.Panicf("Database inaccessible: %v", err)
	}

	knownTables = extractTablesFromDB(db)

	// allow plugins (including this one) to start immediately on existing database
	// Note: this MUST have no tables as that is used as an indicator
	return &common.Snapshot{
		SnapshotInfo: snapshot,
	}
}

// will keep retrying with backoff until success
func downloadSnapshot(scopes []string, snapshot *common.Snapshot, quitPolling chan bool) error {

	log.Debug("downloadSnapshot")

	snapshotUri, err := url.Parse(config.GetString(configSnapServerBaseURI))
	if err != nil {
		log.Panicf("bad url value for config %s: %s", snapshotUri, err)
	}

	snapshotUri.Path = path.Join(snapshotUri.Path, "snapshots")

	v := url.Values{}
	for _, scope := range scopes {
		v.Add("scope", scope)
	}
	snapshotUri.RawQuery = v.Encode()
	uri := snapshotUri.String()
	log.Infof("Snapshot Download: %s", uri)

	client := &http.Client{
		CheckRedirect: Redirect,
		Timeout:       httpTimeout,
	}

	//pollWithBackoff only accepts function that accept a single quit channel
	//to accomadate functions which need more parameters, wrap them in closures
	attemptDownload := getAttemptDownloadClosure(client, snapshot, uri)

	pollWithBackoff(quitPolling, attemptDownload, handleSnapshotServerError)
	return nil

}

func getAttemptDownloadClosure(client *http.Client, snapshot *common.Snapshot, uri string) func(chan bool) error{
	return func(_ chan bool) error {
		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			// should never happen, but if it does, it's unrecoverable anyway
			log.Panicf("Snapshotserver comm error: %v", err)
		}
		addHeaders(req)

		var processSnapshotResponse func(*http.Response, *common.Snapshot) (error)

		// Set the transport protocol type based on conf file input
		if config.GetString(configSnapshotProtocol) == "json" {
			req.Header.Set("Accept", "application/json")
			processSnapshotResponse = processSnapshotServerJsonResponse
		} else if config.GetString(configSnapshotProtocol) == "sqlite" {
			req.Header.Set("Accept", "application/transicator+sqlite")
			processSnapshotResponse = processSnapshotServerFileResponse
		}
		
		// Issue the request to the snapshot server
		r, err := client.Do(req)
		if err != nil {
			log.Errorf("Snapshotserver comm error: %v", err)
			return err
		}

		defer r.Body.Close()

		if r.StatusCode != 200 {
			body, _ := ioutil.ReadAll(r.Body)
			log.Errorf("Snapshot server conn failed with resp code %d, body: %s", r.StatusCode, string(body))
			return expected200Error{}
		}

		// Decode the Snapshot server response
		err = processSnapshotResponse(r, snapshot)
		if err != nil {
			log.Errorf("Response Data not parsable: %v", err)
			return err
		}

		return nil
	}
}

func persistKnownTablesToDB(tables map[string]bool, db apid.DB) {
	log.Debugf("Inserting table names found in snapshot into db")

	tx, err := db.Begin()
	if err != nil {
		log.Panicf("Error starting transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("CREATE TABLE _known_tables (name text, PRIMARY KEY(name));")
	if err != nil {
		log.Panicf("Could not create _known_tables table: %s", err)
	}

	for name := range tables {
		log.Debugf("Inserting %s into _known_tables", name)
		_, err := tx.Exec("INSERT INTO _known_tables VALUES(?);", name)
		if err != nil {
			log.Panicf("Error encountered inserting into known tables ", err)
		}

	}

	err = tx.Commit()
	if err != nil {
		log.Panicf("Error committing transaction: %v", err)

	}
}

func processSnapshotServerJsonResponse(r *http.Response, snapshot *common.Snapshot) error {
	return json.NewDecoder(r.Body).Decode(snapshot)
}

func processSnapshotServerFileResponse(r *http.Response, snapshot *common.Snapshot) error {
	dbId := r.Header.Get("Transicator-Snapshot-TXID")
	out, err := os.Create(data.DBPath(dbId))
	if err != nil {
		return err
	}
	defer out.Close()

	//stream respose to DB
	_, err = io.Copy(out, r.Body)

	if err != nil {
		return err
	}

	snapshot.SnapshotInfo = dbId
	//TODO get timestamp from transicator.  Not currently in response

	return nil
}

func handleSnapshotServerError(err error) {
	log.Debugf("Error connecting to snapshot server: %v", err)
}