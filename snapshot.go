package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/30x/apid-core/data"
	"github.com/apigee-labs/transicator/common"
	"net/http"
	"os"

	"io"
	"io/ioutil"
	"net/url"
	"path"
	"sync/atomic"
	"time"
)

type simpleSnapShotManager struct {
	// to send quit signal to the downloading thread
	quitChan chan bool
	// to mark the graceful close of snapshotManager
	finishChan chan bool
	// 0 for not closed, 1 for closed
	isClosed *int32
	// make sure close() returns immediately if there's no downloading/processing snapshot
	isDownloading *int32
}

func createSnapShotManager() *simpleSnapShotManager {
	isClosedInt := int32(0)
	isDownloadingInt := int32(0)
	return &simpleSnapShotManager{
		quitChan:      make(chan bool, 1),
		finishChan:    make(chan bool, 1),
		isClosed:      &isClosedInt,
		isDownloading: &isDownloadingInt,
	}
}

/*
 * thread-safe close of snapShotManager
 * It marks status as closed immediately, and quits backoff downloading
 * use <- close() for blocking close
 * should only be called by pollChangeManager, because pollChangeManager is dependent on it
 */
func (s *simpleSnapShotManager) close() <-chan bool {
	//has been closed before
	if atomic.SwapInt32(s.isClosed, 1) == int32(1) {
		log.Error("snapShotManager: close() called on a closed snapShotManager!")
		go func() {
			s.finishChan <- false
			log.Debug("change manager closed")
		}()
		return s.finishChan
	}
	s.quitChan <- true
	// wait until no downloading
	for atomic.LoadInt32(s.isDownloading) == int32(1) {
		time.Sleep(time.Millisecond)
	}
	s.finishChan <- true
	return s.finishChan
}

// retrieve boot information: apid_config and apid_config_scope
func (s *simpleSnapShotManager) downloadBootSnapshot() {
	if atomic.SwapInt32(s.isDownloading, 1) == int32(1) {
		log.Panic("downloadBootSnapshot: only 1 thread can download snapshot at the same time!")
	}
	defer atomic.StoreInt32(s.isDownloading, int32(0))

	// has been closed
	if atomic.LoadInt32(s.isClosed) == int32(1) {
		log.Warn("snapShotManager: downloadBootSnapshot called on closed snapShotManager")
		return
	}

	log.Debug("download Snapshot for boot data")

	scopes := []string{apidInfo.ClusterID}
	snapshot := &common.Snapshot{}

	err := s.downloadSnapshot(true, scopes, snapshot)
	if err != nil {
		// this may happen during shutdown
		if _, ok := err.(quitSignalError); ok {
			log.Warn("downloadBootSnapshot failed due to shutdown: " + err.Error())
		}
		return
	}

	// has been closed
	if atomic.LoadInt32(s.isClosed) == int32(1) {
		log.Error("snapShotManager: processSnapshot called on closed snapShotManager")
		return
	}

	// note that for boot snapshot case, we don't need to inform plugins as they'll get the data snapshot
	s.storeBootSnapshot(snapshot)
}

func (s *simpleSnapShotManager) storeBootSnapshot(snapshot *common.Snapshot) {
	processSnapshot(snapshot)
}

// use the scope IDs from the boot snapshot to get all the data associated with the scopes
func (s *simpleSnapShotManager) downloadDataSnapshot() {
	if atomic.SwapInt32(s.isDownloading, 1) == int32(1) {
		log.Panic("downloadDataSnapshot: only 1 thread can download snapshot at the same time!")
	}
	defer atomic.StoreInt32(s.isDownloading, int32(0))

	// has been closed
	if atomic.LoadInt32(s.isClosed) == int32(1) {
		log.Warn("snapShotManager: downloadDataSnapshot called on closed snapShotManager")
		return
	}

	log.Debug("download Snapshot for data scopes")

	scopes := findScopesForId(apidInfo.ClusterID)
	scopes = append(scopes, apidInfo.ClusterID)
	snapshot := &common.Snapshot{}
	err := s.downloadSnapshot(false, scopes, snapshot)
	if err != nil {
		// this may happen during shutdown
		if _, ok := err.(quitSignalError); ok {
			log.Warn("downloadDataSnapshot failed due to shutdown: " + err.Error())
		}
		return
	}
	s.storeDataSnapshot(snapshot)
}

func (s *simpleSnapShotManager) storeDataSnapshot(snapshot *common.Snapshot) {
	knownTables = extractTablesFromSnapshot(snapshot)

	_, err := dataService.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Database inaccessible: %v", err)
	}

	log.Info("Emitting Snapshot to plugins")

	select {
	case <-time.After(pluginTimeout):
		log.Panic("Timeout. Plugins failed to respond to snapshot.")
	case <-events.Emit(ApigeeSyncEventSelector, snapshot):
		// the new snapshot has been processed
		// if close() happen after persistKnownTablesToDB(), will not interrupt snapshot processing to maintain consistency
	}

}

func extractTablesFromSnapshot(snapshot *common.Snapshot) (tables map[string]bool) {

	tables = make(map[string]bool)

	log.Debug("Extracting table names from snapshot")
	if snapshot.Tables == nil {
		//if this panic ever fires, it's a bug
		db, err := dataService.DBVersion(snapshot.SnapshotInfo)
		if err != nil {
			log.Panicf("Database inaccessible: %v", err)
		}
		rows, err := db.Query("SELECT DISTINCT tableName FROM _transicator_tables;")
		if err != nil {
			log.Panicf("Unable to read in known snapshot tables from sqlite file")
		}
		for rows.Next() {
			var tableName string
			rows.Scan(&tableName)
			if err != nil {
				log.Panic("Error scaning tableNames from _transicator_tables")
			}
			tables[tableName] = true
		}

	} else {

		for _, table := range snapshot.Tables {
			tables[table.Name] = true
		}
	}
	return tables

}

func extractTablesFromDB(db apid.DB) (tables map[string]bool) {

	tables = make(map[string]bool)

	log.Debug("Extracting table names from existing DB")
	rows, err := db.Query("SELECT DISTINCT tableName FROM _transicator_tables;")
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
func startOnLocalSnapshot(snapshot string) *common.Snapshot {
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

// a blocking method
// will keep retrying with backoff until success

func (s *simpleSnapShotManager) downloadSnapshot(isBoot bool, scopes []string, snapshot *common.Snapshot) error {
	// if closed
	if atomic.LoadInt32(s.isClosed) == int32(1) {
		log.Warn("Trying to download snapshot with a closed snapShotManager")
		return quitSignalError{}
	}

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

	//pollWithBackoff only accepts function that accept a single quit channel
	//to accommodate functions which need more parameters, wrap them in closures
	attemptDownload := getAttemptDownloadClosure(isBoot, snapshot, uri)
	pollWithBackoff(s.quitChan, attemptDownload, handleSnapshotServerError)
	return nil
}

func getAttemptDownloadClosure(isBoot bool, snapshot *common.Snapshot, uri string) func(chan bool) error {
	return func(_ chan bool) error {

		var tid string
		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			// should never happen, but if it does, it's unrecoverable anyway
			log.Panicf("Snapshotserver comm error: %v", err)
		}
		addHeaders(req)

		var processSnapshotResponse func(string, io.Reader, *common.Snapshot) error

		if config.GetString(configSnapshotProtocol) != "sqlite" {
			log.Panic("Only currently supported snashot protocol is sqlite")

		}
		req.Header.Set("Accept", "application/transicator+sqlite")
		processSnapshotResponse = processSnapshotServerFileResponse

		// Issue the request to the snapshot server
		r, err := httpclient.Do(req)
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

		// Bootstrap scope is a special case, that can occur only once. The tid is
		// hardcoded to "bootstrap" to ensure there can be no clash of tid between
		// bootstrap and subsequent data scopes.
		if isBoot == false {
			tid = r.Header.Get("Transicator-Snapshot-TXID")
		} else {
			tid = "bootstrap"
		}
		// Decode the Snapshot server response
		err = processSnapshotResponse(tid, r.Body, snapshot)
		if err != nil {
			log.Errorf("Snapshot server response Data not parsable: %v", err)
			return err
		}

		return nil
	}
}

func processSnapshotServerFileResponse(dbId string, body io.Reader, snapshot *common.Snapshot) error {
	dbPath := data.DBPath("common/" + dbId)
	log.Infof("Attempting to stream the sqlite snapshot to %s", dbPath)

	//this path includes the sqlite3 file name.  why does mkdir all stop at parent??
	log.Infof("Creating directory with mkdirall %s", dbPath)
	err := os.MkdirAll(dbPath[0:len(dbPath)-7], 0700)
	if err != nil {
		log.Errorf("Error creating db path %s", err)
	}
	out, err := os.Create(dbPath)
	if err != nil {
		return err
	}
	defer out.Close()

	//stream respose to DB
	_, err = io.Copy(out, body)

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
