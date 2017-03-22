package apidApigeeSync

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"time"

	"io/ioutil"

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	"io"
	"github.com/30x/apid-core/data"
	"os"
)

const (
	httpTimeout       = time.Minute
	pluginTimeout     = time.Minute
	maxBackoffTimeout = time.Minute
)

var (
	block        string = "45"
	lastSequence string
	knownTables = make(map[string]bool)
)

/*
 * Polls change agent for changes. In event of errors, uses a doubling
 * backoff from 200ms up to a max delay of the configPollInterval value.
 */
func pollWithBackoff(quit chan bool, toExecute func(chan bool) error, handleError func(error)) {

	backoff := NewExponentialBackoff(200*time.Millisecond, config.GetDuration(configPollInterval), 2)
	retry := time.After(0 * time.Millisecond)

	for {
		select {
		case <-quit:
			return
		case <-retry:
			start := time.Now()
			err := toExecute(quit)
			if _, ok := err.(quitSignalError); ok {
				return
			}

			end := time.Now()
			handleError(err)

			if end.After(start.Add(time.Second)) {
				backoff.Reset()
				retry = time.After(0 * time.Millisecond)
			} else {
				retry = time.After(backoff.Duration())
			}
		}
	}
}

/*
 * Long polls the change agent with a 45 second block. Parses the response from
 * change agent and raises an event. Called by pollWithRetry().
 */
func pollChangeAgent(quit chan bool) error {

	changesUri, err := url.Parse(config.GetString(configChangeServerBaseURI))
	if err != nil {
		log.Errorf("bad url value for config %s: %s", changesUri, err)
		return err
	}
	changesUri.Path = path.Join(changesUri.Path, "changes")

	/*
	 * Check to see if we have lastSequence already saved in the DB,
	 * in which case, it has to be used to prevent re-reading same data
	 */
	lastSequence = getLastSequence()

	for {
		select {
		case <-quit:
			return quitSignalError{}
		default:
			err := getChanges(changesUri)
			if err != nil {
				return err
			}
		}
	}
}

func getChanges(changesUri *url.URL) error {
	log.Debug("polling...")

	/* Find the scopes associated with the config id */
	scopes := findScopesForId(apidInfo.ClusterID)
	v := url.Values{}

	/* Sequence added to the query if available */
	if lastSequence != "" {
		v.Add("since", lastSequence)
	}
	v.Add("block", block)

	/*
 	* Include all the scopes associated with the config Id
 	* The Config Id is included as well, as it acts as the
 	* Bootstrap scope
 	*/
	for _, scope := range scopes {
		v.Add("scope", scope)
	}
	v.Add("scope", apidInfo.ClusterID)
	v.Add("snapshot", apidInfo.LastSnapshot)
	changesUri.RawQuery = v.Encode()
	uri := changesUri.String()
	log.Debugf("Fetching changes: %s", uri)

	/* If error, break the loop, and retry after interval */
	client := &http.Client{Timeout: httpTimeout} // must be greater than block value
	req, err := http.NewRequest("GET", uri, nil)
	addHeaders(req)

	r, err := client.Do(req)
	if err != nil {
		log.Errorf("change agent comm error: %s", err)
		return err
	}

	if r.StatusCode != http.StatusOK {
		log.Errorf("Get changes request failed with status code: %d", r.StatusCode)
		switch r.StatusCode {
		case http.StatusUnauthorized:
			tokenManager.invalidateToken()

		case http.StatusNotModified:
			r.Body.Close()
			return nil

		case http.StatusBadRequest:
			var apiErr changeServerError
			var b []byte
			b, err = ioutil.ReadAll(r.Body)
			if err != nil {
				log.Errorf("Unable to read response body: %v", err)
				break
			}
			err = json.Unmarshal(b, &apiErr)
			if err != nil {
				log.Errorf("JSON Response Data not parsable: %s", string(b))
				break
			}
			if apiErr.Code == "SNAPSHOT_TOO_OLD" {
				log.Debug("Received SNAPSHOT_TOO_OLD message from change server.")
				err = apiErr
			}
		}

		r.Body.Close()
		return err
	}

	var resp common.ChangeList
	err = json.NewDecoder(r.Body).Decode(&resp)
	r.Body.Close()
	if err != nil {
		log.Errorf("JSON Response Data not parsable: %v", err)
		return err
	}

	if changesRequireDDLSync(resp) {
		log.Info("Detected DDL changes, going to fetch a new snapshot to sync...")
		return changeServerError{
			Code: "DDL changes detected; must get new snapshot",
		}
	}

	/* If valid data present, Emit to plugins */
	if len(resp.Changes) > 0 {
		done := make(chan bool)
		events.EmitWithCallback(ApigeeSyncEventSelector, &resp, func(event apid.Event) {
			done <- true
		})

		select {
		case <-time.After(httpTimeout):
			log.Panic("Timeout. Plugins failed to respond to changes.")
		case <-done:
		}
	} else {
		log.Debugf("No Changes detected for Scopes: %s", scopes)
	}

	if lastSequence != resp.LastSequence {
		lastSequence = resp.LastSequence
		err := updateLastSequence(resp.LastSequence)
		if err != nil {
			log.Panic("Unable to update Sequence in DB")
		}
	}
	return nil
}


func changesRequireDDLSync(changes common.ChangeList) bool {

	return !mapIsSubset(knownTables, extractTablesFromChangelist(changes))
}

// simple doubling back-off
func createBackOff(retryIn, maxBackOff time.Duration) func() {
	return func() {
		if retryIn > maxBackOff {
			retryIn = maxBackOff
		}
		log.Debugf("backoff called. will retry in %s.", retryIn)
		time.Sleep(retryIn)
		retryIn = retryIn * time.Duration(2)
	}
}

func Redirect(req *http.Request, via []*http.Request) error {
	req.Header.Add("Authorization", "Bearer "+tokenManager.getBearerToken())
	req.Header.Add("org", apidInfo.ClusterID) // todo: this is strange.. is it needed?
	return nil
}

// pollWithBackoff should usually be true, tests use the flag
func bootstrap(quitPolling chan bool) {

	if apidInfo.LastSnapshot != "" {
		startOnLocalSnapshot(apidInfo.LastSnapshot, quitPolling)
		return
	}

	downloadBootSnapshot()
	downloadDataSnapshot()

	go pollWithBackoff(quitPolling, pollChangeAgent, handleChangeServerError)

}

// retrieve boot information: apid_config and apid_config_scope
func downloadBootSnapshot() {
	log.Debug("download Snapshot for boot data")

	scopes := []string{apidInfo.ClusterID}
	snapshot := downloadSnapshot(scopes)
	// note that for boot snapshot case, we don't need to inform plugins as they'll get the data snapshot
	processSnapshot(&snapshot)
}

// use the scope IDs from the boot snapshot to get all the data associated with the scopes
func downloadDataSnapshot() {
	log.Debug("download Snapshot for data scopes")

	var scopes = findScopesForId(apidInfo.ClusterID)
	scopes = append(scopes, apidInfo.ClusterID)
	resp := downloadSnapshot(scopes)

	knownTables = extractTablesFromSnapshot(resp)

	db, err := dataService.DBVersion(resp.SnapshotInfo)
	if err != nil {
		log.Panicf("Database inaccessible: %v", err)
	}
	persistKnownTablesToDB(knownTables, db)

	done := make(chan bool)
	log.Info("Emitting Snapshot to plugins")
	events.EmitWithCallback(ApigeeSyncEventSelector, &resp, func(event apid.Event) {
		done <- true
	})

	select {
	case <-time.After(pluginTimeout):
		log.Panic("Timeout. Plugins failed to respond to snapshot.")
	case <-done:
	}
}

func extractTablesFromSnapshot(snapshot common.Snapshot) (tables map[string]bool) {

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

func extractTablesFromChangelist(changes common.ChangeList) (tables map[string] bool) {

	tables = make(map[string]bool)

	for _, change := range changes.Changes {
		tables[change.Table] = true
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

// Skip Downloading snapshot if there is already a snapshot available from previous run
func startOnLocalSnapshot(snapshot string, quitPolling chan bool) {
	log.Infof("Starting on local snapshot: %s", snapshot)

	// ensure DB version will be accessible on behalf of dependant plugins
	db, err := dataService.DBVersion(snapshot)
	if err != nil {
		log.Panicf("Database inaccessible: %v", err)
	}

	knownTables = extractTablesFromDB(db)

	// allow plugins (including this one) to start immediately on existing database
	// Note: this MUST have no tables as that is used as an indicator
	snap := &common.Snapshot{
		SnapshotInfo: snapshot,
	}
	events.EmitWithCallback(ApigeeSyncEventSelector, snap, func(event apid.Event) {
		go pollWithBackoff(quitPolling, pollChangeAgent, handleChangeServerError)
	})

	log.Infof("Started on local snapshot: %s", snapshot)
}

// will keep retrying with backoff until success
func downloadSnapshot(scopes []string) common.Snapshot {

	log.Debug("downloadSnapshot")

	snapshotUri, err := url.Parse(config.GetString(configSnapServerBaseURI))
	if err != nil {
		log.Panicf("bad url value for config %s: %s", snapshotUri, err)
	}

	/* Frame and send the snapshot request */
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

	retryIn := 5 * time.Millisecond
	maxBackOff := maxBackoffTimeout
	backOffFunc := createBackOff(retryIn, maxBackOff)
	first := true

	for {
		if first {
			first = false
		} else {
			backOffFunc()
		}

		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			// should never happen, but if it does, it's unrecoverable anyway
			log.Panicf("Snapshotserver comm error: %v", err)
		}
		addHeaders(req)

		var processSnapshotResponse func(*http.Response, *common.Snapshot)(error)

		// Set the transport protocol type based on conf file input
		if config.GetString(configSnapshotProtocol) == "json" {
			req.Header.Set("Accept", "application/json")
			processSnapshotResponse = processSnapshotServerJsonResponse
		} else if config.GetString(configSnapshotProtocol) == "sqlite"{
			req.Header.Set("Accept", "application/transicator+sqlite")
			processSnapshotResponse = processSnapshotServerFileResponse
		}

		// Issue the request to the snapshot server
		r, err := client.Do(req)
		if err != nil {
			log.Errorf("Snapshotserver comm error: %v", err)
			continue
		}

		if r.StatusCode != 200 {
			log.Errorf("Snapshot server conn failed with snapshot code %d", r.StatusCode)
			r.Body.Close()
			continue
		}

		// Decode the Snapshot server response
		var snapshot common.Snapshot
		err = processSnapshotResponse(r, &snapshot)
		if err != nil {
			log.Errorf("Response Data not parsable: %v", err)
			r.Body.Close()
			continue
		}

		r.Body.Close()
		return snapshot
	}
}

func processSnapshotServerJsonResponse(r *http.Response, snapshot *common.Snapshot) error {
	return json.NewDecoder(r.Body).Decode(snapshot)
}

func processSnapshotServerFileResponse(r *http.Response, snapshot *common.Snapshot) error {
	dbId := r.Header.Get("transicator-snapshoot-txid")
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

func addHeaders(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+tokenManager.getBearerToken())
	req.Header.Set("apid_instance_id", apidInfo.InstanceID)
	req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
	req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
}

func handleChangeServerError(err error) {
	if err != nil {
		if _, ok := err.(changeServerError); ok {
			downloadDataSnapshot()
		}
		log.Debugf("Error connecting to changeserver: %v", err)
	}
}

type changeServerError struct {
	Code string `json:"code"`
}

type quitSignalError struct {
}

func (a quitSignalError) Error() string {
	return "Signal to quit encountered"
}

func (a changeServerError) Error() string {
	return a.Code
}

/*
 * Determine if map b is a subset of map a
 */
func mapIsSubset(a map[string]bool, b map[string]bool) bool {

	//nil maps should not be passed in.  Making the distinction between nil map and empty map
	if a == nil || b == nil {
		return false;
	}

	for k := range b {
		if !a[k] {
			return false
		}
	}

	return true
}