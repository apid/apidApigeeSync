package apidApigeeSync

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"
)

var token string
var tokenActive, downloadDataSnapshot, downloadBootSnapshot, chfin bool
var lastSequence string
var gsnapshotInfo string

func donehandler(e apid.Event) {
	if rsp, ok := e.(apid.EventDeliveryEvent); ok {
		if rsp.Description == "event complete" {
			if ev, ok := rsp.Event.(*common.Snapshot); ok {
				if downloadBootSnapshot == false {
					downloadBootSnapshot = true
					log.Debug("Updated bootstrap SnapshotInfo")
				} else {
					gsnapshotInfo = ev.SnapshotInfo
					downloadDataSnapshot = true
					log.Debug("Updated data SnapshotInfo")
				}
			} else if ev, ok := rsp.Event.(*common.ChangeList); ok {
				lastSequence = ev.LastSequence
				status := persistChange(lastSequence)
				if status == false {
					log.Fatal("Unable to update Sequence in DB")
				}
				chfin = true
			}
		}
	}
}

/*
 * Helper function that sleeps for N seconds, if comm. with change agent
 * fails. The retry interval gradually is incremented each time it fails
 * till it reaches the Polling Int time, and after which it constantly
 * retries at the polling time interval
 */
func updatePeriodicChanges() {

	times := 1
	pollInterval := config.GetInt(configPollInterval)
	for {
		startTime := time.Second
		_ = pollChangeAgent() // todo: handle error
		endTime := time.Second
		// Gradually increase retry interval, and max at some level
		if endTime-startTime <= 1 {
			if times < pollInterval {
				times++
			} else {
				times = pollInterval
			}
			log.Debugf("Connecting to changeserver...")
			time.Sleep(time.Duration(times) * 100 * time.Millisecond)
		} else {
			// Reset sleep interval
			times = 1
		}

	}
}

/*
 * Long polls every 45 seconds the change agent. Parses the response from
 * change agent and raises an event.
 */
func pollChangeAgent() error {

	if downloadDataSnapshot != true {
		log.Warning("Waiting for snapshot download to complete")
		return errors.New("Snapshot download in progress...")
	}
	changesUri, err := url.Parse(config.GetString(configChangeServerBaseURI))
	if err != nil {
		log.Errorf("bad url value for config %s: %s", changesUri, err)
		return err
	}
	changesUri.Path = path.Join(changesUri.Path, "/changes")

	/*
	 * Check to see if we have lastSequence already saved in the DB,
	 * in which case, it has to be used to prevent re-reading same data
	 */
	lastSequence = findLastSeqInfo(gapidConfigId)
	for {
		log.Debug("polling...")
		if tokenActive == false {
			/* token not valid?, get a new token */
			status := getBearerToken()
			if status == false {
				return errors.New("Unable to get new token")
			}
		}

		/* Find the scopes associated with the config id */
		scopes := findScopesforId(gapidConfigId)
		v := url.Values{}

		/* Sequence added to the query if available */
		if lastSequence != "" {
			v.Add("since", lastSequence)
		}
		v.Add("block", "45")

		/*
		 * Include all the scopes associated with the config Id
		 * The Config Id is included as well, as it acts as the
		 * Bootstrap scope
		 */
		for _, scope := range scopes {
			v.Add("scope", scope)
		}
		v.Add("scope", gapidConfigId)
		v.Add("snapshot", gsnapshotInfo)
		changesUri.RawQuery = v.Encode()
		uri := changesUri.String()
		log.Info("Fetching changes: ", uri)

		/* If error, break the loop, and retry after interval */
		client := &http.Client{}
		req, err := http.NewRequest("GET", uri, nil)
		req.Header.Add("Authorization", "Bearer "+token)
		r, err := client.Do(req)
		if err != nil {
			log.Errorf("change agent comm error: %s", err)
			return err
		}

		/* If the call is not Authorized, update flag */
		if r.StatusCode != http.StatusOK {
			if r.StatusCode == http.StatusUnauthorized {
				tokenActive = false
				log.Errorf("Token expired? Unauthorized request.")
			}
			r.Body.Close()
			log.Errorf("Get Changes request failed with Resp err: %d",
				r.StatusCode)
			return err
		}

		var resp common.ChangeList
		err = json.NewDecoder(r.Body).Decode(&resp)
		r.Body.Close()
		if err != nil {
			log.Errorf("JSON Response Data not parsable: [%s] ", err)
			return err
		}

		/* If valid data present, Emit to plugins */
		if len(resp.Changes) > 0 {
			chfin = false
			events.ListenFunc(apid.EventDeliveredSelector, donehandler)
			events.Emit(ApigeeSyncEventSelector, &resp)
			/*
			 * The plugins should have finished what they are doing.
			 * Wait till they are done.
			 * If they take longer than expected - abort apid(?)
			 * (Should there be a configurable Fudge factor?) FIXME
			 */
			for count := 0; count < 1000; count++ {
				if chfin == false {
					log.Info("Waiting for plugins to complete...")
					time.Sleep(time.Duration(count) * 100 * time.Millisecond)
				} else {
					break
				}
			}
			if chfin == false {
				log.Fatal("Never got ack from plugins. Investigate..")
			}
		} else {
			log.Info("No Changes detected for Scopes ", scopes)
		}
	}
}

/*
 * This function will (for now) use the Access Key/Secret Key/ApidConfig Id
 * to get the bearer token, and the scopes (as comma separated scope)
 */
func getBearerToken() bool {

	log.Info("Getting a Bearer token.")
	uri, err := url.Parse(config.GetString(configProxyServerBaseURI))
	if err != nil {
		log.Error(err)
		return false
	}
	uri.Path = path.Join(uri.Path, "/accesstoken")
	tokenActive = false
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Add("client_id", config.GetString(configConsumerKey))
	form.Add("display_name", ginstName)
	form.Add("apid_instance_id", guuid)
	form.Add("apid_cluster_Id", gapidConfigId)
	form.Add("status", "ONLINE")
	form.Add("created_at", time.Now().Format(time.RFC3339))
	form.Add("plugin_details", gpgInfo)
	form.Add("client_secret", config.GetString(configConsumerSecret))
	req, err := http.NewRequest("POST", uri.String(), bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Unable to Connect to Edge Proxy Server ", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Error("Oauth Request Failed with Resp Code ", resp.StatusCode)
		return false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("Unable to read EdgeProxy Sever response ", err)
		return false
	}

	var oauthResp oauthTokenResp
	err = json.Unmarshal(body, &oauthResp)
	if err != nil {
		log.Error(err)
		return false
	}
	token = oauthResp.AccessToken
	tokenActive = true
	log.Info("Got a new Bearer token.")
	return true
}

type oauthTokenResp struct {
	IssuedAt       int64    `json:"issuedAt"`
	AppName        string   `json:"applicationName"`
	Scope          string   `json:"scope"`
	Status         string   `json:"status"`
	ApiProdList    []string `json:"apiProductList"`
	ExpiresIn      int64    `json:"expiresIn"`
	DeveloperEmail string   `json:"developerEmail"`
	TokenType      string   `json:"tokenType"`
	ClientId       string   `json:"clientId"`
	AccessToken    string   `json:"accessToken"`
	TokenExpIn     int64    `json:"refreshTokenExpiresIn"`
	RefreshCount   int64    `json:"refreshCount"`
}

func Redirect(req *http.Request, via []*http.Request) error {
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Add("org", gapidConfigId)
	return nil
}

/*
 * Method downloads the snapshot in a two phased manner.
 * Phase 1: Use the apidConfigId as the bootstrap scope, and
 * get the apid_config and apid_config_scope from the snapshot
 * server.
 * Phase 2: Get all the scopes fetches from phase 1, and issue
 * the second call to the snapshot server to get all the data
 * associated with the scope(s).
 * Emit the data for the necessary plugins to process.
 * If there is already previous data in sqlite, don't fetch
 * again from snapshot server.
 */
func DownloadSnapshots() {

	/*
	 * Skip Downloading snapshot, if there is already a snapshot
	 * available from previous run of APID
	 */
	gsnapshotInfo = findSnapshotInfo(gapidConfigId)
	if gsnapshotInfo != "" {
		downloadDataSnapshot = true
		downloadBootSnapshot = true

		log.Infof("Starting on downloaded snapshot: %s", gsnapshotInfo)

		// verify DB is accessible
		_, err := data.DBVersion(gsnapshotInfo)
		if err != nil {
			log.Panicf("Database inaccessible: %v", err)
		}

		// allow plugins to start immediately on existing database
		snap := &common.Snapshot{
			SnapshotInfo: gsnapshotInfo,
		}
		events.Emit(ApigeeSyncEventSelector, snap)

		return
	}

	/* Phase 1 */
	DownloadSnapshot()

	/*
	 * Give some time for all the plugins to process the Downloaded
	 * Snapshot
	 */
	for count := 0; count < 60; count++ {
		if !downloadBootSnapshot {
			log.Debug("Waiting for bootscope snapshot download...")
			time.Sleep(time.Duration(count) * 100 * time.Millisecond)
		} else {
			break
		}
	}

	/* Phase 2 */
	if downloadBootSnapshot && downloadDataSnapshot {
		log.Debug("Proceeding with existing Sqlite data")
	} else if downloadBootSnapshot == true {
		log.Debug("Proceed to download Snapshot for data scopes")
		DownloadSnapshot()
	} else {
		log.Fatal("Snapshot for bootscope failed")
	}
}

func DownloadSnapshot() {

	var scopes []string

	/* Get the bearer token */
	status := getBearerToken()
	if status == false {
		log.Fatal("Unable to get Bearer token or is Invalid")
	}
	snapshotUri, err := url.Parse(config.GetString(configSnapServerBaseURI))
	if err != nil {
		log.Fatalf("bad url value for config %s: %s", snapshotUri, err)
	}

	if downloadBootSnapshot == false {
		scopes = append(scopes, (gapidConfigId))
	} else {
		scopes = findScopesforId(gapidConfigId)
	}
	if scopes == nil {
		log.Fatal("Scope cannot be found to download snapshot")
	}
	/* Frame and send the snapshot request */
	snapshotUri.Path = path.Join(snapshotUri.Path, "/snapshots")

	v := url.Values{}
	for _, scope := range scopes {
		v.Add("scope", scope)
	}
	snapshotUri.RawQuery = v.Encode()
	uri := snapshotUri.String()
	log.Info("Snapshot Download : ", uri)

	client := &http.Client{
		CheckRedirect: Redirect,
	}
	req, err := http.NewRequest("GET", uri, nil)
	req.Header.Add("Authorization", "Bearer "+token)

	/* Set the transport protocol type based on conf file input */
	if config.GetString(configSnapshotProtocol) == "json" {
		req.Header.Set("Accept", "application/json")
	} else {
		req.Header.Set("Accept", "application/proto")
	}

	/* Issue the request to the snapshot server */
	r, err := client.Do(req)
	if err != nil {
		log.Fatalf("Snapshotserver comm error: [%s] ", err)
	}
	defer r.Body.Close()

	/* Decode the Snapshot server response */
	var resp common.Snapshot
	err = json.NewDecoder(r.Body).Decode(&resp)
	if err != nil {

		if downloadBootSnapshot == false {
			log.Fatal("JSON Response Data not parsable: ", err)
		} else {

			/*
			 * If the data set is empty, allow it to proceed, as changeserver
			 * will feed data. Since Bootstrapping has passed, it has the
			 * Bootstrap config id to function.
			 */
			downloadDataSnapshot = true
			return
		}
	}

	if r.StatusCode == 200 {
		log.Info("Emit Snapshot response to plugins")
		events.ListenFunc(apid.EventDeliveredSelector, donehandler)
		events.Emit(ApigeeSyncEventSelector, &resp)

	} else {
		log.Fatalf("Snapshot server conn failed. HTTP Resp code %d", r.StatusCode)
	}

}

/*
 * For the given apidConfigId, this function will retrieve all the scopes
 * associated with it
 */
func findScopesforId(configId string) (scopes []string) {

	var scope string
	db, err := data.DB()
	if err != nil {
		log.Errorf("DB open Error: %s", err)
		return nil
	}

	rows, err := db.Query("select scope from APID_CONFIG_SCOPE where apid_config_id = $1", configId)
	if err != nil {
		log.Errorf("Failed to query APID_CONFIG_SCOPE. Err: %s", err)
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&scope)
		scopes = append(scopes, scope)
	}
	return scopes
}

/*
 * Retrieve LastSequence for the given apidConfigId from apid_config table
 */
func findLastSeqInfo(configId string) (info string) {

	db, err := data.DB()
	if err != nil {
		log.Errorf("DB open Error: %s", err)
		return ""
	}

	rows, err := db.Query("select lastSequence from APID_CONFIG where id = $1", configId)
	if err != nil {
		log.Errorf("Failed to query APID_CONFIG. Err: %s", err)
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&info)
	}
	return info
}

/*
 * Retrieve SnapshotInfo for the given apidConfigId from apid_config table
 */
func findSnapshotInfo(configId string) (info string) {

	db, err := data.DB()
	if err != nil {
		log.Errorf("DB open Error: %s", err)
		return ""
	}

	rows, err := db.Query("select snapshotInfo from APID_CONFIG where id = $1", configId)
	if err != nil {
		log.Errorf("Failed to query APID_CONFIG. Err: %s", err)
		return ""
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&info)
	}
	return info
}

/*
 * Persist the last change Id each time a change has been successfully
 * processed by the plugin(s)
 */
func persistChange(lastChange string) bool {
	db, err := data.DB()
	if err != nil {
		log.Errorf("DB open Error: %s", err)
		return false
	}
	txn, err := db.Begin()
	if err != nil {
		log.Error("Unable to create Sqlite transaction")
		return false
	}
	prep, err := txn.Prepare("UPDATE APID_CONFIG SET lastSequence=$1 WHERE id=$2;")
	if err != nil {
		log.Error("INSERT APID_CONFIG Failed: ", err)
		return false
	}
	defer prep.Close()
	s := txn.Stmt(prep)
	_, err = s.Exec(lastChange, gapidConfigId)
	s.Close()
	if err != nil {
		log.Error("UPDATE APID_CONFIG_SCOPE Failed: ", err)
		txn.Rollback()
		return false
	} else {
		log.Info("UPDATE  APID_CONFIG_SCOPE Success: (", lastChange, ")")
		txn.Commit()
		return true
	}

}
