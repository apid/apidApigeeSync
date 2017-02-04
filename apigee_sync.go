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
var downloadDataSnapshot, downloadBootSnapshot, changeFinished bool
var lastSequence string

func addHeaders(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Set("apid_instance_id", apidInfo.InstanceID)
	req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
	req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
}

func postPluginDataDelivery(e apid.Event) {

	if ede, ok := e.(apid.EventDeliveryEvent); ok {

		if ev, ok := ede.Event.(*common.ChangeList); ok {
			if lastSequence != ev.LastSequence {
				lastSequence = ev.LastSequence
				err := updateLastSequence(lastSequence)
				if err != nil {
					log.Panic("Unable to update Sequence in DB")
				}
			}
			changeFinished = true

		} else if _, ok := ede.Event.(*common.Snapshot); ok {
			if downloadBootSnapshot == false {
				downloadBootSnapshot = true
				log.Debug("Updated bootstrap SnapshotInfo")
			} else {
				downloadDataSnapshot = true
				log.Debug("Updated data SnapshotInfo")
			}
		}
	}
}

/*
 * Helper function that sleeps for N seconds if comm with change agent
 * fails. The retry interval gradually is incremented each time it fails
 * till it reaches the Polling Int time, and after which it constantly
 * retries at the polling time interval
 */
func updatePeriodicChanges() {

	times := 1
	pollInterval := config.GetInt(configPollInterval)
	for {
		startTime := time.Second
		err := pollChangeAgent()
		if err != nil {
			log.Debugf("Error connecting to changeserver: %v", err)
		}
		endTime := time.Second
		// Gradually increase retry interval, and max at some level
		if endTime-startTime <= 1 {
			if times < pollInterval {
				times++
			} else {
				times = pollInterval
			}
			log.Debugf("Connecting to changeserver...")
			time.Sleep(time.Duration(times) * 200 * time.Millisecond)
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
		log.Debug("Waiting for snapshot download to complete")
		return errors.New("Snapshot download in progress...")
	}
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
		log.Debug("polling...")
		if token == "" {
			// invalid token, loop until we get one
			getBearerToken()
		}

		/* Find the scopes associated with the config id */
		scopes := findScopesForId(apidInfo.ClusterID)
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
		v.Add("scope", apidInfo.ClusterID)
		v.Add("snapshot", apidInfo.LastSnapshot)
		changesUri.RawQuery = v.Encode()
		uri := changesUri.String()
		log.Debugf("Fetching changes: %s", uri)

		/* If error, break the loop, and retry after interval */
		client := &http.Client{}
		req, err := http.NewRequest("GET", uri, nil)
		addHeaders(req)
		r, err := client.Do(req)
		if err != nil {
			log.Errorf("change agent comm error: %s", err)
			return err
		}

		// todo: should StatusNotChanged be a special case here?

		/* If the call is not Authorized, update flag */
		if r.StatusCode != http.StatusOK {
			if r.StatusCode == http.StatusUnauthorized {
				token = ""
				log.Errorf("Token expired? Unauthorized request.")
			}
			r.Body.Close()
			log.Errorf("Get Changes request failed with Resp err: %d", r.StatusCode)
			return err
		}

		var resp common.ChangeList
		err = json.NewDecoder(r.Body).Decode(&resp)
		r.Body.Close()
		if err != nil {
			log.Errorf("JSON Response Data not parsable: %v", err)
			return err
		}

		/* If valid data present, Emit to plugins */
		if len(resp.Changes) > 0 {
			changeFinished = false
			events.ListenFunc(apid.EventDeliveredSelector, postPluginDataDelivery)
			events.Emit(ApigeeSyncEventSelector, &resp)
			/*
			 * The plugins should have finished what they are doing.
			 * Wait till they are done.
			 * If they take longer than expected - abort apid(?)
			 * (Should there be a configurable Fudge factor?) FIXME
			 */
			for count := 0; count < 1000; count++ {
				if changeFinished == false {
					log.Debug("Waiting for plugins to complete...")
					time.Sleep(time.Duration(count) * 100 * time.Millisecond)
				} else {
					break
				}
			}
			if changeFinished == false {
				log.Panic("Never got ack from plugins. Investigate.")
			}
		} else {
			log.Debugf("No Changes detected for Scopes: %s", scopes)

			if lastSequence != resp.LastSequence {
				lastSequence = resp.LastSequence
				err := updateLastSequence(lastSequence)
				if err != nil {
					log.Panic("Unable to update Sequence in DB")
				}
			}
		}
	}
}


// simple doubling back-off
func createBackOff(retryIn, maxBackOff time.Duration) func() {
	return func() {
		log.Debugf("backoff called. will retry in %s.", retryIn)
		time.Sleep(retryIn)
		retryIn = retryIn * time.Duration(2)
		if retryIn > maxBackOff {
			retryIn = maxBackOff
		}
	}
}

/*
 * This function will (for now) use the Access Key/Secret Key/ApidConfig Id
 * to get the bearer token, and the scopes (as comma separated scope)
 */
func getBearerToken() {

	log.Info("Getting a Bearer token...")
	uriString := config.GetString(configProxyServerBaseURI)
	uri, err := url.Parse(uriString)
	if err != nil {
		log.Panicf("unable to parse uri config '%s' value: '%s': %v", configProxyServerBaseURI, uriString, err)
	}
	uri.Path = path.Join(uri.Path, "/accesstoken")

	retryIn := 5 * time.Millisecond
	maxBackOff := 1 * time.Minute
	backOffFunc := createBackOff(retryIn, maxBackOff)
	first := true

	for {
		if first {
			first = false
		} else {
			backOffFunc()
		}

		token = ""
		form := url.Values{}
		form.Set("grant_type", "client_credentials")
		form.Add("client_id", config.GetString(configConsumerKey))
		form.Add("client_secret", config.GetString(configConsumerSecret))
		req, err := http.NewRequest("POST", uri.String(), bytes.NewBufferString(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
		req.Header.Set("display_name", apidInfo.InstanceName)
		req.Header.Set("apid_instance_id", apidInfo.InstanceID)
		req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
		req.Header.Set("status", "ONLINE")
		req.Header.Set("created_at_apid", time.Now().Format(time.RFC3339))
		req.Header.Set("plugin_details", apidPluginDetails)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("Unable to Connect to Edge Proxy Server: %v", err)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Errorf("Oauth Request Failed with Resp Code: %v", resp.StatusCode)
			continue
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Errorf("Unable to read EdgeProxy Sever response: %v", err)
			continue
		}

		var oauthResp oauthTokenResp
		log.Debugf("Response: %s ", body)
		err = json.Unmarshal(body, &oauthResp)
		if err != nil {
			log.Error("unable to unmarshal JSON response %s: %v", string(body), err)
			continue
		}
		token = oauthResp.AccessToken

		/*
		 * This stores the bearer token for any other plugin to
		 * consume.
		 */
		config.Set(bearerToken, token)

		log.Debug("Got a new Bearer token.")

		return
	}
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
	req.Header.Add("org", apidInfo.ClusterID) // todo: this is strange.. is it needed?
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
func bootstrap() {

	// Skip Downloading snapshot if there is already a snapshot available from previous run of APID
	if apidInfo.LastSnapshot != "" {

		log.Infof("Starting on downloaded snapshot: %s", apidInfo.LastSnapshot)

		// ensure DB version will be accessible on behalf of dependant plugins
		_, err := data.DBVersion(apidInfo.LastSnapshot)
		if err != nil {
			log.Panicf("Database inaccessible: %v", err)
		}

		// allow plugins (including this one) to start immediately on existing database
		snap := &common.Snapshot{
			SnapshotInfo: apidInfo.LastSnapshot,
		}
		events.EmitWithCallback(ApigeeSyncEventSelector, snap, func(event apid.Event) {
			downloadBootSnapshot = true
			downloadDataSnapshot = true

			go updatePeriodicChanges()
		})

		return
	}

	/* Phase 1 */
	downloadSnapshot()

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
		downloadSnapshot()
	} else {
		log.Panic("Snapshot for bootscope failed")
	}

	go updatePeriodicChanges()
}

func downloadSnapshot() {

	log.Debugf("downloadSnapshot")

	snapshotUri, err := url.Parse(config.GetString(configSnapServerBaseURI))
	if err != nil {
		log.Panicf("bad url value for config %s: %s", snapshotUri, err)
	}

	// getBearerToken loops until good
	getBearerToken()
	// todo: this could expire... ensure it's called again as needed

	var scopes []string
	if downloadBootSnapshot {
		scopes = findScopesForId(apidInfo.ClusterID)
	}

	// always include boot cluster
	scopes = append(scopes, apidInfo.ClusterID)

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
	}

	retryIn := 5 * time.Millisecond
	maxBackOff := 1 * time.Minute
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

		// Set the transport protocol type based on conf file input
		if config.GetString(configSnapshotProtocol) == "json" {
			req.Header.Set("Accept", "application/json")
		} else {
			req.Header.Set("Accept", "application/proto")
		}

		// Issue the request to the snapshot server
		r, err := client.Do(req)
		if err != nil {
			log.Errorf("Snapshotserver comm error: %v", err)
			continue
		}

		// Decode the Snapshot server response
		var resp common.Snapshot
		err = json.NewDecoder(r.Body).Decode(&resp)
		r.Body.Close()
		if err != nil {
			if downloadBootSnapshot {
				/*
				 * If the data set is empty, allow it to proceed, as change server
				 * will feed data. Since Bootstrapping has passed, it has the
				 * Bootstrap config id to function.
				 */
				downloadDataSnapshot = true
				return
			} else {
				log.Errorf("JSON Response Data not parsable: %v", err)
				continue
			}
		}

		if r.StatusCode != 200 {
			log.Errorf("Snapshot server conn failed. HTTP Resp code %d", r.StatusCode)
			continue
		}

		log.Info("Emitting Snapshot to plugins")
		events.ListenFunc(apid.EventDeliveredSelector, postPluginDataDelivery)
		events.Emit(ApigeeSyncEventSelector, &resp)

		break
	}
}
