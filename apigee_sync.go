package apidApigeeSync

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	httpTimeout       = time.Minute
	pluginTimeout     = time.Minute
	maxBackoffTimeout = time.Minute
)

var (
	block        string = "45"
	token        string
	lastSequence string
	polling      bool
)

/*
 * Polls change agent for changes. In event of errors, uses a doubling
 * backoff from 200ms up to a max delay of the configPollInterval value.
 */
func pollForChanges() {

	// ensure there's just one polling thread
	if polling {
		return
	}
	polling = true

	var backOffFunc func()
	pollInterval := config.GetDuration(configPollInterval)
	for {
		start := time.Now()
		err := pollChangeAgent()
		end := time.Now()
		if err != nil {
			if _, ok := err.(apiError); ok {
				downloadDataSnapshot()
				continue
			}
			log.Debugf("Error connecting to changeserver: %v", err)
		}
		if end.After(start.Add(time.Second)) {
			backOffFunc = nil
			continue
		}
		if backOffFunc == nil {
			backOffFunc = createBackOff(200*time.Millisecond, pollInterval)
		}
		backOffFunc()
	}

	polling = false
}

/*
 * Long polls the change agent with a 45 second block. Parses the response from
 * change agent and raises an event. Called by pollForChanges().
 */
func pollChangeAgent() error {

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
				token = ""

			case http.StatusNotModified:
				continue

			case http.StatusBadRequest:
				var apiErr apiError
				err = json.NewDecoder(r.Body).Decode(&apiErr)
				if err != nil {
					log.Errorf("JSON Response Data not parsable: %v", err)
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
				close(done)
			}
		} else {
			log.Debugf("No Changes detected for Scopes: %s", scopes)
		}

		if lastSequence != resp.LastSequence {
			lastSequence = resp.LastSequence
			err := updateLastSequence(lastSequence)
			if err != nil {
				log.Panic("Unable to update Sequence in DB")
			}
		}
	}
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
	maxBackOff := maxBackoffTimeout
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
		req.Header.Set("plugin_details", apidPluginDetails)

		if newInstanceID {
			req.Header.Set("created_at_apid", time.Now().Format(time.RFC3339))
		} else {
			req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
		}

		client := &http.Client{Timeout: httpTimeout}
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("Unable to Connect to Edge Proxy Server: %v", err)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Errorf("Unable to read EdgeProxy Sever response: %v", err)
			continue
		}

		if resp.StatusCode != 200 {
			log.Errorf("Oauth Request Failed with Resp Code: %d. Body: %s", resp.StatusCode, string(body))
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

		if newInstanceID {
			newInstanceID = false
			updateApidInstanceInfo()
		}

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

// pollForChanges should usually be true, tests use the flag
func bootstrap() {

	if apidInfo.LastSnapshot != "" {
		startOnLocalSnapshot(apidInfo.LastSnapshot)
		return
	}

	downloadBootSnapshot()
	downloadDataSnapshot()
	go pollForChanges()
}

// retrieve boot information: apid_config and apid_config_scope
func downloadBootSnapshot() {
	log.Debug("download Snapshot for boot data")

	scopes := []string{apidInfo.ClusterID}
	downloadSnapshot(scopes)
	// note that for boot snapshot case, we don't need to inform plugins as they'll get the data snapshot
}

// use the scope IDs from the boot snapshot to get all the data associated with the scopes
func downloadDataSnapshot() {
	log.Debug("download Snapshot for data scopes")

	var scopes = findScopesForId(apidInfo.ClusterID)
	scopes = append(scopes, apidInfo.ClusterID)
	resp := downloadSnapshot(scopes)

	done := make(chan bool)
	log.Info("Emitting Snapshot to plugins")
	events.EmitWithCallback(ApigeeSyncEventSelector, &resp, func(event apid.Event) {
		done <- true
	})

	select {
	case <-time.After(pluginTimeout):
		log.Panic("Timeout. Plugins failed to respond to snapshot.")
	case <-done:
		close(done)
	}
}

// Skip Downloading snapshot if there is already a snapshot available from previous run
func startOnLocalSnapshot(snapshot string) {
	log.Infof("Starting on local snapshot: %s", snapshot)

	// ensure DB version will be accessible on behalf of dependant plugins
	_, err := data.DBVersion(snapshot)
	if err != nil {
		log.Panicf("Database inaccessible: %v", err)
	}

	// allow plugins (including this one) to start immediately on existing database
	// Note: this MUST have no tables as that is used as an indicator
	snap := &common.Snapshot{
		SnapshotInfo: apidInfo.LastSnapshot,
	}
	events.EmitWithCallback(ApigeeSyncEventSelector, snap, func(event apid.Event) {
		go pollForChanges()
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

	// getBearerToken loops until good
	getBearerToken()
	// todo: this could expire... ensure it's called again as needed

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

		if r.StatusCode != 200 {
			log.Errorf("Snapshot server conn failed with resp code %d", r.StatusCode)
			r.Body.Close()
			continue
		}

		// Decode the Snapshot server response
		var resp common.Snapshot
		err = json.NewDecoder(r.Body).Decode(&resp)
		if err != nil {
			log.Errorf("JSON Response Data not parsable: %v", err)
			r.Body.Close()
			continue
		}

		r.Body.Close()
		return resp
	}
}

func addHeaders(req *http.Request) {
	req.Header.Add("Authorization", "Bearer "+token)
	req.Header.Set("apid_instance_id", apidInfo.InstanceID)
	req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
	req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
}

type apiError struct {
	Code string `json:"code"`
}

func (a apiError) Error() string {
	return a.Code
}
