package apidApigeeSync

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/30x/transicator/common"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"
)

// todo: The following was largely copied from old APID - needs review

var latestSequence int64
var token string
var tokenActive, downloadSnapshot, downloadBootSnapshot, gotSequence bool
var lastSequence string
var snapshotInfo string

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
			log.Debugf("sleep for %d secs", time.Duration(times))
			time.Sleep(time.Duration(times) * time.Second)
		} else {
			// Reset sleep interval
			times = 1
		}

	}
}

/*
 * Long polls every 2 minutes the change agent. Parses the response from
 * change agent and raises an event.
 */
func pollChangeAgent() error {

	if downloadSnapshot != true {
		log.Error("Waiting for snapshot download to complete")
		return errors.New("Snapshot download in progress...")
	}
	changesUri, err := url.Parse(config.GetString(configChangeServerBaseURI))
	if err != nil {
		log.Errorf("bad url value for config %s: %s", changesUri, err)
		return err
	}
	changesUri.Path = path.Join(changesUri.Path, "/changes")

	/*
	 * FIXME: This is a hack, while the correct procedure it to use the
	 * bootstrap scope
	 */
	configId := config.GetString(configScopeId)

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
		scopes := findScopesforId(configId)

		/* A Blocking call for 1 Minute  */
		v := url.Values{}
		if gotSequence == true {
			v.Add("since", lastSequence)
		}
		v.Add("block", "60")
		for _, scope := range scopes {
			v.Add("scope", scope)
		}
		v.Add("snapshot", snapshotInfo)
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

		if len(resp.Changes) > 0 {
			events.Emit(ApigeeSyncEventSelector, &resp)
		} else {
			log.Error("No Changes detected for Scopes ", scopes)
		}
		lastSequence = resp.LastSequence
		gotSequence = true
	}
}

/*
 * This function will (for now) use the Access Key/Secret Key/ApidConfig Id
 * to get the bearer token, and the scopes (as comma separated scope)
 */
func getBearerToken() bool {

	uri, err := url.Parse(config.GetString(configProxyServerBaseURI))
	if err != nil {
		log.Error(err)
		return false
	}
	uri.Path = path.Join(uri.Path, "/accesstoken")
	tokenActive = false

	form := url.Values{}
	form.Set("grantType", "client_credentials")
	form.Add("client_id", config.GetString(configConsumerKey))
	form.Add("client_secret", config.GetString(configConsumerSecret))
	req, err := http.NewRequest("POST", uri.String(), bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Error("Oauth Request Failed with Resp Code ", resp.StatusCode)
		return false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
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
	log.Info("Got a new token..")
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
	req.Header.Add("org", config.GetString(configScopeId))
	return nil
}

func DownloadSnapshot() error {

RETRY:
	var scopes []string

	/* Get the bearer token */
	status := getBearerToken()
	if status == false {
		return errors.New("Unable to get new token")
	}
	snapshotUri, err := url.Parse(config.GetString(configSnapServerBaseURI))
	if err != nil {
		log.Fatalf("bad url value for config %s: %s", snapshotUri, err)
	}

	if downloadBootSnapshot == false {
		scopes = append(scopes, (config.GetString(configScopeId)))
	} else {
		scopes = findScopesforId(config.GetString(configScopeId))
	}

	/* Frame and send the snapshot request */
	snapshotUri.Path = path.Join(snapshotUri.Path, "/snapshots")

	v := url.Values{}
	for _, scope := range scopes {
		v.Add("scopes", scope)
	}
	snapshotUri.RawQuery = v.Encode()
	uri := snapshotUri.String()
	log.Info("Snapshot Download : ", uri)

	client := &http.Client{
		CheckRedirect: Redirect,
	}
	req, err := http.NewRequest("GET", uri, nil)
	req.Header.Add("Authorization", "Bearer "+token)
	r, err := client.Do(req)
	if err != nil {
		log.Fatalf("Snapshotserver comm error: [%s] ", err)
	}
	defer r.Body.Close()

	/* Decode the Snapshot server response */
	var resp common.Snapshot
	err = json.NewDecoder(r.Body).Decode(&resp)
	if err != nil {
		log.Fatalf("JSON Response Data not parsable: [%s] ", err)
		return err
	}

	/*
	 * The idea here is that you download snapshot for the scopes
	 * associated with the apidconfig Id, and then download the
	 * data based on the scopes retrieved in the first round
	 */
	if r.StatusCode == 200 {
		log.Info("Emit Snapshot response to plugins")
		events.Emit(ApigeeSyncEventSelector, &resp)
		snapshotInfo = resp.SnapshotInfo
		if downloadBootSnapshot == false {
			downloadBootSnapshot = true
			goto RETRY
		} else if downloadBootSnapshot == true {
			downloadSnapshot = true
		}
	} else {
		log.Fatalf("Snapshot server Connect failed. Resp code %d", r.StatusCode)
	}

	return err
}

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
