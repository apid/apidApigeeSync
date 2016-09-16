package apidApigeeSync

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// todo: The following was largely copied from old APID - needs review

var latestMsgID int64
var token string
var tokenActive bool

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

	changesUri, err := url.Parse(config.GetString(configProxyServerBaseURI))
	if err != nil {
		log.Errorf("bad url value for config %s: %s", configProxyServerBaseURI, err)
		return err
	}
	changesUri.Path = path.Join(changesUri.Path, "/v1/edgex/changeagent/changes")

	for {
		log.Debug("polling...")
		org := config.GetString(configOrganization)
		/* token not valid try again */
		if tokenActive == false {
			status := getTokenForOrg(org)
			if status == false {
				return errors.New("Unable to get new token")
			}
		}

		/* A Blocking call for 1 Minute  */
		v := url.Values{}
		v.Add("since", strconv.FormatInt(int64(latestMsgID), 10))
		v.Add("block", "60")
		v.Add("tag", "org:"+org)
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
			}
			r.Body.Close()
			return err
		}

		var resp ChangeSet
		err = json.NewDecoder(r.Body).Decode(&resp)
		r.Body.Close()
		if err != nil {
			log.Errorf("JSON Response Data not parsable: [%s] ", err)
			return err
		}

		if len(resp.Changes) > 0 {
			events.Emit(ApigeeSyncEventSelector, resp)

			lastMsgID := resp.Changes[len(resp.Changes)-1].LastMsId
			if lastMsgID > 0 {
				log.Infof("Updated last msg id for org %s is %s", org, lastMsgID)

				err = storeLastMsgID(org, lastMsgID)
				if err != nil {
					// todo: what is appropriate recovery (if anything)?
					return err
				}

				latestMsgID = lastMsgID
			}
		} else {
			log.Error("Change message decoding error for org ", org)
		}
	}
}

/*
 * Persist the Last Change Id in the DB
 */
func storeLastMsgID(org string, lastID int64) error {

	db, err := data.DB()
	if err != nil {
		return err
	}

	result, err := db.Exec("UPDATE change_id SET snapshot_change_id=? WHERE org=?;", lastID, org)
	if err != nil {
		log.Errorf("UPDATE change_id failed (%s: %s): %s", lastID, org, err)
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err == nil && rowsAffected == 0 {
		_, err = db.Exec("INSERT INTO change_id (snapshot_change_id, org) VALUES (?, ?);", lastID, org)
	}
	if err != nil {
		log.Errorf("UPDATE change_id failed (%s: %s): %s", lastID, org, err)
		return err
	}

	log.Info("UPDATE change_id success (%s: %s)", lastID, org)
	return nil
}

func getTokenForOrg(org string) bool {

	uri, err := url.Parse(config.GetString(configProxyServerBaseURI))
	if err != nil {
		log.Error(err)
		return false
	}
	uri.Path = path.Join(uri.Path, "/v1/edgex/accesstoken")
	tokenActive = false

	form := url.Values{}
	form.Add("grantType", "client_credentials")
	form.Add("org", org)
	req, err := http.NewRequest("POST", uri.String(), strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")

	consumerKey := config.GetString(configConsumerKey)
	req.SetBasicAuth(consumerKey, config.GetString(configConsumerSecret))
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
	log.Info("Got a new token for Consumer: ", consumerKey)
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
