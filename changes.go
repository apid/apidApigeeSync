package apidApigeeSync

import (
	"time"
	"net/url"
	"path"
	"net/http"
	"io/ioutil"
	"encoding/json"

	"github.com/apigee-labs/transicator/common"

)

var lastSequence string
var block        string = "45"

/*
 * Long polls the change agent with a 45 second block. Parses the response from
 * change agent and raises an event. Called by pollWithBackoff().
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

//TODO refactor this method more, split it up
/* Make a single request to the changeserver to get a changelist */
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

	client := &http.Client{Timeout: httpTimeout} // must be greater than block value
	req, err := http.NewRequest("GET", uri, nil)
	addHeaders(req)

	r, err := client.Do(req)
	defer r.Body.Close()
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
			return nil

		case http.StatusBadRequest:
			var apiErr changeServerError
			var b []byte
			b, err = ioutil.ReadAll(r.Body)
			if err != nil {
				log.Errorf("Unable to read response body: %v", err)
				return err
			}
			err = json.Unmarshal(b, &apiErr)
			if err != nil {
				log.Errorf("JSON Response Data not parsable: %s", string(b))
				return err
			}
			if apiErr.Code == "SNAPSHOT_TOO_OLD" {
				log.Debug("Received SNAPSHOT_TOO_OLD message from change server.")
				err = apiErr
			}
		}

		return err
	}

	var resp common.ChangeList
	err = json.NewDecoder(r.Body).Decode(&resp)
	if err != nil {
		log.Errorf("JSON Response Data not parsable: %v", err)
		return err
	}

	if changesRequireDDLSync(resp) {
		return changeServerError{
			Code: "DDL changes detected; must get new snapshot",
		}
	}

	/* If valid data present, Emit to plugins */
	if len(resp.Changes) > 0 {
		select {
		case <-time.After(httpTimeout):
			log.Panic("Timeout. Plugins failed to respond to changes.")
		case <-events.Emit(ApigeeSyncEventSelector, &resp):
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
	return changesHaveNewTables(knownTables, changes.Changes)
}

func handleChangeServerError(err error) {

	if _, ok := err.(changeServerError); ok {
		log.Info("Detected DDL changes, going to fetch a new snapshot to sync...")
		downloadDataSnapshot(nil)
	} else {
		log.Debugf("Error connecting to changeserver: %v", err)
	}
}

/*
 * Determine if any tables in changes are not present in known tables
 */
func changesHaveNewTables(a map[string]bool, changes []common.Change) bool {

	//nil maps should not be passed in.  Making the distinction between nil map and empty map
	if a == nil || changes == nil{
		return true;
	}

	for _, change := range changes {
		if !a[change.Table] {
			return true
		}
	}

	return false
}
