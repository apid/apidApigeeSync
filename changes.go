package apidApigeeSync

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/apigee-labs/transicator/common"
	"sync/atomic"
)

var lastSequence string
var block string = "45"

type pollChangeManager struct {
	// 0 for not closed, 1 for closed
	isClosed *int32
	// 0 for pollChangeWithBackoff() not launched, 1 for launched
	isLaunched *int32
	quitChan   chan bool
}

func createChangeManager() *pollChangeManager {
	isClosedInt := int32(0)
	isLaunchedInt := int32(0)
	return &pollChangeManager{
		isClosed:   &isClosedInt,
		quitChan:   make(chan bool),
		isLaunched: &isLaunchedInt,
	}
}

/*
 * thread-safe close of pollChangeManager
 * It marks status as closed immediately, quits backoff polling agent, and closes tokenManager
 * use <- close() for blocking close
 */
func (c *pollChangeManager) close() <-chan bool {
	finishChan := make(chan bool, 1)
	//has been closed
	if atomic.SwapInt32(c.isClosed, 1) == int32(1) {
		log.Error("pollChangeManager: close() called on a closed pollChangeManager!")
		go func() {
			finishChan <- false
			log.Debug("change manager closed")
		}()
		return finishChan
	}
	// not launched
	if atomic.LoadInt32(c.isLaunched) == int32(0) {
		log.Error("pollChangeManager: close() called when pollChangeWithBackoff unlaunched! close tokenManager!")
		go func() {
			tokenManager.close()
			<-snapManager.close()
			finishChan <- false
			log.Debug("change manager closed")
		}()
		return finishChan
	}
	// launched
	log.Debug("pollChangeManager: close pollChangeWithBackoff and token manager")
	go func() {
		c.quitChan <- true
		tokenManager.close()
		<-snapManager.close()
		finishChan <- true
		log.Debug("change manager closed")
	}()
	return finishChan
}

/*
 * thread-safe pollChangeWithBackoff(), guaranteed: only one polling thread
 */

func (c *pollChangeManager) pollChangeWithBackoff() {
	// closed
	if atomic.LoadInt32(c.isClosed) == int32(1) {
		log.Error("pollChangeManager: pollChangeWithBackoff() called after closed")
		return
	}
	// has been launched before
	if atomic.SwapInt32(c.isLaunched, 1) == int32(1) {
		log.Error("pollChangeManager: pollChangeWithBackoff() has been launched before")
		return
	}

	go pollWithBackoff(c.quitChan, c.pollChangeAgent, c.handleChangeServerError)
	log.Debug("pollChangeManager: pollChangeWithBackoff() started pollWithBackoff")

}

/*
 * Long polls the change agent with a 45 second block. Parses the response from
 * change agent and raises an event. Called by pollWithBackoff().
 */
func (c *pollChangeManager) pollChangeAgent(dummyQuit chan bool) error {

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
		case <-c.quitChan:
			log.Info("pollChangeAgent; Recevied quit signal to stop polling change server, close token manager")
			return quitSignalError{}
		default:
			err := c.getChanges(changesUri)
			if err != nil {
				if _, ok := err.(quitSignalError); ok {
					log.Debug("pollChangeAgent: consuming the quit signal")
					<-c.quitChan
				}
				return err
			}
		}
	}
}

//TODO refactor this method more, split it up
/* Make a single request to the changeserver to get a changelist */
func (c *pollChangeManager) getChanges(changesUri *url.URL) error {
	// if closed
	if atomic.LoadInt32(c.isClosed) == int32(1) {
		return quitSignalError{}
	}
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
	if err != nil {
		log.Errorf("change agent comm error: %s", err)
		// if closed
		if atomic.LoadInt32(c.isClosed) == int32(1) {
			return quitSignalError{}
		}
		return err
	}
	defer r.Body.Close()

	// has been closed
	if atomic.LoadInt32(c.isClosed) == int32(1) {
		log.Debugf("getChanges: changeManager has been closed")
		return quitSignalError{}
	}

	if r.StatusCode != http.StatusOK {
		log.Errorf("Get changes request failed with status code: %d", r.StatusCode)
		switch r.StatusCode {
		case http.StatusUnauthorized:
			tokenManager.invalidateToken()
			return nil

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
			return nil
		}
		return nil
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

func (c *pollChangeManager) handleChangeServerError(err error) {
	// has been closed
	if atomic.LoadInt32(c.isClosed) == int32(1) {
		log.Debugf("handleChangeServerError: changeManager has been closed")
		return
	}
	if _, ok := err.(changeServerError); ok {
		log.Info("Detected DDL changes, going to fetch a new snapshot to sync...")
		snapManager.downloadDataSnapshot()
	} else {
		log.Debugf("Error connecting to changeserver: %v", err)
	}
}

/*
 * Determine if any tables in changes are not present in known tables
 */
func changesHaveNewTables(a map[string]bool, changes []common.Change) bool {

	//nil maps should not be passed in.  Making the distinction between nil map and empty map
	if a == nil || changes == nil {
		return true
	}

	for _, change := range changes {
		if !a[change.Table] {
			log.Infof("Unable to find %s table in current known tables", change.Table)
			return true
		}
	}

	return false
}
