// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apidApigeeSync

import (
	"encoding/json"
	"github.com/apigee-labs/transicator/common"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

type pollChangeManager struct {
	// 0 for not closed, 1 for closed
	isClosed *int32
	// 0 for pollChangeWithBackoff() not launched, 1 for launched
	isLaunched   *int32
	quitChan     chan bool
	block        int
	lastSequence string
	dbMan        DbManager
	snapMan      snapShotManager
	tokenMan     tokenManager
	client       *http.Client
}

func createChangeManager(dbMan DbManager, snapMan snapShotManager, tokenMan tokenManager, client *http.Client) *pollChangeManager {
	isClosedInt := int32(0)
	isLaunchedInt := int32(0)
	return &pollChangeManager{
		isClosed:   &isClosedInt,
		quitChan:   make(chan bool),
		isLaunched: &isLaunchedInt,
		block:      45,
		dbMan:      dbMan,
		snapMan:    snapMan,
		tokenMan:   tokenMan,
		client:     client,
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
			log.Debug("change manager closed")
			finishChan <- false
		}()
		return finishChan
	}
	// not launched
	if atomic.LoadInt32(c.isLaunched) == int32(0) {
		log.Warn("pollChangeManager: close() called when pollChangeWithBackoff unlaunched! Will wait until pollChangeWithBackoff is launched and then kill it and tokenManager!")
		go func() {
			c.quitChan <- true
			c.tokenMan.close()
			<-c.snapMan.close()
			log.Debug("change manager closed")
			finishChan <- false
		}()
		return finishChan
	}
	// launched
	log.Debug("pollChangeManager: close pollChangeWithBackoff and token manager")
	go func() {
		c.quitChan <- true
		c.tokenMan.close()
		<-c.snapMan.close()
		log.Debug("change manager closed")
		finishChan <- true
	}()
	return finishChan
}

/*
 * thread-safe pollChangeWithBackoff(), guaranteed: only one polling thread
 */

func (c *pollChangeManager) pollChangeWithBackoff() {
	// has been launched before
	if atomic.SwapInt32(c.isLaunched, 1) == int32(1) {
		log.Error("pollChangeManager: pollChangeWithBackoff() has been launched before")
		return
	}

	log.Debug("pollChangeManager: pollChangeWithBackoff() started pollWithBackoff")
	go pollWithBackoff(c.quitChan, c.pollChangeAgent, c.handleChangeServerError)

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
	c.lastSequence = c.dbMan.getLastSequence()
	for {
		select {
		case <-c.quitChan:
			log.Info("pollChangeAgent; Recevied quit signal to stop polling change server, close token manager")
			return quitSignalError{}
		default:
			scopes, err := c.dbMan.findScopesForId(apidInfo.ClusterID)
			if err != nil {
				return err
			}
			r, err := c.getChanges(scopes, changesUri)
			if err != nil {
				return err
			}
			cl, err := c.parseChangeResp(r)
			if err != nil {
				return err
			}
			if err = c.emitChangeList(scopes, cl); err != nil {
				return err
			}
		}
	}
}

func (c *pollChangeManager) parseChangeResp(r *http.Response) (*common.ChangeList, error) {
	var err error
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		log.Errorf("Get changes request failed with status code: %d", r.StatusCode)
		switch r.StatusCode {
		case http.StatusUnauthorized:
			err = c.tokenMan.invalidateToken()
			if err != nil {
				return nil, err
			}
			return nil, authFailError{}

		case http.StatusNotModified:
			return nil, nil
		case http.StatusBadRequest:
			var apiErr changeServerError
			var b []byte
			b, err = ioutil.ReadAll(r.Body)
			if err != nil {
				log.Errorf("Unable to read response body: %v", err)
				return nil, err
			}
			err = json.Unmarshal(b, &apiErr)
			if err != nil {
				log.Errorf("JSON Response Data not parsable: %s", string(b))
				return nil, err
			}
			if apiErr.Code == "SNAPSHOT_TOO_OLD" {
				log.Debug("Received SNAPSHOT_TOO_OLD message from change server.")
				err = apiErr
			}
			return nil, err
		default:
			log.Errorf("Unknown response code from change server: %v", r.Status)
			return nil, nil
		}
	}

	resp := &common.ChangeList{}
	err = json.NewDecoder(r.Body).Decode(resp)
	if err != nil {
		log.Errorf("JSON Response Data not parsable: %v", err)
		return nil, err
	}
	return resp, nil
}

func (c *pollChangeManager) emitChangeList(scopes []string, cl *common.ChangeList) error {
	var err error
	/*
	 * If the lastSequence is already newer or the same than what we got via
	 * cl.LastSequence, Ignore it.
	 */
	if c.lastSequence != "" &&
		getChangeStatus(c.lastSequence, cl.LastSequence) != 1 {
		return nil
	}

	if changesRequireDDLSync(c.dbMan.getKnowTables(), cl) {
		return changeServerError{
			Code: "DDL changes detected; must get new snapshot",
		}
	}

	/* If valid data present, Emit to plugins */
	if len(cl.Changes) > 0 {
		if err = c.dbMan.processChangeList(cl); err != nil {
			log.Errorf("Error in processChangeList: %v", err)
			return err
		}
		select {
		case <-time.After(httpTimeout):
			log.Panic("Timeout. Plugins failed to respond to changes.")
		case <-eventService.Emit(ApigeeSyncEventSelector, cl):
		}
	} else if c.lastSequence == "" {
		select {
		case <-time.After(httpTimeout):
			log.Panic("Timeout. Plugins failed to respond to changes.")
		case <-eventService.Emit(ApigeeSyncEventSelector, cl):
		}
	} else {
		log.Debugf("No Changes detected")
	}

	err = c.dbMan.updateLastSequence(cl.LastSequence)
	if err != nil {
		log.Panicf("Unable to update Sequence in DB. Err {%v}", err)
	}
	c.lastSequence = cl.LastSequence

	/*
	 * Check to see if there was any change in scope. If found, handle it
	 * by getting a new snapshot
	 */
	newScopes, err := c.dbMan.findScopesForId(apidInfo.ClusterID)
	if err != nil {
		return err
	}
	cs := scopeChanged(newScopes, scopes)
	if cs != nil {
		return cs
	}

	return nil
}

/* Make a single request to the changeserver to get a changelist */
func (c *pollChangeManager) getChanges(scopes []string, changesUri *url.URL) (*http.Response, error) {
	log.Debug("polling...")

	/* Find the scopes associated with the config id */
	v := url.Values{}

	blockValue := strconv.Itoa(c.block)
	/* Sequence added to the query if available */
	if c.lastSequence != "" {
		v.Add("since", c.lastSequence)
	} else {
		blockValue = "0"
	}
	v.Add("block", blockValue)

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
	req, err := http.NewRequest("GET", uri, nil)
	addHeaders(req, c.tokenMan.getBearerToken())
	r, err := c.client.Do(req)
	if err != nil {
		log.Errorf("change agent comm error: %s", err)
		return nil, err
	}
	return r, nil
}

func changesRequireDDLSync(knownTables map[string]bool, changes *common.ChangeList) bool {
	return changesHaveNewTables(knownTables, changes.Changes)
}

func (c *pollChangeManager) handleChangeServerError(err error) {
	// has been closed
	if atomic.LoadInt32(c.isClosed) == int32(1) {
		log.Debugf("handleChangeServerError: changeManager has been closed")
		return
	}

	switch e := err.(type) {
	case changeServerError:
		log.Debugf("%s. Fetch a new snapshot to sync...", e.Code)
		c.snapMan.downloadDataSnapshot()
	default:
		log.Debugf("Error connecting to changeserver: %v", err)
	}
}

/*
 * Determine if any tables in changes are not present in known tables
 */
func changesHaveNewTables(a map[string]bool, changes []common.Change) bool {

	//nil maps should not be passed in.  Making the distinction between nil map and empty map
	if len(a) == 0 {
		log.Warn("Nil map passed to function changesHaveNewTables, may be bug")
		return true
	}

	for _, change := range changes {
		if !a[normalizeTableName(change.Table)] {
			log.Infof("Unable to find %s table in current known tables", change.Table)
			return true
		}
	}

	return false
}

/*
 * seqCurr.Compare() will return 1, if its newer than seqPrev,
 * else will return 0, if same, or -1 if older.
 */
func getChangeStatus(lastSeq string, currSeq string) int {
	seqPrev, err := common.ParseSequence(lastSeq)
	if err != nil {
		log.Panicf("Unable to parse previous sequence string: %v", err)
	}
	seqCurr, err := common.ParseSequence(currSeq)
	if err != nil {
		log.Panicf("Unable to parse current sequence string: %v", err)
	}
	return seqCurr.Compare(seqPrev)
}

/*
 * Returns nil if the two arrays have matching contents
 */
func scopeChanged(a, b []string) error {

	if len(a) != len(b) {
		return changeServerError{
			Code: "Scope changes detected; must get new snapshot",
		}
	}
	sort.Strings(a)
	sort.Strings(b)
	for i, v := range a {
		if v != b[i] {
			return changeServerError{
				Code: "Scope changes detected; must get new snapshot",
			}
		}
	}
	return nil
}

type offlineChangeManager struct {
}

func (o *offlineChangeManager) close() <-chan bool {
	c := make(chan bool, 1)
	c <- true
	return c
}

func (o *offlineChangeManager) pollChangeWithBackoff() {}
