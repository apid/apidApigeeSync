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
	"github.com/apid/apid-core/data"
	"github.com/apigee-labs/transicator/common"
	"net/http"
	"os"

	"fmt"
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
	tokenMan      tokenManager
	dbMan         DbManager
	client        *http.Client
}

func createSnapShotManager(dbMan DbManager, tokenMan tokenManager, client *http.Client) *simpleSnapShotManager {
	isClosedInt := int32(0)
	isDownloadingInt := int32(0)
	return &simpleSnapShotManager{
		quitChan:      make(chan bool, 1),
		finishChan:    make(chan bool, 1),
		isClosed:      &isClosedInt,
		isDownloading: &isDownloadingInt,
		dbMan:         dbMan,
		tokenMan:      tokenMan,
		client:        client,
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
	if err := s.dbMan.processSnapshot(snapshot, false); err != nil {
		log.Panic(err)
	}
}

// use the scope IDs from the boot snapshot to get all the data associated with the scopes
func (s *simpleSnapShotManager) downloadDataSnapshot() error {
	if atomic.SwapInt32(s.isDownloading, 1) == int32(1) {
		log.Panic("downloadDataSnapshot: only 1 thread can download snapshot at the same time!")
	}
	defer atomic.StoreInt32(s.isDownloading, int32(0))

	// has been closed
	if atomic.LoadInt32(s.isClosed) == int32(1) {
		log.Warn("snapShotManager: downloadDataSnapshot called on closed snapShotManager")
		return nil
	}

	log.Debug("download Snapshot for data scopes")

	scopes, err := s.dbMan.findScopesForId(apidInfo.ClusterID)
	if err != nil {
		return err
	}
	scopes = append(scopes, apidInfo.ClusterID)
	snapshot := &common.Snapshot{}
	err = s.downloadSnapshot(false, scopes, snapshot)
	if err != nil {
		// this may happen during shutdown
		if _, ok := err.(quitSignalError); ok {
			log.Warn("downloadDataSnapshot failed due to shutdown: " + err.Error())
		}
		return err
	}
	return s.startOnDataSnapshot(snapshot.SnapshotInfo)
}

// Skip Downloading snapshot if there is already a snapshot available from previous run
func (s *simpleSnapShotManager) startOnDataSnapshot(snapshotName string) error {
	log.Infof("Processing snapshot: %s", snapshotName)
	snapshot := &common.Snapshot{
		SnapshotInfo: snapshotName,
	}
	if err := s.dbMan.processSnapshot(snapshot, true); err != nil {
		return err
	}
	log.Info("Emitting Snapshot to plugins")
	select {
	case <-time.After(pluginTimeout):
		return fmt.Errorf("timeout, plugins failed to respond to snapshot")
	case <-eventService.Emit(ApigeeSyncEventSelector, snapshot):
		// the new snapshot has been processed
	}
	return nil
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
	attemptDownload := s.getAttemptDownloadClosure(isBoot, snapshot, uri)
	pollWithBackoff(s.quitChan, attemptDownload, handleSnapshotServerError)
	return nil
}

func (s *simpleSnapShotManager) getAttemptDownloadClosure(isBoot bool, snapshot *common.Snapshot, uri string) func(chan bool) error {
	return func(_ chan bool) error {

		var tid string
		req, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			// should never happen, but if it does, it's unrecoverable anyway
			log.Panicf("Snapshotserver comm error: %v", err)
		}
		addHeaders(req, s.tokenMan.getBearerToken())

		var processSnapshotResponse func(string, io.Reader, *common.Snapshot) error

		if config.GetString(configSnapshotProtocol) != "sqlite" {
			log.Panic("Only currently supported snashot protocol is sqlite")

		}
		req.Header.Set("Accept", "application/transicator+sqlite")
		processSnapshotResponse = processSnapshotServerFileResponse

		// Issue the request to the snapshot server
		r, err := s.client.Do(req)
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
		if isBoot {
			tid = "bootstrap"
		} else {
			tid = r.Header.Get("Transicator-Snapshot-TXID")
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
	dbDir := dbPath[0 : len(dbPath)-7]
	log.Infof("Attempting to stream the sqlite snapshot to %s", dbPath)

	// if exists, delete the old snapshot file
	if _, err := os.Stat(dbDir); !os.IsNotExist(err) {
		if err = os.RemoveAll(dbDir); err != nil {
			log.Errorf("Failed to delete old snapshot; %v", err)
			return err
		}
	}

	//this path includes the sqlite3 file name.  why does mkdir all stop at parent??
	log.Infof("Creating directory with mkdirall %s", dbPath)
	err := os.MkdirAll(dbDir, 0700)
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

type offlineSnapshotManager struct {
	dbMan DbManager
}

func (o *offlineSnapshotManager) close() <-chan bool {
	c := make(chan bool, 1)
	c <- true
	return c
}

func (o *offlineSnapshotManager) downloadBootSnapshot() {
	log.Panic("downloadBootSnapshot called for offlineSnapshotManager")
}

func (o *offlineSnapshotManager) downloadDataSnapshot() error {
	return fmt.Errorf("downloadDataSnapshot called for offlineSnapshotManager")
}

func (o *offlineSnapshotManager) startOnDataSnapshot(snapshotName string) error {
	log.Infof("Processing snapshot: %s", snapshotName)
	snapshot := &common.Snapshot{
		SnapshotInfo: snapshotName,
	}
	if err := o.dbMan.processSnapshot(snapshot, true); err != nil {
		return err
	}
	log.Info("Emitting Snapshot to plugins")
	select {
	case <-time.After(pluginTimeout):
		return fmt.Errorf("timeout, plugins failed to respond to snapshot")
	case <-eventService.Emit(ApigeeSyncEventSelector, snapshot):
		// the new snapshot has been processed
	}
	return nil
}
