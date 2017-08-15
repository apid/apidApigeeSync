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
	"github.com/apigee-labs/transicator/common"
	"net/url"
)

type dummyChangeManager struct {
	pollChangeWithBackoffChan chan bool
}

func (d *dummyChangeManager) close() <-chan bool {
	c := make(chan bool, 1)
	c <- true
	return c
}

func (d *dummyChangeManager) pollChangeWithBackoff() {
	d.pollChangeWithBackoffChan <- true
}

type dummyTokenManager struct {
	invalidateChan chan bool
}

func (t *dummyTokenManager) getTokenReadyChannel() <-chan bool {
	return nil
}

func (t *dummyTokenManager) getBearerToken() string {
	return ""
}

func (t *dummyTokenManager) invalidateToken() error {
	log.Debug("invalidateToken called")
	testMock.passAuthCheck()
	t.invalidateChan <- true
	return nil
}

func (t *dummyTokenManager) getToken() *OauthToken {
	return nil
}

func (t *dummyTokenManager) close() {
	return
}

func (t *dummyTokenManager) getRetrieveNewTokenClosure(*url.URL) func(chan bool) error {
	return func(chan bool) error {
		return nil
	}
}

func (t *dummyTokenManager) start() {

}

type dummySnapshotManager struct {
	downloadCalledChan chan bool
}

func (s *dummySnapshotManager) close() <-chan bool {
	closeChan := make(chan bool)
	close(closeChan)
	return closeChan
}

func (s *dummySnapshotManager) downloadBootSnapshot() {

}

func (s *dummySnapshotManager) storeBootSnapshot(snapshot *common.Snapshot) {

}

func (s *dummySnapshotManager) downloadDataSnapshot() {
	log.Debug("dummySnapshotManager.downloadDataSnapshot() called")
	s.downloadCalledChan <- true
}

func (s *dummySnapshotManager) storeDataSnapshot(snapshot *common.Snapshot) {

}

func (s *dummySnapshotManager) downloadSnapshot(isBoot bool, scopes []string, snapshot *common.Snapshot) error {
	return nil
}

func (s *dummySnapshotManager) startOnLocalSnapshot(snapshot string) *common.Snapshot {
	return &common.Snapshot{
		SnapshotInfo: snapshot,
	}
}
