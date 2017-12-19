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
	"github.com/apid/apid-core"
	"github.com/apigee-labs/transicator/common"
	"math/rand"
	"net/http"
	"strconv"
)

type mockService struct {
	config apid.ConfigService
	log    apid.LogService
	api    apid.APIService
	data   apid.DataService
	events apid.EventsService
}

func (s *mockService) API() apid.APIService {
	return s.api
}

func (s *mockService) Config() apid.ConfigService {
	return s.config
}

func (s *mockService) Data() apid.DataService {
	return s.data
}

func (s *mockService) Events() apid.EventsService {
	return s.events
}

func (s *mockService) Log() apid.LogService {
	return s.log
}

type mockApi struct {
	handleMap map[string]http.HandlerFunc
}

func (m *mockApi) Listen() error {
	return nil
}
func (m *mockApi) Handle(path string, handler http.Handler) apid.Route {
	return nil
}
func (m *mockApi) HandleFunc(path string, handlerFunc http.HandlerFunc) apid.Route {
	m.handleMap[path] = handlerFunc
	return apid.API().HandleFunc(path+strconv.Itoa(rand.Int()), handlerFunc)
}
func (m *mockApi) Vars(r *http.Request) map[string]string {
	return nil
}

func (m *mockApi) Router() apid.Router {
	return nil
}

type mockData struct {
}

func (m *mockData) DB() (apid.DB, error) {
	return nil, nil
}

func (m *mockData) DBForID(id string) (apid.DB, error) {
	return nil, nil
}

func (m *mockData) DBVersion(version string) (apid.DB, error) {
	return nil, nil
}
func (m *mockData) DBVersionForID(id, version string) (apid.DB, error) {
	return nil, nil
}

func (m *mockData) ReleaseDB(version string)          {}
func (m *mockData) ReleaseCommonDB()                  {}
func (m *mockData) ReleaseDBForID(id, version string) {}

type mockEvent struct {
	listenerMap map[apid.EventSelector]apid.EventHandlerFunc
}

func (e *mockEvent) Emit(selector apid.EventSelector, event apid.Event) chan apid.Event {
	return nil
}

func (e *mockEvent) EmitWithCallback(selector apid.EventSelector, event apid.Event, handler apid.EventHandlerFunc) {

}

func (e *mockEvent) Listen(selector apid.EventSelector, handler apid.EventHandler) {

}

func (e *mockEvent) ListenFunc(selector apid.EventSelector, handler apid.EventHandlerFunc) {

}

func (e *mockEvent) ListenOnceFunc(selector apid.EventSelector, handler apid.EventHandlerFunc) {
	e.listenerMap[selector] = handler
}

func (e *mockEvent) StopListening(selector apid.EventSelector, handler apid.EventHandler) {

}

func (e *mockEvent) Close() {}

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
	token          string
	tokenReadyChan chan bool
}

func (t *dummyTokenManager) getTokenReadyChannel() <-chan bool {
	return t.tokenReadyChan
}

func (t *dummyTokenManager) getBearerToken() string {
	return t.token
}

func (t *dummyTokenManager) invalidateToken() {
	log.Debug("invalidateToken called")
	t.invalidateChan <- true
}

func (t *dummyTokenManager) close() {
	return
}

func (t *dummyTokenManager) start() {

}

type dummySnapshotManager struct {
	downloadCalledChan chan bool
	startCalledChan    chan bool
}

func (s *dummySnapshotManager) close() <-chan bool {
	closeChan := make(chan bool)
	close(closeChan)
	return closeChan
}

func (s *dummySnapshotManager) downloadBootSnapshot() {
	s.downloadCalledChan <- false
}

func (s *dummySnapshotManager) downloadDataSnapshot() error {
	s.downloadCalledChan <- true
	return nil
}

func (s *dummySnapshotManager) startOnDataSnapshot(snapshot string) error {
	s.startCalledChan <- true
	return nil
}

type dummyDbManager struct {
	lastSequence   string
	knownTables    map[string]bool
	scopes         []string
	snapshot       *common.Snapshot
	isDataSnapshot bool
	lastSeqUpdated chan string
}

func (d *dummyDbManager) initDB() error {
	return nil
}
func (d *dummyDbManager) setDB(db apid.DB) {

}
func (d *dummyDbManager) getLastSequence() (lastSequence string) {
	return d.lastSequence
}
func (d *dummyDbManager) findScopesForId(configId string) (scopes []string, err error) {
	return d.scopes, nil
}
func (d *dummyDbManager) updateLastSequence(lastSequence string) error {
	d.lastSeqUpdated <- lastSequence
	return nil
}
func (d *dummyDbManager) getApidInstanceInfo() (info apidInstanceInfo, err error) {
	return apidInstanceInfo{
		InstanceID:   "",
		InstanceName: "",
		ClusterID:    "",
		LastSnapshot: "",
	}, nil
}
func (d *dummyDbManager) processChangeList(changes *common.ChangeList) error {
	return nil
}
func (d *dummyDbManager) processSnapshot(snapshot *common.Snapshot, isDataSnapshot bool) error {
	d.snapshot = snapshot
	d.isDataSnapshot = isDataSnapshot
	return nil
}
func (d *dummyDbManager) getKnowTables() map[string]bool {
	return d.knownTables
}
