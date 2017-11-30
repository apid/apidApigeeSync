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
	"github.com/apid/apid-core/api"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	"net/http"
	"net/http/httptest"
	"time"
)

const (
	expectedInstanceId = "dummy"
)

var _ = Describe("Change Agent", func() {

	Context("Change Agent Unit Tests", func() {
		testCount := 0
		var testChangeMan *pollChangeManager
		var dummyDbMan *dummyDbManager
		var dummySnapMan *dummySnapshotManager
		var dummyTokenMan *dummyTokenManager
		var testServer *httptest.Server
		var testRouter apid.Router
		var testMock *MockServer
		BeforeEach(func() {
			testCount++
			dummyDbMan = &dummyDbManager{
				knownTables: map[string]bool{
					"_transicator_metadata":                true,
					"_transicator_tables":                  true,
					"attributes":                           true,
					"edgex_apid_cluster":                   true,
					"edgex_data_scope":                     true,
					"kms_api_product":                      true,
					"kms_app":                              true,
					"kms_app_credential":                   true,
					"kms_app_credential_apiproduct_mapper": true,
					"kms_company":                          true,
					"kms_company_developer":                true,
					"kms_deployment":                       true,
					"kms_developer":                        true,
					"kms_organization":                     true,
				},
				scopes: []string{"43aef41d"},
			}
			dummySnapMan = &dummySnapshotManager{
				downloadCalledChan: make(chan bool, 1),
			}
			dummyTokenMan = &dummyTokenManager{
				invalidateChan: make(chan bool, 1),
			}
			client := &http.Client{}
			testChangeMan = createChangeManager(dummyDbMan, dummySnapMan, dummyTokenMan, client)
			testChangeMan.block = 0

			// create a new API service to have a new router for testing
			testRouter = api.CreateService().Router()
			testServer = httptest.NewServer(testRouter)
			// set up mock server
			mockParms := MockParms{
				ReliableAPI:  true,
				ClusterID:    config.GetString(configApidClusterId),
				TokenKey:     config.GetString(configConsumerKey),
				TokenSecret:  config.GetString(configConsumerSecret),
				Scope:        "ert452",
				Organization: "att",
				Environment:  "prod",
			}
			apidInfo.ClusterID = expectedClusterId
			apidInfo.InstanceID = expectedInstanceId
			testMock = Mock(mockParms, testRouter)
			config.Set(configProxyServerBaseURI, testServer.URL)
			config.Set(configSnapServerBaseURI, testServer.URL)
			config.Set(configChangeServerBaseURI, testServer.URL)
			config.Set(configPollInterval, 1*time.Millisecond)

		})

		AfterEach(func() {
			testServer.Close()
			<-testChangeMan.close()
			config.Set(configProxyServerBaseURI, dummyConfigValue)
			config.Set(configSnapServerBaseURI, dummyConfigValue)
			config.Set(configChangeServerBaseURI, dummyConfigValue)
			config.Set(configPollInterval, 10*time.Millisecond)
		})

		It("test change agent with authorization failure", func() {
			log.Debug("test change agent with authorization failure")
			testMock.forceAuthFail()
			testChangeMan.pollChangeWithBackoff()
			// auth check fails
			<-dummyTokenMan.invalidateChan
			log.Debug("closing")
		})

		It("test change agent with too old snapshot", func() {
			log.Debug("test change agent with too old snapshot")
			testMock.passAuthCheck()
			testMock.forceNewSnapshot()
			testChangeMan.pollChangeWithBackoff()
			<-dummySnapMan.downloadCalledChan
			log.Debug("closing")
		})

		It("change agent should retry with authorization failure", func(done Done) {
			log.Debug("change agent should retry with authorization failure")
			testMock.forceAuthFail()
			testMock.forceNoSnapshot()
			apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {

				if _, ok := event.(*common.ChangeList); ok {
					closeDone := testChangeMan.close()
					log.Debug("closing")
					go func() {
						// when close done, all handlers for the first snapshot have been executed
						<-closeDone
						close(done)
					}()

				}
			})

			testChangeMan.pollChangeWithBackoff()
			// auth check fails
			<-dummyTokenMan.invalidateChan
			testMock.passAuthCheck()
		}, 3)

	})
})
