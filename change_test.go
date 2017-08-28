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
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	"os"
	"time"
)

var _ = Describe("Change Agent", func() {

	Context("Change Agent Unit Tests", func() {

		var createTestDb = func(sqlfile string, dbId string) common.Snapshot {
			initDb(sqlfile, "./mockdb_change.sqlite3")
			file, err := os.Open("./mockdb_change.sqlite3")
			Expect(err).Should(Succeed())
			s := common.Snapshot{}
			err = processSnapshotServerFileResponse(dbId, file, &s)
			Expect(err).Should(Succeed())
			return s
		}

		var initializeContext = func() {
			testRouter = apid.API().Router()
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
			testMock = Mock(mockParms, testRouter)

			config.Set(configProxyServerBaseURI, testServer.URL)
			config.Set(configSnapServerBaseURI, testServer.URL)
			config.Set(configChangeServerBaseURI, testServer.URL)
			config.Set(configPollInterval, 1*time.Millisecond)
		}

		var restoreContext = func() {

			testServer.Close()
			config.Set(configProxyServerBaseURI, dummyConfigValue)
			config.Set(configSnapServerBaseURI, dummyConfigValue)
			config.Set(configChangeServerBaseURI, dummyConfigValue)
			config.Set(configPollInterval, 10*time.Millisecond)
		}

		var _ = BeforeEach(func() {
			_initPlugin(apid.AllServices())
			createManagers()
			event := createTestDb("./sql/init_mock_db.sql", "test_change")
			processSnapshot(&event)
			knownTables = extractTablesFromDB(getDB())
		})

		var _ = AfterEach(func() {
			restoreContext()
			if wipeDBAferTest {
				db, err := dataService.DB()
				Expect(err).Should(Succeed())
				_, err = db.Exec("DELETE FROM APID")
				Expect(err).Should(Succeed())
			}
			wipeDBAferTest = true
		})

		It("test change agent with authorization failure", func() {
			log.Debug("test change agent with authorization failure")
			testTokenManager := &dummyTokenManager{make(chan bool)}
			apidTokenManager = testTokenManager
			apidTokenManager.start()
			apidSnapshotManager = &dummySnapshotManager{}
			initializeContext()
			testMock.forceAuthFail()
			wipeDBAferTest = true
			apidChangeManager.pollChangeWithBackoff()
			// auth check fails
			<-testTokenManager.invalidateChan
			log.Debug("closing")
			<-apidChangeManager.close()
		})

		It("test change agent with too old snapshot", func() {
			log.Debug("test change agent with too old snapshot")
			testTokenManager := &dummyTokenManager{make(chan bool)}
			apidTokenManager = testTokenManager
			apidTokenManager.start()
			testSnapshotManager := &dummySnapshotManager{make(chan bool)}
			apidSnapshotManager = testSnapshotManager
			initializeContext()

			testMock.passAuthCheck()
			testMock.forceNewSnapshot()
			wipeDBAferTest = true
			apidChangeManager.pollChangeWithBackoff()
			<-testSnapshotManager.downloadCalledChan
			log.Debug("closing")
			<-apidChangeManager.close()
		})

		It("change agent should retry with authorization failure", func(done Done) {
			log.Debug("change agent should retry with authorization failure")
			testTokenManager := &dummyTokenManager{make(chan bool)}
			apidTokenManager = testTokenManager
			apidTokenManager.start()
			apidSnapshotManager = &dummySnapshotManager{}
			initializeContext()
			testMock.forceAuthFail()
			testMock.forceNoSnapshot()
			wipeDBAferTest = true

			apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {

				if _, ok := event.(*common.ChangeList); ok {
					closeDone := apidChangeManager.close()
					log.Debug("closing")
					go func() {
						// when close done, all handlers for the first snapshot have been executed
						<-closeDone
						close(done)
					}()

				}
			})

			apidChangeManager.pollChangeWithBackoff()
			// auth check fails
			<-testTokenManager.invalidateChan
		}, 2)

	})
})
