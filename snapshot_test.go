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
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"
)

var _ = Describe("Snapshot Manager", func() {
	testCount := 0
	var dummyDbMan *dummyDbManager
	BeforeEach(func() {
		testCount++
		dummyDbMan = &dummyDbManager{}
	})

	Context("offlineSnapshotManager", func() {
		var testSnapMan *offlineSnapshotManager
		BeforeEach(func() {
			testSnapMan = &offlineSnapshotManager{
				dbMan: dummyDbMan,
			}
		})
		AfterEach(func() {
			<-testSnapMan.close()
		})

		It("should have error if download called", func() {
			Expect(testSnapMan.downloadDataSnapshot()).ToNot(Succeed())
			Expect(func() { testSnapMan.downloadBootSnapshot() }).To(Panic())
		})

		It("startOnDataSnapshot should emit events", func() {
			called := false
			eventService.ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
				if _, ok := event.(*common.Snapshot); ok {
					called = true
				}
			})
			snapshotId := "test_snapshot_" + strconv.Itoa(testCount)
			Expect(testSnapMan.startOnDataSnapshot(snapshotId)).Should(Succeed())
			Expect(dummyDbMan.snapshot.SnapshotInfo).Should(Equal(snapshotId))
			Expect(called).Should(BeTrue())
		})
	})

	Context("apidSnapshotManager", func() {
		var testSnapMan *apidSnapshotManager
		var dummyTokenMan *dummyTokenManager
		var testServer *httptest.Server
		var testRouter apid.Router
		var testMock *MockServer
		BeforeEach(func() {
			dummyTokenMan = &dummyTokenManager{
				invalidateChan: make(chan bool, 1),
			}
			client := &http.Client{}
			testSnapMan = createSnapShotManager(dummyDbMan, dummyTokenMan, client)

			// create a new API service to have a new router for testing
			testRouter = api.CreateService().Router()
			testServer = httptest.NewServer(testRouter)
			// set up mock server
			mockParms := MockParms{
				ReliableAPI:  true,
				ClusterID:    config.GetString(configApidClusterId),
				TokenKey:     config.GetString(configConsumerKey),
				TokenSecret:  config.GetString(configConsumerSecret),
				Scope:        "",
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

			initialBackoffInterval = time.Millisecond
			testMock.oauthToken = "test_token_" + strconv.Itoa(testCount)
			dummyTokenMan.token = testMock.oauthToken
		})

		AfterEach(func() {
			<-testSnapMan.close()
		})

		It("downloadBootSnapshot happy path", func() {
			testMock.normalAuthCheck()
			testSnapMan.downloadBootSnapshot()
			Expect(dummyDbMan.isDataSnapshot).Should(BeFalse())
			Expect(dummyDbMan.snapshot.SnapshotInfo).Should(Equal(bootstrapSnapshotName))
		})

		It("downloadBootSnapshot should retry for auth failure", func() {
			testMock.forceAuthFailOnce()
			testSnapMan.downloadBootSnapshot()
			Expect(dummyDbMan.isDataSnapshot).Should(BeFalse())
			Expect(dummyDbMan.snapshot.SnapshotInfo).Should(Equal(bootstrapSnapshotName))
			Expect(<-dummyTokenMan.invalidateChan).Should(BeTrue())
		})

		It("downloadDataSnapshot happy path", func() {
			testMock.params.Scope = "test_scope_" + strconv.Itoa(testCount)
			dummyDbMan.scopes = []string{testMock.params.Scope}
			testMock.normalAuthCheck()
			testSnapMan.downloadDataSnapshot()
			Expect(dummyDbMan.isDataSnapshot).Should(BeTrue())
			Expect(dummyDbMan.snapshot.SnapshotInfo).Should(Equal(testMock.snapshotID))
		})

		It("downloadDataSnapshot should retry for auth failure", func() {
			testMock.params.Scope = "test_scope_" + strconv.Itoa(testCount)
			dummyDbMan.scopes = []string{testMock.params.Scope}
			testMock.forceAuthFailOnce()
			testSnapMan.downloadDataSnapshot()
			Expect(dummyDbMan.isDataSnapshot).Should(BeTrue())
			Expect(dummyDbMan.snapshot.SnapshotInfo).Should(Equal(testMock.snapshotID))
			Expect(<-dummyTokenMan.invalidateChan).Should(BeTrue())
		})

	})

})
