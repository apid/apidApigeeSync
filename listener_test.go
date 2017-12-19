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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
)

var _ = Describe("listener", func() {
	testCount := 0
	var testListenerMan *listenerManager
	var dummyChangeMan *dummyChangeManager
	var dummySnapMan *dummySnapshotManager
	var dummyTokenMan *dummyTokenManager

	BeforeEach(func() {
		testCount++
		dummySnapMan = &dummySnapshotManager{
			downloadCalledChan: make(chan bool, 1),
			startCalledChan:    make(chan bool, 1),
		}
		dummyTokenMan = &dummyTokenManager{
			invalidateChan: make(chan bool, 1),
		}
		dummyChangeMan = &dummyChangeManager{
			pollChangeWithBackoffChan: make(chan bool, 1),
		}
		testListenerMan = &listenerManager{
			changeMan: dummyChangeMan,
			snapMan:   dummySnapMan,
			tokenMan:  dummyTokenMan,
		}
	})

	AfterEach(func() {

	})

	It("postInitPlugins, start cleanly", func() {
		testEvent := apid.EventSelector("test event" + strconv.Itoa(testCount))
		eventService.ListenOnceFunc(testEvent, testListenerMan.postInitPlugins)
		eventService.Emit(testEvent, apid.PluginsInitializedEvent{
			Description: "test",
			Plugins: []apid.PluginData{
				{
					Name:    "name",
					Version: "0.0.1",
					ExtraData: map[string]interface{}{
						"schemaVersion": "0.0.1",
					},
				},
			},
		})
		Expect(<-dummySnapMan.downloadCalledChan).Should(BeFalse())
		Expect(<-dummyChangeMan.pollChangeWithBackoffChan).Should(BeTrue())
	})

	It("postInitPlugins, start from local db", func() {
		apidInfo.LastSnapshot = "test_snapshot"
		testEvent := apid.EventSelector("test event" + strconv.Itoa(testCount))
		eventService.ListenOnceFunc(testEvent, testListenerMan.postInitPlugins)
		eventService.Emit(testEvent, apid.PluginsInitializedEvent{
			Description: "test",
			Plugins: []apid.PluginData{
				{
					Name:    "name",
					Version: "0.0.1",
					ExtraData: map[string]interface{}{
						"schemaVersion": "0.0.1",
					},
				},
			},
		})
		Expect(<-dummySnapMan.startCalledChan).Should(BeTrue())
		Expect(<-dummyChangeMan.pollChangeWithBackoffChan).Should(BeTrue())
	})

})
