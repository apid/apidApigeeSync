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
	"net/http"
	"strconv"
)

var _ = Describe("init", func() {
	testCount := 0
	BeforeEach(func() {
		testCount++
	})

	Context("Apid Instance display name", func() {
		AfterEach(func() {
			eventService = apid.Events()
			apiService = apid.API()
		})

		It("should be hostname by default", func() {
			me := &mockEvent{
				listenerMap: make(map[apid.EventSelector]apid.EventHandlerFunc),
			}
			ma := &mockApi{
				handleMap: make(map[string]http.HandlerFunc),
			}
			ms := &mockService{
				config: apid.Config(),
				log:    apid.Log(),
				api:    ma,
				data:   apid.Data(),
				events: me,
			}
			testname := "test_" + strconv.Itoa(testCount)
			config.Set(configName, testname)
			pd, err := initPlugin(ms)
			Expect(err).Should(Succeed())
			Expect(apidInfo.InstanceName).To(Equal(testname))
			Expect(me.listenerMap[apid.SystemEventsSelector]).ToNot(BeNil())
			Expect(ma.handleMap[tokenEndpoint]).ToNot(BeNil())
			Expect(pd).Should(Equal(pluginData))
			Expect(apidInfo.IsNewInstance).Should(BeTrue())
			dataService.ReleaseCommonDB()
		}, 3)

	})
})
