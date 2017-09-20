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
)

var _ = Describe("init", func() {
	var _ = BeforeEach(func() {
		_initPlugin(apid.AllServices())
	})

	Context("Apid Instance display name", func() {

		It("should be hostname by default", func() {
			log.Info("Starting init tests...")

			initConfigDefaults()
			Expect(apidInfo.InstanceName).To(Equal("testhost"))
		}, 3)

		It("accept display name from config", func() {
			config.Set(configName, "aa01")
			initConfigDefaults()
			var apidInfoLatest apidInstanceInfo
			apidInfoLatest, _ = getApidInstanceInfo()
			Expect(apidInfoLatest.InstanceName).To(Equal("aa01"))
			Expect(apidInfoLatest.LastSnapshot).To(Equal(""))
		}, 3)

	})

	It("should put apigeesync_apid_instance_id value in config", func() {
		instanceID := config.GetString(configApidInstanceID)
		Expect(instanceID).NotTo(BeEmpty())
		Expect(instanceID).To(Equal(apidInfo.InstanceID))
	})
})
