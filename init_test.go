package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("init", func() {

	Context("Apid Instance display name", func() {

		It("should be hostname by default", func() {
			log.Info("Starting init tests...")

			initConfigDefaults()
			Expect(apidInfo.InstanceName).To(Equal("testhost"))
		})

		It("accept display name from config", func() {
			config.Set(configName, "aa01")
			initConfigDefaults()
			var apidInfoLatest apidInstanceInfo
			apidInfoLatest, _ = getApidInstanceInfo()
			Expect(apidInfoLatest.InstanceName).To(Equal("aa01"))
			Expect(apidInfoLatest.LastSnapshot).To(Equal(""))
		})

	})

	It("should put apigeesync_apid_instance_id value in config", func() {
		instanceID := config.GetString(configApidInstanceID)
		Expect(instanceID).NotTo(BeEmpty())
		Expect(instanceID).To(Equal(apidInfo.InstanceID))
	})
})
