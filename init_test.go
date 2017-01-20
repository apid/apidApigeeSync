package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("init", func() {

	Context("Apid Instance display name", func() {

		It("should be hostname by defauls", func() {
			initDefaults()
			Expect(apidInfo.InstanceName).To(Equal("testhost"))
		})
		It("accept display name from config", func() {
			config.Set(configName, "aa01")
			initDefaults()
			var apidInfoLatest apidInstanceInfo
			apidInfoLatest , _ = getApidInstanceInfo()
			Expect(apidInfoLatest.InstanceName).To(Equal("aa01"))
		})

	})
})
