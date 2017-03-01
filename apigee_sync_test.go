package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("listener", func() {

	It("should bootstrap from local DB if present", func(done Done) {

		Expect(apidInfo.LastSnapshot).NotTo(BeEmpty())

		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
			defer GinkgoRecover()

			if s, ok := event.(*common.Snapshot); ok {
				Expect(s.SnapshotInfo).Should(Equal(apidInfo.LastSnapshot))
				Expect(s.Tables).To(BeNil())

				close(done)
			}
		})

		bootstrap()
	})


	It("should not panic when auth server when host lookup fails", func(done Done) {
		defer GinkgoRecover()

		Expect(func() {
			config.Set(configProxyServerBaseURI, "http://lookup-failure-auth-url")
			go getBearerToken()}).ToNot(Panic())

		time.Sleep(200 * time.Millisecond)
		close(done)
	})
	It("should not panic when auth server when url contains bad schema", func(done Done) {
		defer GinkgoRecover()

		Expect(func() {
			config.Set(configProxyServerBaseURI, "bad-schema-auth-url")
			go getBearerToken()}).ToNot(Panic())

		time.Sleep(200 * time.Millisecond)
		close(done)
	})


	It("should not panic when ss server when host lookup fails", func(done Done) {
		defer GinkgoRecover()

		Expect(func() {
			config.Set(configSnapServerBaseURI, "http://lookup-failure-ss-url")
			go downloadSnapshot()}).ToNot(Panic())

		time.Sleep(200 * time.Millisecond)
		close(done)
	})
	It("should not panic when ss server when url contains bad schema", func(done Done) {
		defer GinkgoRecover()

		Expect(func() {
			config.Set(configSnapServerBaseURI, "bad-schema-ss-url")
			go downloadSnapshot()}).ToNot(Panic())

		time.Sleep(200 * time.Millisecond)
		close(done)
	})

	It("should not panic when change server host lookup fails", func(done Done) {
		defer GinkgoRecover()

		Expect(func() {
			config.Set(configChangeServerBaseURI, "http://lookup-failure-cs-url")
			downloadDataSnapshot = true;
			token = "prevent acces token call"
			go updatePeriodicChanges()}).ToNot(Panic())

		time.Sleep(200 * time.Millisecond)
		close(done)
	})
	It("should not panic when change server url contains bad schema", func(done Done) {
		defer GinkgoRecover()

		Expect(func() {
			config.Set(configChangeServerBaseURI, "bad-schema-cs-url")
			downloadDataSnapshot = true;
			token = "prevent getBearerToken call"
			go updatePeriodicChanges()}).ToNot(Panic())

		time.Sleep(200 * time.Millisecond)
		close(done)
	})
})