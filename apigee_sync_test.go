package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
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
})
