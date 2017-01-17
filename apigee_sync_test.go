package apidApigeeSync

//import (
//	"github.com/30x/apid"
//	"github.com/apigee-labs/transicator/common"
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//)
//
//var _ = Describe("api", func() {
//
//	It("should perform all sync phases", func(done Done) {
//
//		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
//			defer GinkgoRecover()
//			if _, ok := event.(*common.Snapshot); ok {
//				if phase > 1 {
//					db := getDB()
//					// verify event data (post snapshot)
//					var count int
//					err := db.QueryRow(`
//							Select count(scp.id)
//							FROM data_scope AS scp
//							INNER JOIN apid_cluster AS ap
//							WHERE scp.apid_cluster_id = ap.id
//						`).Scan(&count)
//					Expect(err).NotTo(HaveOccurred())
//					Expect(count).To(Equal(1))
//				}
//			} else if _, ok := event.(*common.ChangeList); ok {
//				// verify event data (post change)
//				// There should be 2 scopes now
//				//time.Sleep(200 * time.Millisecond)
//				db := getDB()
//				var count int
//				err := db.QueryRow(`
//						SELECT count(scp.id)
//						FROM data_scope AS scp
//						INNER JOIN apid_cluster AS ap
//						WHERE scp.apid_cluster_id = ap.id
//					`).Scan(&count)
//				Expect(err).NotTo(HaveOccurred())
//				Expect(count).To(Equal(2))
//				close(done)
//			} else {
//				Fail("Unexpected event")
//			}
//
//		})
//	})
//})
