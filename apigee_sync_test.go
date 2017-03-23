package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("listener", func() {

	It("should bootstrap from local DB if present", func(done Done) {
		expectedTables := common.ChangeList{
			Changes: []common.Change{common.Change{Table: "kms.company"},
				common.Change{Table: "edgex.apid_cluster"},
				common.Change{Table: "edgex.data_scope"}},
		}

		Expect(apidInfo.LastSnapshot).NotTo(BeEmpty())

		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
			defer GinkgoRecover()

			if s, ok := event.(*common.Snapshot); ok {

				//verify that the knownTables array has been properly populated from existing DB
				Expect(changesRequireDDLSync(expectedTables)).To(BeFalse())

				Expect(s.SnapshotInfo).Should(Equal(apidInfo.LastSnapshot))
				Expect(s.Tables).To(BeNil())

				close(done)
			}
		})

		bootstrap(make(chan bool), make(chan bool))
	})

	It("should correctly identify non-proper subsets with respect to maps", func() {

		//test b proper subset of a
		Expect(changesHaveNewTables(map[string]bool{"a": true, "b": true},
			[]common.Change{common.Change{Table: "b"}},
		)).To(BeFalse())

		//test a == b
		Expect(changesHaveNewTables(map[string]bool{"a": true, "b": true},
			[]common.Change{common.Change{Table: "a"}, common.Change{Table: "b"}},
		)).To(BeFalse())

		//test b superset of a
		Expect(changesHaveNewTables(map[string]bool{"a": true, "b": true},
			[]common.Change{common.Change{Table: "a"}, common.Change{Table: "b"}, common.Change{Table: "c"}},
		)).To(BeTrue())

		//test b not subset of a
		Expect(changesHaveNewTables(map[string]bool{"a": true, "b": true},
			[]common.Change{common.Change{Table: "c"}},
		)).To(BeTrue())

		//test a empty
		Expect(changesHaveNewTables(map[string]bool{},
			[]common.Change{common.Change{Table: "a"}},
		)).To(BeTrue())

		//test b empty
		Expect(changesHaveNewTables(map[string]bool{"a": true, "b": true},
			[]common.Change{},
		)).To(BeFalse())

		//test b nil
		Expect(changesHaveNewTables(map[string]bool{"a": true, "b": true}, nil)).To(BeTrue())

		//test a nil
		Expect(changesHaveNewTables(nil,
			[]common.Change{common.Change{Table: "a"}},
		)).To(BeTrue())
	})

	// todo: disabled for now -
	// there is precondition I haven't been able to track down that breaks this test on occasion
	XIt("should process a new snapshot when change server requires it", func(done Done) {
		oldSnap := apidInfo.LastSnapshot
		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
			defer GinkgoRecover()

			if s, ok := event.(*common.Snapshot); ok {
				Expect(s.SnapshotInfo).NotTo(Equal(oldSnap))
				close(done)
			}
		})
		testMock.forceNewSnapshot()
	})
})
