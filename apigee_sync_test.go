package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("listener", func() {

	It("should succesfully bootstrap from clean slate", func(done Done) {
		log.Info("Starting sync tests...")
		
		// do not wipe DB after.  Lets use it
		wipeDBAferTest = false
		var lastSnapshot *common.Snapshot

		expectedSnapshotTables := common.ChangeList{
			Changes: []common.Change{common.Change{Table: "kms.company"},
						 common.Change{Table: "edgex.apid_cluster"},
						 common.Change{Table: "edgex.data_scope"}},
		}

		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {

			if s, ok := event.(*common.Snapshot); ok {

				Expect(changesRequireDDLSync(expectedSnapshotTables)).To(BeFalse())

				//add apid_cluster and data_scope since those would present if this were a real scenario
				knownTables["kms.app_credential"] = true
				knownTables["kms.app_credential_apiproduct_mapper"] = true
				knownTables["kms.developer"] = true
				knownTables["kms.company_developer"] = true
				knownTables["kms.api_product"] = true
				knownTables["kms.app"] = true

				lastSnapshot = s

				for _, t := range s.Tables {
					switch t.Name {

					case "edgex.apid_cluster":
						Expect(t.Rows).To(HaveLen(1))
						r := t.Rows[0]
						var id string
						r.Get("id", &id)
						Expect(id).To(Equal("bootstrap"))

					case "edgex.data_scope":
						Expect(t.Rows).To(HaveLen(2))
						r := t.Rows[1] // get the non-cluster row

						var id, clusterID, env, org, scope string
						r.Get("id", &id)
						r.Get("apid_cluster_id", &clusterID)
						r.Get("env", &env)
						r.Get("org", &org)
						r.Get("scope", &scope)

						Expect(id).To(Equal("ert452"))
						Expect(scope).To(Equal("ert452"))
						Expect(clusterID).To(Equal("bootstrap"))
						Expect(env).To(Equal("prod"))
						Expect(org).To(Equal("att"))
					}
				}

			} else if cl, ok := event.(*common.ChangeList); ok {
				go func(){quitPollingChangeServer <- true}()
				// ensure that snapshot switched DB versions
				Expect(apidInfo.LastSnapshot).To(Equal(lastSnapshot.SnapshotInfo))
				expectedDB, err := dataService.DBVersion(lastSnapshot.SnapshotInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(getDB() == expectedDB).Should(BeTrue())

				Expect(cl.Changes).To(HaveLen(6))

				var tables []string
				for _, c := range cl.Changes {
					tables = append(tables, c.Table)
					Expect(c.NewRow).ToNot(BeNil())

					var tenantID string
					c.NewRow.Get("tenant_id", &tenantID)
					Expect(tenantID).To(Equal("ert452"))
				}

				Expect(tables).To(ContainElement("kms.app_credential"))
				Expect(tables).To(ContainElement("kms.app_credential_apiproduct_mapper"))
				Expect(tables).To(ContainElement("kms.developer"))
				Expect(tables).To(ContainElement("kms.company_developer"))
				Expect(tables).To(ContainElement("kms.api_product"))
				Expect(tables).To(ContainElement("kms.app"))

				events.ListenFunc(apid.EventDeliveredSelector, func(e apid.Event) {
					defer GinkgoRecover()

					// allow other handler to execute to insert last_sequence
					time.Sleep(50 * time.Millisecond)
					var seq string
					err = getDB().
						QueryRow("SELECT last_sequence FROM APID_CLUSTER LIMIT 1;").
						Scan(&seq)

					Expect(err).NotTo(HaveOccurred())
					Expect(seq).To(Equal(cl.LastSequence))

					tokenManager.close()
					//sleep to ensure tokenManager has closed.  t.close() is non blocking
					time.Sleep(500 * time.Millisecond)
					close(done)
				})
			}
		})
		apid.InitializePlugins()
	}, 3)

	//this test has a dependency on the one above it.  Ideally we would write a test db to the disk instead
	It("should bootstrap from local DB if present", func(done Done) {

		/* postPluginInit event would have been emitted for the above test, clearing the list of registered plugins
		 * In general, any additional sync tests (or any tests causing postInitPlugins to fire)
		 * will need to re-register the plugin
		 */
		apid.RegisterPlugin(initPlugin)

		expectedTables := common.ChangeList{
			Changes: []common.Change{common.Change{Table: "kms.company"},
				common.Change{Table: "edgex.apid_cluster"},
				common.Change{Table: "edgex.data_scope"}},
		}

		Expect(apidInfo.LastSnapshot).NotTo(BeEmpty())

		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {

			if s, ok := event.(*common.Snapshot); ok {
				go func(){quitPollingChangeServer <- true}()
				//verify that the knownTables array has been properly populated from existing DB
				Expect(changesRequireDDLSync(expectedTables)).To(BeFalse())

				Expect(s.SnapshotInfo).Should(Equal(apidInfo.LastSnapshot))
				Expect(s.Tables).To(BeNil())

				tokenManager.close()
				//sleep to ensure tokenManager has closed.  t.close() is non blocking
				time.Sleep(500 * time.Millisecond)
				close(done)
			}
		})
		apid.InitializePlugins()

	}, 3)

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
