package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	//"time"
)

var _ = Describe("Sync", func() {

	Context("Sync", func() {

		var initializeContext = func() {
			testRouter = apid.API().Router()
			testServer = httptest.NewServer(testRouter)

			// set up mock server
			mockParms := MockParms{
				ReliableAPI:  false,
				ClusterID:    config.GetString(configApidClusterId),
				TokenKey:     config.GetString(configConsumerKey),
				TokenSecret:  config.GetString(configConsumerSecret),
				Scope:        "ert452",
				Organization: "att",
				Environment:  "prod",
			}
			testMock = Mock(mockParms, testRouter)

			config.Set(configProxyServerBaseURI, testServer.URL)
			config.Set(configSnapServerBaseURI, testServer.URL)
			config.Set(configChangeServerBaseURI, testServer.URL)
		}

		var restoreContext = func() {

			testServer.Close()

			config.Set(configProxyServerBaseURI, dummyConfigValue)
			config.Set(configSnapServerBaseURI, dummyConfigValue)
			config.Set(configChangeServerBaseURI, dummyConfigValue)

		}

		It("should succesfully bootstrap from clean slate", func(done Done) {
			log.Info("Starting sync tests...")
			var closeDone <-chan bool
			initializeContext()
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
					closeDone = changeManager.close()
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

					go func() {
						// when close done, all handlers for the first changeList have been executed
						<-closeDone
						defer GinkgoRecover()
						// allow other handler to execute to insert last_sequence
						var seq string
						//for seq = ""; seq == ""; {
						//	time.Sleep(50 * time.Millisecond)
						err := getDB().
							QueryRow("SELECT last_sequence FROM APID_CLUSTER LIMIT 1;").
							Scan(&seq)
						Expect(err).NotTo(HaveOccurred())
						//}
						Expect(seq).To(Equal(cl.LastSequence))

						restoreContext()
						close(done)
					}()

				}
			})
			pie := apid.PluginsInitializedEvent{
				Description: "plugins initialized",
			}
			pie.Plugins = append(pie.Plugins, pluginData)
			postInitPlugins(pie)
		}, 3)

		It("should bootstrap from local DB if present", func(done Done) {

			var closeDone <-chan bool

			initializeContext()
			expectedTables := common.ChangeList{
				Changes: []common.Change{common.Change{Table: "kms.company"},
					common.Change{Table: "edgex.apid_cluster"},
					common.Change{Table: "edgex.data_scope"}},
			}
			Expect(apidInfo.LastSnapshot).NotTo(BeEmpty())

			apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {

				if s, ok := event.(*common.Snapshot); ok {
					// In this test, the changeManager.pollChangeWithBackoff() has not been launched when changeManager closed
					// This is because the changeManager.pollChangeWithBackoff() in bootstrap() happened after this handler
					closeDone = changeManager.close()
					go func() {
						// when close done, all handlers for the first snapshot have been executed
						<-closeDone
						//verify that the knownTables array has been properly populated from existing DB
						Expect(changesRequireDDLSync(expectedTables)).To(BeFalse())

						Expect(s.SnapshotInfo).Should(Equal(apidInfo.LastSnapshot))
						Expect(s.Tables).To(BeNil())

						restoreContext()
						close(done)
					}()

				}
			})
			pie := apid.PluginsInitializedEvent{
				Description: "plugins initialized",
			}
			pie.Plugins = append(pie.Plugins, pluginData)
			postInitPlugins(pie)

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
		}, 3)

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

		It("Verify the Sequence Number Logic works as expected", func() {
			Expect(getChangeStatus("1.1.1", "1.1.2")).To(Equal(1))
			Expect(getChangeStatus("1.1.1", "1.2.1")).To(Equal(1))
			Expect(getChangeStatus("1.2.1", "1.2.1")).To(Equal(0))
			Expect(getChangeStatus("1.2.1", "1.2.2")).To(Equal(1))
			Expect(getChangeStatus("2.2.1", "1.2.2")).To(Equal(-1))
			Expect(getChangeStatus("2.2.1", "2.2.0")).To(Equal(-1))
		}, 3)

		/*
		 * XAPID-869, there should not be any panic if received duplicate snapshots during bootstrap
		 */
		It("Should be able to handle duplicate snapshot during bootstrap", func() {
			initializeContext()

			tokenManager = createTokenManager()
			snapManager = createSnapShotManager()
			events.Listen(ApigeeSyncEventSelector, &handler{})

			scopes := []string{apidInfo.ClusterID}
			snapshot := &common.Snapshot{}
			snapManager.downloadSnapshot(scopes, snapshot)
			snapManager.storeBootSnapshot(snapshot)
			snapManager.storeDataSnapshot(snapshot)
			restoreContext()
			<-snapManager.close()
			tokenManager.close()
		}, 3)
	})
})
