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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/apigee-labs/transicator/common"
	"os"
)

var _ = Describe("listener", func() {

	var createTestDb = func(sqlfile string, dbId string) common.Snapshot {
		initDb(sqlfile, "./mockdb.sqlite3")
		file, err := os.Open("./mockdb.sqlite3")
		Expect(err).ShouldNot(HaveOccurred())

		s := common.Snapshot{}
		err = processSnapshotServerFileResponse(dbId, file, &s)
		Expect(err).ShouldNot(HaveOccurred())
		return s
	}

	Context("ApigeeSync snapshot event", func() {

		It("should fail if more than one apid_cluster rows", func() {
			event := createTestDb("./sql/init_listener_test_duplicate_apids.sql", "test_snapshot_fail_multiple_clusters")
			Expect(func() { processSnapshot(&event) }).To(Panic())
		}, 3)

		It("should fail if more than one apid_cluster rows", func() {
			newScopes := []string{"foo"}
			scopes := []string{"bar"}
			Expect(scopeChanged(newScopes, scopes)).To(Equal(changeServerError{Code: "Scope changes detected; must get new snapshot"}))
			newScopes = []string{"foo", "bar"}
			scopes = []string{"bar"}
			Expect(scopeChanged(newScopes, scopes)).To(Equal(changeServerError{Code: "Scope changes detected; must get new snapshot"}))
			newScopes = []string{"foo"}
			scopes = []string{"bar", "foo"}
			Expect(scopeChanged(newScopes, scopes)).To(Equal(changeServerError{Code: "Scope changes detected; must get new snapshot"}))
			newScopes = []string{"foo", "bar"}
			scopes = []string{"bar", "foo"}
			Expect(scopeChanged(newScopes, scopes)).To(BeNil())

		}, 3)

		It("should process a valid Snapshot", func() {

			event := createTestDb("./sql/init_listener_test_valid_snapshot.sql", "test_snapshot_valid")

			processSnapshot(&event)

			info, err := getApidInstanceInfo()
			Expect(err).NotTo(HaveOccurred())

			Expect(info.LastSnapshot).To(Equal(event.SnapshotInfo))

			db := getDB()

			expectedDB, err := dataService.DBVersion(event.SnapshotInfo)
			Expect(err).NotTo(HaveOccurred())

			Expect(db == expectedDB).Should(BeTrue())

			// apid Cluster
			var dcs []dataApidCluster

			rows, err := db.Query(`
			SELECT id, name, description, umbrella_org_app_name,
				created, created_by, updated, updated_by
			FROM EDGEX_APID_CLUSTER`)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			c := dataApidCluster{}
			for rows.Next() {
				rows.Scan(&c.ID, &c.Name, &c.Description, &c.OrgAppName,
					&c.Created, &c.CreatedBy, &c.Updated, &c.UpdatedBy)
				dcs = append(dcs, c)
			}

			Expect(len(dcs)).To(Equal(1))
			dc := dcs[0]

			Expect(dc.ID).To(Equal("i"))
			Expect(dc.Name).To(Equal("n"))
			Expect(dc.Description).To(Equal("d"))
			Expect(dc.OrgAppName).To(Equal("o"))
			Expect(dc.Created).To(Equal("c"))
			Expect(dc.CreatedBy).To(Equal("c"))
			Expect(dc.Updated).To(Equal("u"))
			Expect(dc.UpdatedBy).To(Equal("u"))

			// Data Scope
			var dds []dataDataScope

			rows, err = db.Query(`
			SELECT id, apid_cluster_id, scope, org,
				env, created, created_by, updated,
				updated_by
			FROM EDGEX_DATA_SCOPE`)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			d := dataDataScope{}
			for rows.Next() {
				rows.Scan(&d.ID, &d.ClusterID, &d.Scope, &d.Org,
					&d.Env, &d.Created, &d.CreatedBy, &d.Updated,
					&d.UpdatedBy)
				dds = append(dds, d)
			}

			Expect(len(dds)).To(Equal(3))
			ds := dds[0]

			Expect(ds.ID).To(Equal("i"))
			Expect(ds.Org).To(Equal("o"))
			Expect(ds.Env).To(Equal("e1"))
			Expect(ds.Scope).To(Equal("s1"))
			Expect(ds.Created).To(Equal("c"))
			Expect(ds.CreatedBy).To(Equal("c"))
			Expect(ds.Updated).To(Equal("u"))
			Expect(ds.UpdatedBy).To(Equal("u"))

			ds = dds[1]
			Expect(ds.Env).To(Equal("e2"))
			Expect(ds.Scope).To(Equal("s1"))
			ds = dds[2]
			Expect(ds.Env).To(Equal("e3"))
			Expect(ds.Scope).To(Equal("s2"))

			scopes := findScopesForId("a")
			Expect(len(scopes)).To(Equal(2))
			Expect(scopes[0]).To(Equal("s1"))
			Expect(scopes[1]).To(Equal("s2"))

			//restore the last snapshot
		}, 3)
	})

	Context("ApigeeSync change event", func() {

		Context(LISTENER_TABLE_APID_CLUSTER, func() {

			It("insert event should panic", func() {
				ssEvent := createTestDb("./sql/init_listener_test_valid_snapshot.sql", "test_changes_insert_panic")
				processSnapshot(&ssEvent)

				//save the last snapshot, so we can restore it at the end of this context

				csEvent := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_APID_CLUSTER,
						},
					},
				}

				Expect(func() { processChangeList(&csEvent) }).To(Panic())
			}, 3)

			It("update event should panic", func() {
				ssEvent := createTestDb("./sql/init_listener_test_valid_snapshot.sql", "test_changes_update_panic")
				processSnapshot(&ssEvent)

				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Update,
							Table:     LISTENER_TABLE_APID_CLUSTER,
						},
					},
				}

				Expect(func() { processChangeList(&event) }).To(Panic())
				//restore the last snapshot
			}, 3)

		})

		Context(LISTENER_TABLE_DATA_SCOPE, func() {

			It("insert event should add", func() {
				ssEvent := createTestDb("./sql/init_listener_test_no_datascopes.sql", "test_changes_insert")
				processSnapshot(&ssEvent)

				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							NewRow: common.Row{
								"id":               &common.ColumnVal{Value: "i"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s1"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
								"_change_selector": &common.ColumnVal{Value: "cs"},
							},
						},
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							NewRow: common.Row{
								"id":               &common.ColumnVal{Value: "j"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s2"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
								"_change_selector": &common.ColumnVal{Value: "cs"},
							},
						},
					},
				}

				processChangeList(&event)

				var dds []dataDataScope

				rows, err := getDB().Query(`
				SELECT id, apid_cluster_id, scope, org,
					env, created, created_by, updated,
					updated_by
				FROM EDGEX_DATA_SCOPE`)
				Expect(err).NotTo(HaveOccurred())
				defer rows.Close()

				d := dataDataScope{}
				for rows.Next() {
					rows.Scan(&d.ID, &d.ClusterID, &d.Scope, &d.Org,
						&d.Env, &d.Created, &d.CreatedBy, &d.Updated,
						&d.UpdatedBy)
					dds = append(dds, d)
				}

				//three already existing
				Expect(len(dds)).To(Equal(2))
				ds := dds[0]

				Expect(ds.ID).To(Equal("i"))
				Expect(ds.Org).To(Equal("o"))
				Expect(ds.Env).To(Equal("e"))
				Expect(ds.Scope).To(Equal("s1"))
				Expect(ds.Created).To(Equal("c"))
				Expect(ds.CreatedBy).To(Equal("c"))
				Expect(ds.Updated).To(Equal("u"))
				Expect(ds.UpdatedBy).To(Equal("u"))

				ds = dds[1]
				Expect(ds.Scope).To(Equal("s2"))

				scopes := findScopesForId("a")
				Expect(len(scopes)).To(Equal(2))
				Expect(scopes[0]).To(Equal("s1"))
				Expect(scopes[1]).To(Equal("s2"))

			}, 3)

			It("delete event should delete", func() {
				ssEvent := createTestDb("./sql/init_listener_test_no_datascopes.sql", "test_changes_delete")
				processSnapshot(&ssEvent)
				insert := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							NewRow: common.Row{
								"id":               &common.ColumnVal{Value: "i"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
								"_change_selector": &common.ColumnVal{Value: "cs"},
							},
						},
					},
				}

				processChangeList(&insert)

				delete := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Delete,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							OldRow:    insert.Changes[0].NewRow,
						},
					},
				}

				processChangeList(&delete)

				var nRows int
				err := getDB().QueryRow("SELECT count(id) FROM EDGEX_DATA_SCOPE").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())

				Expect(0).To(Equal(nRows))
			}, 3)

			It("update event should panic for data scopes table", func() {
				ssEvent := createTestDb("./sql/init_listener_test_valid_snapshot.sql", "test_update_panic")
				processSnapshot(&ssEvent)

				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Update,
							Table:     LISTENER_TABLE_DATA_SCOPE,
						},
					},
				}

				Expect(func() { processChangeList(&event) }).To(Panic())
				//restore the last snapshot
			}, 3)

			//TODO add tests for update/insert/delete cluster
		})
	})
})
