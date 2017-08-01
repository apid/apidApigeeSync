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
	var testListenerMan *listenerManager
	var mockDbMan *dummyDbMan
	var testCount int
	BeforeEach(func() {
		mockDbMan = &dummyDbMan{}
		testListenerMan = &listenerManager{
			dbm: mockDbMan,
		}
		testCount += 1
	})

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
			mockDbMan.clusterCount = 2
			Expect(func() { testListenerMan.processSnapshot(&common.Snapshot{}) }).To(Panic())
		}, 3)

		It("test scope change", func() {
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
	})

	Context("ApigeeSync change event", func() {

		Context(LISTENER_TABLE_APID_CLUSTER, func() {

			It("insert event should panic", func() {
				csEvent := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_APID_CLUSTER,
						},
					},
				}

				Expect(func() { testListenerMan.processChangeList(&csEvent) }).To(Panic())
			}, 3)

			It("update event should panic", func() {
				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Update,
							Table:     LISTENER_TABLE_APID_CLUSTER,
						},
					},
				}

				Expect(func() { testListenerMan.processChangeList(&event) }).To(Panic())
				//restore the last snapshot
			}, 3)

		})

		Context(LISTENER_TABLE_DATA_SCOPE, func() {

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
