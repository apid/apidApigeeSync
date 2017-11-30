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
	"database/sql"
	"github.com/apid/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"sort"
	"strconv"
)

var _ = Describe("data access tests", func() {
	testCount := 0
	var testDbMan *dbManager
	var dbVersion string
	BeforeEach(func() {
		testDbMan = creatDbManager()
		testCount++
		dbVersion = "data_test_" + strconv.Itoa(testCount)
		db, err := dataService.DBVersion(dbVersion)
		Expect(err).Should(Succeed())
		testDbMan.setDB(db)
	})

	AfterEach(func() {
		dataService.ReleaseDB(dbVersion)
	})

	It("check scope changes", func() {
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

	})

	Context("build Sql", func() {
		It("unit test buildUpdateSql with single primary key", func() {
			testRow := common.Row{
				"id": {
					Value: "ch_api_product_2",
				},
				"api_resources": {
					Value: "{}",
				},
				"environments": {
					Value: "{Env_0, Env_1}",
				},
				"tenant_id": {
					Value: "tenant_id_0",
				},
				"_change_selector": {
					Value: "test_org0",
				},
			}

			var orderedColumns []string
			for column := range testRow {
				orderedColumns = append(orderedColumns, column)
			}
			sort.Strings(orderedColumns)

			result := testDbMan.buildUpdateSql("TEST_TABLE", orderedColumns, testRow, []string{"id"})
			Expect("UPDATE TEST_TABLE SET _change_selector=$1, api_resources=$2, environments=$3, id=$4, tenant_id=$5" +
				" WHERE id=$6").To(Equal(result))
		})

		It("unit test buildUpdateSql with composite primary key", func() {
			testRow := common.Row{
				"id1": {
					Value: "composite-key-1",
				},
				"id2": {
					Value: "composite-key-2",
				},
				"api_resources": {
					Value: "{}",
				},
				"environments": {
					Value: "{Env_0, Env_1}",
				},
				"tenant_id": {
					Value: "tenant_id_0",
				},
				"_change_selector": {
					Value: "test_org0",
				},
			}

			var orderedColumns []string
			for column := range testRow {
				orderedColumns = append(orderedColumns, column)
			}
			sort.Strings(orderedColumns)

			result := testDbMan.buildUpdateSql("TEST_TABLE", orderedColumns, testRow, []string{"id1", "id2"})
			Expect("UPDATE TEST_TABLE SET _change_selector=$1, api_resources=$2, environments=$3, id1=$4, id2=$5, tenant_id=$6" +
				" WHERE id1=$7 AND id2=$8").To(Equal(result))
		})

		It("Properly constructs insert sql for one row", func() {
			newRow := common.Row{
				"id": {
					Value: "new_id",
				},
				"api_resources": {
					Value: "{/**}",
				},
				"environments": {
					Value: "{test}",
				},
				"tenant_id": {
					Value: "43aef41d",
				},
				"description": {
					Value: "new description",
				},
				"created_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"updated_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"_change_selector": {
					Value: "43aef41d",
				},
			}

			var orderedColumns []string
			for column := range newRow {
				orderedColumns = append(orderedColumns, column)
			}
			sort.Strings(orderedColumns)

			expectedSql := "INSERT INTO api_product(_change_selector,api_resources,created_at,description,environments,id,tenant_id,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
			Expect(expectedSql).To(Equal(testDbMan.buildInsertSql("api_product", orderedColumns, []common.Row{newRow})))
		})

		It("Properly constructs insert sql for multiple rows", func() {
			newRow1 := common.Row{
				"id": {
					Value: "1",
				},
				"api_resources": {
					Value: "{/**}",
				},
				"environments": {
					Value: "{test}",
				},
				"tenant_id": {
					Value: "43aef41d",
				},
				"description": {
					Value: "new description",
				},
				"created_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"updated_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"_change_selector": {
					Value: "43aef41d",
				},
			}
			newRow2 := common.Row{
				"id": {
					Value: "2",
				},
				"api_resources": {
					Value: "{/**}",
				},
				"environments": {
					Value: "{test}",
				},
				"tenant_id": {
					Value: "43aef41d",
				},
				"description": {
					Value: "new description",
				},
				"created_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"updated_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"_change_selector": {
					Value: "43aef41d",
				},
			}

			var orderedColumns []string
			for column := range newRow1 {
				orderedColumns = append(orderedColumns, column)
			}
			sort.Strings(orderedColumns)

			expectedSql := "INSERT INTO api_product(_change_selector,api_resources,created_at,description,environments,id,tenant_id,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8),($9,$10,$11,$12,$13,$14,$15,$16)"
			Expect(expectedSql).To(Equal(testDbMan.buildInsertSql("api_product", orderedColumns, []common.Row{newRow1, newRow2})))
		})

		It("Properly constructs sql prepare for Delete", func() {
			createBootstrapTables(testDbMan.getDB())
			row := common.Row{
				"id": {
					Value: "new_id",
				},
				"api_resources": {
					Value: "{/**}",
				},
				"environments": {
					Value: "{test}",
				},
				"tenant_id": {
					Value: "43aef41d",
				},
				"description": {
					Value: "new description",
				},
				"created_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"updated_at": {
					Value: "2017-03-01 22:50:41.75+00:00",
				},
				"_change_selector": {
					Value: "43aef41d",
				},
			}

			pkeys, err := testDbMan.getPkeysForTable("kms_api_product")
			Expect(err).Should(Succeed())
			sql := testDbMan.buildDeleteSql("kms_api_product", row, pkeys)
			Expect(sql).To(Equal("DELETE FROM kms_api_product WHERE created_at=$1 AND id=$2 AND tenant_id=$3 AND updated_at=$4"))
		})
	})

	Context("Process Changelist", func() {
		BeforeEach(func() {
			createBootstrapTables(testDbMan.getDB())
		})

		Context("Update processing", func() {
			It("test update with composite primary key", func() {
				event := &common.ChangeList{}

				//this needs to match what is actually in the DB
				oldRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "A product for testing Greg",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				newRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "new description",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    oldRow,
						Operation: 1,
					},
				}
				//insert and assert success
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//create update event
				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    oldRow,
						NewRow:    newRow,
						Operation: 2,
					},
				}

				//do the update
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				err = testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='new description'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

			})

			It("update should succeed if newrow modifies the primary key", func() {
				event := &common.ChangeList{}

				//this needs to match what is actually in the DB
				oldRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "A product for testing Greg",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				newRow := common.Row{
					"id": {
						Value: "new_id",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "new description",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    oldRow,
						Operation: 1,
					},
				}
				//insert and assert success
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//create update event
				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    oldRow,
						NewRow:    newRow,
						Operation: 2,
					},
				}

				//do the update
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				err = testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='new_id' and description='new description'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))
			})

			It("update should succeed if newrow contains fewer fields than oldrow", func() {
				event := &common.ChangeList{}

				oldRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "A product for testing Greg",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				newRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "new description",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    oldRow,
						NewRow:    newRow,
						Operation: 2,
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    oldRow,
						Operation: 1,
					},
				}
				//insert and assert success
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//create update event
				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    oldRow,
						NewRow:    newRow,
						Operation: 2,
					},
				}

				//do the update
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				err = testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='new description'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))
			})

			It("update should succeed if oldrow contains fewer fields than newrow", func() {
				event := &common.ChangeList{}

				oldRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "A product for testing Greg",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				newRow := common.Row{
					"id": {
						Value: "87a4bfaa-b3c4-47cd-b6c5-378cdb68610c",
					},
					"api_resources": {
						Value: "{/**}",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "43aef41d",
					},
					"description": {
						Value: "new description",
					},
					"created_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"updated_at": {
						Value: "2017-03-01 22:50:41.75+00:00",
					},
					"_change_selector": {
						Value: "43aef41d",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    oldRow,
						NewRow:    newRow,
						Operation: 2,
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    oldRow,
						Operation: 1,
					},
				}
				//insert and assert success
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//create update event
				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    oldRow,
						NewRow:    newRow,
						Operation: 2,
					},
				}

				//do the update
				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				err = testDbMan.getDB().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='new description'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))
			})
		})

		Context("Insert processing", func() {
			It("Properly executes insert for a single rows", func() {
				event := &common.ChangeList{}

				newRow1 := common.Row{
					"id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    newRow1,
						Operation: 1,
					},
				}

				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

			})

			It("Properly executed insert for multiple rows", func() {
				event := &common.ChangeList{}

				newRow1 := common.Row{
					"id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}
				newRow2 := common.Row{
					"id": {
						Value: "b",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    newRow1,
						Operation: 1,
					},
					{
						Table:     "kms.api_product",
						NewRow:    newRow2,
						Operation: 1,
					},
				}

				Expect(testDbMan.processChangeList(event)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='b' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(2))
			})

			It("Fails to execute if row does not match existing table schema", func() {
				event := &common.ChangeList{}

				newRow1 := common.Row{
					"not_and_id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    newRow1,
						Operation: 1,
					},
				}

				Expect(testDbMan.processChangeList(event)).ShouldNot(Succeed())

				var nRows int
				//assert that no extraneous rows were added
				err := testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(0))
			})

			It("Fails to execute at least one row does not match the table schema, even if other rows are valid", func() {
				event := &common.ChangeList{}
				newRow1 := common.Row{
					"id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				newRow2 := common.Row{
					"not_and_id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    newRow1,
						Operation: 1,
					},
					{
						Table:     "kms.api_product",
						NewRow:    newRow2,
						Operation: 1,
					},
				}

				Expect(testDbMan.processChangeList(event)).ShouldNot(Succeed())
			})
		})

		Context("Delete processing", func() {

			It("Verify execute single insert & single delete works", func() {
				event1 := &common.ChangeList{}
				event2 := &common.ChangeList{}

				Row1 := common.Row{
					"id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event1.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    Row1,
						Operation: 1,
					},
				}
				event2.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    Row1,
						Operation: 3,
					},
				}

				Expect(testDbMan.processChangeList(event1)).Should(Succeed())
				var nRows int
				err := testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				Expect(testDbMan.processChangeList(event2)).Should(Succeed())

				// validate delete
				err = testDbMan.getDB().QueryRow("select count(*) from kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(0))

				// delete again should fail - coz entry will not exist
				Expect(testDbMan.processChangeList(event2)).ShouldNot(Succeed())
			})

			It("verify multiple insert and single delete works", func() {
				event1 := &common.ChangeList{}
				event2 := &common.ChangeList{}

				Row1 := common.Row{
					"id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				Row2 := common.Row{
					"id": {
						Value: "b",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event1.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    Row1,
						Operation: 1,
					},
					{
						Table:     "kms.api_product",
						NewRow:    Row2,
						Operation: 1,
					},
				}
				event2.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    Row1,
						Operation: 3,
					},
				}

				Expect(testDbMan.processChangeList(event1)).Should(Succeed())
				var nRows int
				//verify first row
				err := testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//verify second row
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='b' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(2))

				Expect(testDbMan.processChangeList(event2)).Should(Succeed())

				//verify second row still exists
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='b' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				// validate delete
				err = testDbMan.getDB().QueryRow("select count(*) from kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				// delete again should fail - coz entry will not exist
				Expect(testDbMan.processChangeList(event2)).ShouldNot(Succeed())
			}, 3)

			It("verify single insert and multiple delete fails", func() {
				event1 := &common.ChangeList{}
				event2 := &common.ChangeList{}

				Row1 := common.Row{
					"id": {
						Value: "a",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				Row2 := common.Row{
					"id": {
						Value: "b",
					},
					"api_resources": {
						Value: "r",
					},
					"environments": {
						Value: "{test}",
					},
					"tenant_id": {
						Value: "t",
					},
					"description": {
						Value: "d",
					},
					"created_at": {
						Value: "c",
					},
					"updated_at": {
						Value: "u",
					},
					"_change_selector": {
						Value: "cs",
					},
				}

				event1.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						NewRow:    Row1,
						Operation: 1,
					},
				}
				event2.Changes = []common.Change{
					{
						Table:     "kms.api_product",
						OldRow:    Row1,
						Operation: 3,
					},
					{
						Table:     "kms.api_product",
						OldRow:    Row2,
						Operation: 3,
					},
				}

				Expect(testDbMan.processChangeList(event1)).Should(Succeed())
				var nRows int
				//verify insert
				err := testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
					"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
					"and _change_selector='cs'").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				//assert that no extraneous rows were added
				err = testDbMan.getDB().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())
				Expect(nRows).To(Equal(1))

				Expect(testDbMan.processChangeList(event2)).ShouldNot(Succeed())

			}, 3)
		})

		Context("ApigeeSync change event", func() {

			Context(LISTENER_TABLE_APID_CLUSTER, func() {

				It("should not change LISTENER_TABLE_APID_CLUSTER", func() {
					event := common.ChangeList{
						LastSequence: "test",
						Changes: []common.Change{
							{
								Operation: common.Insert,
								Table:     LISTENER_TABLE_APID_CLUSTER,
							},
						},
					}
					Expect(testDbMan.processChangeList(&event)).NotTo(Succeed())

					event = common.ChangeList{
						LastSequence: "test",
						Changes: []common.Change{
							{
								Operation: common.Update,
								Table:     LISTENER_TABLE_APID_CLUSTER,
							},
						},
					}

					Expect(testDbMan.processChangeList(&event)).NotTo(Succeed())
				})

			})

			Context("data scopes", func() {

				It("insert event should add", func() {
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

					testDbMan.processChangeList(&event)

					count := 0
					id := sql.NullString{}
					rows, err := testDbMan.getDB().Query(`
				SELECT scope FROM EDGEX_DATA_SCOPE`)
					Expect(err).NotTo(HaveOccurred())
					defer rows.Close()
					for rows.Next() {
						count++
						Expect(rows.Scan(&id)).Should(Succeed())
						Expect(id.String).Should(Equal("s" + strconv.Itoa(count)))
					}

					Expect(count).To(Equal(2))
				})

				It("delete event should delete", func() {
					event := common.ChangeList{
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

					testDbMan.processChangeList(&event)

					event = common.ChangeList{
						LastSequence: "test",
						Changes: []common.Change{
							{
								Operation: common.Delete,
								Table:     LISTENER_TABLE_DATA_SCOPE,
								OldRow:    event.Changes[0].NewRow,
							},
						},
					}

					testDbMan.processChangeList(&event)

					var nRows int
					err := testDbMan.getDB().QueryRow("SELECT count(id) FROM EDGEX_DATA_SCOPE").Scan(&nRows)
					Expect(err).NotTo(HaveOccurred())
					Expect(nRows).To(BeZero())
				})

				It("update event should panic for data scopes table", func() {
					event := common.ChangeList{
						LastSequence: "test",
						Changes: []common.Change{
							{
								Operation: common.Update,
								Table:     LISTENER_TABLE_DATA_SCOPE,
							},
						},
					}

					Expect(testDbMan.processChangeList(&event)).ToNot(Succeed())
				})

			})
		})

	})

	Context("Process Snapshot", func() {
		initTestDb := func(sqlFile string, dbMan *dbManager) common.Snapshot {
			stmts, err := ioutil.ReadFile(sqlFile)
			Expect(err).Should(Succeed())
			Expect(testDbMan.getDB().Exec(string(stmts))).ShouldNot(BeNil())
			Expect(testDbMan.initDB()).Should(Succeed())
			return common.Snapshot{
				SnapshotInfo: dbVersion,
			}
		}

		AfterEach(func() {
			dataService.ReleaseCommonDB()
		})

		It("should fail if more than one apid_cluster rows", func() {
			event := initTestDb("./sql/init_listener_test_duplicate_apids.sql", testDbMan)
			Expect(testDbMan.processSnapshot(&event, true)).ToNot(Succeed())
		})

		It("should process a valid Snapshot", func() {
			config.Set(configApidClusterId, "a")
			apidInfo.ClusterID = "a"
			event := initTestDb("./sql/init_listener_test_valid_snapshot.sql", testDbMan)
			Expect(testDbMan.processSnapshot(&event, true)).Should(Succeed())

			info, err := testDbMan.getApidInstanceInfo()
			Expect(err).Should(Succeed())
			Expect(info.LastSnapshot).To(Equal(event.SnapshotInfo))
			Expect(info.IsNewInstance).To(BeFalse())
			Expect(dataService.DBVersion(event.SnapshotInfo)).Should(Equal(testDbMan.getDB()))

			// apid Cluster
			id := &sql.NullString{}
			Expect(testDbMan.getDB().QueryRow(`SELECT id FROM EDGEX_APID_CLUSTER`).
				Scan(id)).Should(Succeed())
			Expect(id.Valid).Should(BeTrue())
			Expect(id.String).To(Equal("i"))

			// Data Scope
			env := &sql.NullString{}
			count := 0
			rows, err := testDbMan.getDB().Query(`SELECT env FROM EDGEX_DATA_SCOPE`)
			Expect(err).Should(Succeed())
			defer rows.Close()
			for rows.Next() {
				count++
				rows.Scan(&env)
				Expect(env.Valid).Should(BeTrue())
				Expect(env.String).To(Equal("e" + strconv.Itoa(count)))
			}
			Expect(count).To(Equal(3))

			//find scopes for Id
			scopes, err := testDbMan.findScopesForId("a")
			Expect(err).Should(Succeed())
			Expect(len(scopes)).To(Equal(6))
			expectedScopes := []string{"s1", "s2", "org_scope_1", "env_scope_1", "env_scope_2", "env_scope_3"}
			sort.Strings(scopes)
			sort.Strings(expectedScopes)
			Expect(scopes).Should(Equal(expectedScopes))
		})

		It("should detect clusterid change", func() {
			Expect(testDbMan.initDB()).Should(Succeed())
			testDbMan.updateApidInstanceInfo("a", "b", "c")
			config.Set(configApidClusterId, "d")

			info, err := testDbMan.getApidInstanceInfo()
			Expect(err).Should(Succeed())
			Expect(info.LastSnapshot).To(BeZero())
			Expect(info.IsNewInstance).To(BeTrue())
		})
	})

})

func createBootstrapTables(db apid.DB) {
	tx, err := db.Begin()
	Expect(err).To(Succeed())
	//all tests in this file operate on the api_product table.  Create the necessary tables for this here
	_, err = tx.Exec(`CREATE TABLE _transicator_tables
	(tableName varchar not null, columnName varchar not null, typid integer, primaryKey bool);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','id',2950,1);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','tenant_id',1043,1);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','name',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','display_name',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','description',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','api_resources',1015,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','approval_type',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','scopes',1015,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','proxies',1015,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','environments',1015,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','quota',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','quota_time_unit',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','quota_interval',23,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','created_at',1114,1);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','created_by',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','updated_at',1114,1);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','updated_by',1043,0);
	INSERT INTO "_transicator_tables" VALUES('kms_api_product','_change_selector',1043,0);
	CREATE TABLE "kms_api_product" (id text,tenant_id text,name text,display_name text,description text,api_resources text,approval_type text,scopes text,proxies text,environments text,quota text,quota_time_unit text,quota_interval integer,created_at blob,created_by text,updated_at blob,updated_by text,_change_selector text, primary key (id,tenant_id,created_at,updated_at));
	`)
	Expect(err).To(Succeed())
	_, err = tx.Exec(`
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','id',1043,1);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','apid_cluster_id',1043,1);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','scope',25,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','org',25,1);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','env',25,1);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','org_scope',1043,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','env_scope',1043,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','created',1114,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','created_by',25,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','updated',1114,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','updated_by',25,0);
	INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','_change_selector',25,1);
	CREATE TABLE "edgex_data_scope" (id text,apid_cluster_id text,scope text,org text,env text,org_scope text,
	env_scope text,created blob,created_by text,updated blob,updated_by text,_change_selector text,
	primary key (id,apid_cluster_id,apid_cluster_id,org,env,_change_selector));
	`)
	Expect(err).To(Succeed())

	Expect(tx.Commit()).To(Succeed())
}
