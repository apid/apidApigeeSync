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
	"github.com/30x/apid-core/data"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

var _ = Describe("DB manager tests", func() {
	var testDbMan *dbManager
	var testCount int
	BeforeEach(func() {
		testDbMan = &dbManager{
			dbMux: sync.RWMutex{},
			data:  dataService,
		}
		testCount += 1

		testDbMan.setDbVersion("data_test_" + strconv.Itoa(testCount))
	})

	var _ = AfterEach(func() {
		testDbMan = nil
		data.Delete(data.VersionedDBID("common", "data_test_"+strconv.Itoa(testCount)))
	})

	Context("Basic Update/Insert/Delete processing", func() {
		BeforeEach(func() {
			db := testDbMan.getDb()

			//all tests in this file operate on the api_product table.  Create the necessary tables for this here
			db.Exec("CREATE TABLE _transicator_tables " +
				"(tableName varchar not null, columnName varchar not null, " +
				"typid integer, primaryKey bool);")
			db.Exec("DELETE from _transicator_tables")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','id',2950,1)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','tenant_id',1043,1)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','description',1043,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','api_resources',1015,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','approval_type',1043,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','scopes',1015,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','proxies',1015,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','environments',1015,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','created_at',1114,1)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','created_by',1043,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','updated_at',1114,1)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','updated_by',1043,0)")
			db.Exec("INSERT INTO _transicator_tables VALUES('kms_api_product','_change_selector',1043,0)")

			db.Exec("CREATE TABLE kms_api_product (id text,tenant_id text,name text, description text, " +
				"api_resources text,approval_type text,scopes text,proxies text, environments text," +
				"created_at blob, created_by text,updated_at blob,updated_by text,_change_selector text, " +
				"primary key (id,tenant_id,created_at,updated_at));")
			db.Exec("DELETE from kms_api_product")

			testDbMan.initDefaultDb()
		})

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

			result := buildUpdateSql("TEST_TABLE", orderedColumns, testRow, []string{"id"})
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

			result := buildUpdateSql("TEST_TABLE", orderedColumns, testRow, []string{"id1", "id2"})
			Expect("UPDATE TEST_TABLE SET _change_selector=$1, api_resources=$2, environments=$3, id1=$4, id2=$5, tenant_id=$6" +
				" WHERE id1=$7 AND id2=$8").To(Equal(result))
		})

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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			err = testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='new description'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			err = testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='new_id' and description='new description'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			err = testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='new description'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='A product for testing Greg'").Scan(&nRows)
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
			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			err = testDbMan.getDb().QueryRow("SELECT count(id) FROM kms_api_product WHERE id='87a4bfaa-b3c4-47cd-b6c5-378cdb68610c' and description='new description'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))
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
			Expect(expectedSql).To(Equal(buildInsertSql("api_product", orderedColumns, []common.Row{newRow})))
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
			Expect(expectedSql).To(Equal(buildInsertSql("api_product", orderedColumns, []common.Row{newRow1, newRow2})))
		})

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

			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
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

			Expect(true).To(Equal(testDbMan.writeTransaction(event)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='b' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
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

			ok := testDbMan.writeTransaction(event)
			Expect(false).To(Equal(ok))

			var nRows int
			//assert that no extraneous rows were added
			err := testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
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

			ok := testDbMan.writeTransaction(event)
			Expect(false).To(Equal(ok))
		})

		It("Properly constructs sql prepare for Delete", func() {
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
			sql := buildDeleteSql("kms_api_product", row, pkeys)
			Expect(sql).To(Equal("DELETE FROM kms_api_product WHERE created_at=$1 AND id=$2 AND tenant_id=$3 AND updated_at=$4"))
		})

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

			Expect(true).To(Equal(testDbMan.writeTransaction(event1)))
			var nRows int
			err := testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			Expect(true).To(Equal(testDbMan.writeTransaction(event2)))

			// validate delete
			err = testDbMan.getDb().QueryRow("select count(*) from kms_api_product").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(0))

			// delete again should fail - coz entry will not exist
			Expect(false).To(Equal(testDbMan.writeTransaction(event2)))
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

			Expect(true).To(Equal(testDbMan.writeTransaction(event1)))
			var nRows int
			//verify first row
			err := testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//verify second row
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='b' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(2))

			Expect(true).To(Equal(testDbMan.writeTransaction(event2)))

			//verify second row still exists
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='b' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			// validate delete
			err = testDbMan.getDb().QueryRow("select count(*) from kms_api_product").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			// delete again should fail - coz entry will not exist
			Expect(false).To(Equal(testDbMan.writeTransaction(event2)))
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

			Expect(true).To(Equal(testDbMan.writeTransaction(event1)))
			var nRows int
			//verify insert
			err := testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product WHERE id='a' and api_resources='r'" +
				"and environments='{test}' and tenant_id='t' and description='d' and created_at='c' and updated_at='u'" +
				"and _change_selector='cs'").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			//assert that no extraneous rows were added
			err = testDbMan.getDb().QueryRow("SELECT count(*) FROM kms_api_product").Scan(&nRows)
			Expect(err).NotTo(HaveOccurred())
			Expect(nRows).To(Equal(1))

			Expect(false).To(Equal(testDbMan.writeTransaction(event2)))

		}, 3)
	})

	Context("ApigeeSync methods", func() {
		var createTestDb = func(sqlfile string) {

			db := testDbMan.getDb()
			sqlStatementsBuffer, err := ioutil.ReadFile(sqlfile)
			Expect(err).Should(Succeed())
			sqlStatementsString := string(sqlStatementsBuffer)
			_, err = db.Exec(sqlStatementsString)
			Expect(err).Should(Succeed())
		}

		It("test getClusterCount", func() {
			createTestDb("./sql/init_listener_test_duplicate_apids.sql")
			Expect(testDbMan.getClusterCount()).To(Equal(2))
		})

		It("should process a valid Snapshot", func() {

			createTestDb("./sql/init_listener_test_valid_snapshot.sql")

			dbVersion := "data_test_" + strconv.Itoa(testCount)
			err := testDbMan.updateApidInstanceInfo()
			Expect(err).Should(Succeed())

			info, err := testDbMan.getApidInstanceInfo()
			Expect(err).Should(Succeed())
			Expect(info.LastSnapshot).To(Equal(dbVersion))

			db := testDbMan.getDb()

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

			scopes := testDbMan.findScopesForId("a")
			Expect(len(scopes)).To(Equal(6))
			expectedScopes := []string{"s1", "s2", "org_scope_1", "env_scope_1", "env_scope_2", "env_scope_3"}
			sort.Strings(scopes)
			sort.Strings(expectedScopes)
			Expect(reflect.DeepEqual(scopes, expectedScopes)).To(BeTrue())
		}, 3)

		It("insert event should add", func() {
			createTestDb("./sql/init_listener_test_no_datascopes.sql", "test_changes_insert")
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

			testDbMan.writeTransaction(&event)

			var dds []dataDataScope

			rows, err := testDbMan.getDb().Query(`
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

			scopes := testDbMan.findScopesForId("a")
			Expect(len(scopes)).To(Equal(2))
			Expect(scopes[0]).To(Equal("s1"))
			Expect(scopes[1]).To(Equal("s2"))

		}, 3)
	})
})
