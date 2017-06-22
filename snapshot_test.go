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
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	"os"
	"strings"
)

var _ = Describe("Change Agent", func() {

	const testDbId = "test_snapshot"

	Context("Change Agent Unit Tests", func() {
		testHandler := handler{}

		var createTestDb = func(sqlfile string, dbId string) common.Snapshot {
			initDb(sqlfile, "./mockdb_snapshot.sqlite3")
			file, err := os.Open("./mockdb_snapshot.sqlite3")
			if err != nil {
				Fail("Failed to open mock db for test")
			}

			s := common.Snapshot{}
			err = processSnapshotServerFileResponse(dbId, file, &s)
			if err != nil {
				Fail("Error processing test snapshots")
			}
			return s
		}

		BeforeEach(func() {
			event := createTestDb("./sql/init_mock_db.sql", testDbId)
			testHandler.Handle(&event)
			knownTables = extractTablesFromDB(getDB())
		})

		It("test extract table columns", func() {
			s := &common.Snapshot{
				SnapshotInfo: testDbId,
			}
			columns := extractTableColumnsFromSnapshot(s)
			for table, cols := range columns {
				log.Error("snapshot TABLE: " + table + " COLUMN: " + strings.Join(cols, "|"))
			}
		})

	})
})