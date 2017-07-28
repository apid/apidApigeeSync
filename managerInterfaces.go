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
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	"net/url"
)

type TokenManagerInterface interface {
	getBearerToken() string
	invalidateToken() error
	getToken() *OauthToken
	close()
	getRetrieveNewTokenClosure(*url.URL) func(chan bool) error
	start()
}

type snapShotManagerInterface interface {
	close() <-chan bool
	downloadBootSnapshot()
	storeBootSnapshot(snapshot *common.Snapshot)
	downloadDataSnapshot()
	storeDataSnapshot(snapshot *common.Snapshot)
	downloadSnapshot(isBoot bool, scopes []string, snapshot *common.Snapshot) error
}

type changeManagerInterface interface {
	close() <-chan bool
	pollChangeWithBackoff()
}

type dbManagerInterface interface {
	initDefaultDb() error
	setDb(apid.DB)
	getDb() apid.DB
	insert(tableName string, rows []common.Row, txn *sql.Tx) bool
	deleteRowsFromTable(tableName string, rows []common.Row, txn *sql.Tx) bool
	update(tableName string, oldRows, newRows []common.Row, txn *sql.Tx) bool
	getPkeysForTable(tableName string) ([]string, error)
	findScopesForId(configId string) (scopes []string)
	getDefaultDb() (apid.DB, error)
	updateApidInstanceInfo() error
	getApidInstanceInfo() (info apidInstanceInfo, err error)
	getLastSequence() string
	updateLastSequence(lastSequence string) error
	getClusterCount() (numApidClusters int, err error)
	alterClusterTable() (err error)
	writeTransaction(*common.ChangeList) bool
}

type listenerManagerInterface interface {
	processSnapshot(snapshot *common.Snapshot)
	processSqliteSnapshot(db apid.DB)
	processChangeList(changes *common.ChangeList) bool
}
