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
	"github.com/apid/apid-core"
	"github.com/apigee-labs/transicator/common"
)

type tokenManager interface {
	getBearerToken() string
	invalidateToken() error
	close()
	start()
	getTokenReadyChannel() <-chan bool
}

type snapShotManager interface {
	close() <-chan bool
	downloadBootSnapshot()
	downloadDataSnapshot() error
	startOnDataSnapshot(snapshot string) error
}

type changeManager interface {
	close() <-chan bool
	pollChangeWithBackoff()
}

type DbManager interface {
	initDB() error
	setDB(db apid.DB)
	getLastSequence() (lastSequence string)
	findScopesForId(configId string) (scopes []string, err error)
	updateLastSequence(lastSequence string) error
	getApidInstanceInfo() (info apidInstanceInfo, err error)
	processChangeList(changes *common.ChangeList) error
	processSnapshot(snapshot *common.Snapshot, isDataSnapshot bool) error
	getKnowTables() map[string]bool
}
