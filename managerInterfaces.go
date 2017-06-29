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
	"net/url"
)

type tokenManager interface {
	getBearerToken() string
	invalidateToken() error
	getToken() *OauthToken
	close()
	getRetrieveNewTokenClosure(*url.URL) func(chan bool) error
	start()
	getTokenReadyChannel() chan bool
}

type snapShotManager interface {
	close() <-chan bool
	downloadBootSnapshot()
	storeBootSnapshot(snapshot *common.Snapshot)
	downloadDataSnapshot()
	storeDataSnapshot(snapshot *common.Snapshot)
	downloadSnapshot(isBoot bool, scopes []string, snapshot *common.Snapshot) error
}

type changeManager interface {
	close() <-chan bool
	pollChangeWithBackoff()
}
