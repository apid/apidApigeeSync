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
	"github.com/30x/apid-core"
	"net/http"
	"time"
)

const (
	httpTimeout         = time.Minute
	pluginTimeout       = time.Minute
	maxIdleConnsPerHost = 10
)

var knownTables = make(map[string]map[string]bool)

/*
 *  Start from existing snapshot if possible
 *  If an existing snapshot does not exist, use the apid scope to fetch
 *  all data scopes, then get a snapshot for those data scopes
 *
 *  Then, poll for changes
 */
func bootstrap() {

	if apidInfo.LastSnapshot != "" {
		snapshot := startOnLocalSnapshot(apidInfo.LastSnapshot)
		processSnapshot(snapshot)
		events.EmitWithCallback(ApigeeSyncEventSelector, snapshot, func(event apid.Event) {
			apidChangeManager.pollChangeWithBackoff()
		})

		log.Infof("Started on local snapshot: %s", snapshot.SnapshotInfo)
		return
	}

	apidSnapshotManager.downloadBootSnapshot()
	apidSnapshotManager.downloadDataSnapshot()

	apidChangeManager.pollChangeWithBackoff()

}

/*
 * Call toExecute repeatedly until it does not return an error, with an exponential backoff policy
 * for retrying on errors
 */
func pollWithBackoff(quit chan bool, toExecute func(chan bool) error, handleError func(error)) {

	backoff := NewExponentialBackoff(200*time.Millisecond, config.GetDuration(configPollInterval), 2, true)

	//inintialize the retry channel to start first attempt immediately
	retry := time.After(0 * time.Millisecond)

	for {
		select {
		case <-quit:
			log.Info("Quit signal recieved.  Returning")
			return
		case <-retry:
			start := time.Now()

			err := toExecute(quit)
			if err == nil {
				return
			}

			if _, ok := err.(quitSignalError); ok {
				return
			}

			end := time.Now()
			//error encountered, since we would have returned above otherwise
			handleError(err)

			/* TODO keep this around? Imagine an immediately erroring service,
			 *  causing many sequential requests which could pollute logs
			 */
			//only backoff if the request took less than one second
			if end.After(start.Add(time.Second)) {
				backoff.Reset()
				retry = time.After(0 * time.Millisecond)
			} else {
				retry = time.After(backoff.Duration())
			}
		}
	}
}

func addHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+apidTokenManager.getBearerToken())
	req.Header.Set("apid_instance_id", apidInfo.InstanceID)
	req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
	req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
}

type changeServerError struct {
	Code string `json:"code"`
}

type quitSignalError struct {
}

type expected200Error struct {
}

type authFailError struct {
}

func (an expected200Error) Error() string {
	return "Did not recieve OK response"
}

func (a quitSignalError) Error() string {
	return "Signal to quit encountered"
}

func (a changeServerError) Error() string {
	return a.Code
}

func (a authFailError) Error() string {
	return "Authorization failed"
}
