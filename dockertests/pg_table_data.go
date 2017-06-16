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

package dockertests

import "time"

type apidClusterRow struct {
	id             string
	name           string
	description    string
	appName        string
	created        time.Time
	createdBy      string
	updated        time.Time
	updatedBy      string
	changeSelector string
}

/* FOREIGN KEY (apid_cluster_id)
 * REFERENCES apid_cluster(id) ON DELETE CASCADE
 */
type dataScopeRow struct {
	id             string
	clusterId      string
	scope          string
	org            string
	env            string
	created        time.Time
	createdBy      string
	updated        time.Time
	updatedBy      string
	changeSelector string
}

/* FOREIGN KEY (data_scope_id)
 * REFERENCES data_scope(id) ON DELETE CASCADE
 */
type bundleConfigRow struct {
	id           string
	scopeId      string
	name         string
	uri          string
	checksumType string
	checksum     string
	created      time.Time
	createdBy    string
	updated      time.Time
	updatedBy    string
}

/* FOREIGN KEY (bundle_config_id)
 * REFERENCES bundle_config(id) ON DELETE CASCADE
 */
type deploymentRow struct {
	id               string
	configId         string
	clusterId        string
	scopeId          string
	bundleConfigName string
	bundleConfigJson string
	configJson       string
	created          time.Time
	createdBy        string
	updated          time.Time
	updatedBy        string
	changeSelector   string
}


type bundleConfigData struct {
	Id string `json:"id"`
	Created string `json:"created"`
	CreatedBy string `json:"createdBy"`
	Updated string `json:"updated"`
	UpdatedBy string `json:"updatedBy"`
	Name string `json:"name"`
	Uri string `json:"uri"`
}