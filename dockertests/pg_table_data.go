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