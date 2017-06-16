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

const (
	pluginName               = "apigeeSyncDockerTest"
	configApidClusterId      = "apigeesync_cluster_id"
	configProxyServerBaseURI = "apigeesync_proxy_server_base"
	configLocalStoragePath   = "local_storage_path"
	configConsumerKey        = "apigeesync_consumer_key"
	configConsumerSecret     = "apigeesync_consumer_secret"
	configName               = "apigeesync_instance_name"
	ApigeeSyncEventSelector  = "ApigeeSync"
	testInitUser             = "dockerTestInit"
	basicTables              = map[string]bool{
		"deployment_history": true,
		"deployment":         true,
		"bundle_config":      true,
		"configuration":      true,
		"apid_cluster":       true,
		"data_scope":         true,
	}
)
