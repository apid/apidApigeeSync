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
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/apid/apid-core"
	"github.com/apid/apid-core/util"
)

const (
	configPollInterval        = "apigeesync_poll_interval"
	configProxyServerBaseURI  = "apigeesync_proxy_server_base"
	configSnapServerBaseURI   = "apigeesync_snapshot_server_base"
	configChangeServerBaseURI = "apigeesync_change_server_base"
	configConsumerKey         = "apigeesync_consumer_key"
	configConsumerSecret      = "apigeesync_consumer_secret"
	configApidClusterId       = "apigeesync_cluster_id"
	configSnapshotProtocol    = "apigeesync_snapshot_proto"
	configName                = "apigeesync_instance_name"
	configDiagnosticMode      = "apigeesync_diagnostic_mode"
	// special value - set by ApigeeSync, not taken from configuration
	configApidInstanceID = "apigeesync_apid_instance_id"
	// This will not be needed once we have plugin handling tokens.
	configBearerToken      = "apigeesync_bearer_token"
	configLocalStoragePath = "local_storage_path"
)

const (
	ApigeeSyncEventSelector = "ApigeeSync"
)

var (
	/* All set during plugin initialization */
	log          apid.LogService
	config       apid.ConfigService
	dataService  apid.DataService
	eventService apid.EventsService
	apiService   apid.APIService
	apidInfo     apidInstanceInfo

	/* Set during post plugin initialization
	 * set this as a default, so that it's guaranteed to be valid even if postInitPlugins isn't called
	 */
	apidPluginDetails string = `[{"name":"apidApigeeSync","schemaVer":"1.0"}]`
)

type apidInstanceInfo struct {
	InstanceID, InstanceName, ClusterID, LastSnapshot string
	IsNewInstance                                     bool
}

type pluginDetail struct {
	Name          string `json:"name"`
	SchemaVersion string `json:"schemaVer"`
}

func init() {
	apid.RegisterPlugin(initPlugin, pluginData)
}

func initConfigDefaults() {
	config.SetDefault(configPollInterval, 120*time.Second)
	config.SetDefault(configSnapshotProtocol, "sqlite")
	config.SetDefault(configDiagnosticMode, false)

	name, errh := os.Hostname()
	if (errh != nil) && (len(config.GetString(configName)) == 0) {
		log.Errorf("Not able to get hostname for kernel. Please set '%s' property in config", configName)
		name = "Undefined"
	}
	config.SetDefault(configName, name)
	log.Debugf("Using %s as display name", config.GetString(configName))
}

func checkForRequiredValues(isOfflineMode bool) error {
	required := []string{configProxyServerBaseURI, configConsumerKey, configConsumerSecret}
	if !isOfflineMode {
		required = append(required, configSnapServerBaseURI, configChangeServerBaseURI)
	}
	// check for required values
	for _, key := range required {
		if !config.IsSet(key) {
			return fmt.Errorf("missing required config value: %s", key)
		}
	}
	proto := config.GetString(configSnapshotProtocol)
	if proto != "sqlite" {
		return fmt.Errorf("illegal value for %s. Only currently supported snashot protocol is sqlite", configSnapshotProtocol)
	}

	return nil
}

func SetLogger(logger apid.LogService) {
	log = logger
}

/* initialization */
func initConfigs(services apid.Services) error {

	return nil
}

func initManagers(isOfflineMode bool) (*listenerManager, *ApiManager, error) {
	// check for forward proxy
	var tr *http.Transport
	tr = util.Transport(config.GetString(util.ConfigfwdProxyPortURL))
	tr.MaxIdleConnsPerHost = maxIdleConnsPerHost

	apidDbManager := creatDbManager()
	db, err := dataService.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to access DB: %v", err)
	}
	apidDbManager.setDB(db)
	err = apidDbManager.initDB()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to access DB: %v", err)
	}

	apidInfo, err = apidDbManager.getApidInstanceInfo()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get apid instance info: %v", err)
	}

	if config.IsSet(configApidInstanceID) {
		log.Warnf("ApigeeSync plugin overriding %s.", configApidInstanceID)
	}
	config.Set(configApidInstanceID, apidInfo.InstanceID)

	apidTokenManager := createApidTokenManager(apidInfo.IsNewInstance)
	var snapMan snapshotManager
	var apidChangeManager changeManager

	if isOfflineMode {
		snapMan = &offlineSnapshotManager{
			dbMan: apidDbManager,
		}
		apidChangeManager = &offlineChangeManager{}
	} else {
		httpClient := &http.Client{
			Transport: tr,
			Timeout:   httpTimeout,
			CheckRedirect: func(req *http.Request, _ []*http.Request) error {
				req.Header.Set("Authorization", "Bearer "+apidTokenManager.getBearerToken())
				return nil
			},
		}
		snapMan = createSnapShotManager(apidDbManager, apidTokenManager, httpClient)
		apidChangeManager = createChangeManager(apidDbManager, snapMan, apidTokenManager, httpClient)
	}

	listenerMan := &listenerManager{
		changeMan:     apidChangeManager,
		snapMan:       snapMan,
		tokenMan:      apidTokenManager,
		isOfflineMode: isOfflineMode,
	}

	apiMan := &ApiManager{
		endpoint: tokenEndpoint,
		tokenMan: apidTokenManager,
	}
	return listenerMan, apiMan, nil
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	SetLogger(services.Log().ForModule("apigeeSync"))
	dataService = services.Data()
	eventService = services.Events()
	apiService = services.API()
	log.Debug("start init")
	config = services.Config()
	initConfigDefaults()

	isOfflineMode := false
	if config.GetBool(configDiagnosticMode) {
		log.Warn("Diagnostic mode: will not download changelist and snapshots!")
		isOfflineMode = true
	}

	err := checkForRequiredValues(isOfflineMode)
	if err != nil {
		return pluginData, err
	}
	if err != nil {
		return pluginData, err
	}
	listenerMan, apiMan, err := initManagers(isOfflineMode)
	if err != nil {
		return pluginData, err
	}
	listenerMan.init()
	apiMan.InitAPI(apiService)

	log.Debug("end init")
	return pluginData, nil
}
