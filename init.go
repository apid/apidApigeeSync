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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/30x/apid-core"
	"strings"
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
	configOnlineMode          = "apigeesync_online"
	// special value - set by ApigeeSync, not taken from configuration
	configApidInstanceID = "apigeesync_apid_instance_id"
	// This will not be needed once we have plugin handling tokens.
	configBearerToken = "apigeesync_bearer_token"
)

const (
	ApigeeSyncEventSelector = "ApigeeSync"
)

const (
	offlineMode = "offline"
)

var (
	/* All set during plugin initialization */
	log                 apid.LogService
	config              apid.ConfigService
	dataService         apid.DataService
	events              apid.EventsService
	apidInfo            apidInstanceInfo
	newInstanceID       bool
	apidTokenManager    tokenManager
	apidChangeManager   changeManager
	apidSnapshotManager snapShotManager
	httpclient          *http.Client
	isOfflineMode       bool

	/* Set during post plugin initialization
	 * set this as a default, so that it's guaranteed to be valid even if postInitPlugins isn't called
	 */
	apidPluginDetails string = `[{"name":"apidApigeeSync","schemaVer":"1.0"}]`
)

type apidInstanceInfo struct {
	InstanceID, InstanceName, ClusterID, LastSnapshot string
}

type pluginDetail struct {
	Name          string `json:"name"`
	SchemaVersion string `json:"schemaVer"`
}

func init() {
	apid.RegisterPlugin(initPlugin)
}

func initConfigDefaults() {
	config.SetDefault(configPollInterval, 120*time.Second)
	config.SetDefault(configSnapshotProtocol, "sqlite")
	config.SetDefault(configOnlineMode, "online")
	name, errh := os.Hostname()
	if (errh != nil) && (len(config.GetString(configName)) == 0) {
		log.Errorf("Not able to get hostname for kernel. Please set '%s' property in config", configName)
		name = "Undefined"
	}
	config.SetDefault(configName, name)
	log.Debugf("Using %s as display name", config.GetString(configName))
}

func initVariables() error {

	tr := &http.Transport{
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
	}
	httpclient = &http.Client{
		Transport: tr,
		Timeout:   httpTimeout,
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			req.Header.Set("Authorization", "Bearer "+apidTokenManager.getBearerToken())
			return nil
		},
	}

	// set up default database
	db, err := dataService.DB()
	if err != nil {
		return fmt.Errorf("Unable to access DB: %v", err)
	}
	err = initDB(db)
	if err != nil {
		return fmt.Errorf("Unable to access DB: %v", err)
	}
	setDB(db)

	apidInfo, err = getApidInstanceInfo()
	if err != nil {
		return fmt.Errorf("Unable to get apid instance info: %v", err)
	}

	if config.IsSet(configApidInstanceID) {
		log.Warnf("ApigeeSync plugin overriding %s.", configApidInstanceID)
	}
	config.Set(configApidInstanceID, apidInfo.InstanceID)

	return nil
}

func createManagers() {
	if isOfflineMode {
		apidSnapshotManager = &offlineSnapshotManager{}
		apidChangeManager = &offlineChangeManager{}
	} else {
		apidSnapshotManager = createSnapShotManager()
		apidChangeManager = createChangeManager()
	}

	apidTokenManager = createSimpleTokenManager()
}

func checkForRequiredValues() error {
	required := []string{configProxyServerBaseURI, configConsumerKey, configConsumerSecret}
	if !isOfflineMode {
		required = append(required, configSnapServerBaseURI, configChangeServerBaseURI)
	}
	// check for required values
	for _, key := range required {
		if !config.IsSet(key) {
			return fmt.Errorf("Missing required config value: %s", key)
		}
	}
	proto := config.GetString(configSnapshotProtocol)
	if proto != "sqlite" {
		return fmt.Errorf("Illegal value for %s. Only currently supported snashot protocol is sqlite", configSnapshotProtocol)
	}

	return nil
}

func SetLogger(logger apid.LogService) {
	log = logger
}

/* initialization */
func _initPlugin(services apid.Services) error {
	log.Debug("start init")

	config = services.Config()
	initConfigDefaults()

	if strings.EqualFold(config.GetString(configOnlineMode), offlineMode) {
		log.Warn("offline mode!")
		isOfflineMode = true
	}

	err := checkForRequiredValues()
	if err != nil {
		return err
	}

	err = initVariables()
	if err != nil {
		return err
	}

	return nil
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	SetLogger(services.Log().ForModule("apigeeSync"))
	dataService = services.Data()
	events = services.Events()

	err := _initPlugin(services)
	if err != nil {
		return pluginData, err
	}

	createManagers()

	/* This callback function will get called once all the plugins are
	 * initialized (not just this plugin). This is needed because,
	 * downloadSnapshots/changes etc have to begin to be processed only
	 * after all the plugins are initialized
	 */
	events.ListenOnceFunc(apid.SystemEventsSelector, postInitPlugins)

	InitAPI(services)
	log.Debug("end init")

	return pluginData, nil
}

// Plugins have all initialized, gather their info and start the ApigeeSync downloads
func postInitPlugins(event apid.Event) {
	var plinfoDetails []pluginDetail
	if pie, ok := event.(apid.PluginsInitializedEvent); ok {
		/*
		 * Store the plugin details in the heap. Needed during
		 * Bearer token generation request.
		 */
		for _, plugin := range pie.Plugins {
			name := plugin.Name
			version := plugin.Version
			if schemaVersion, ok := plugin.ExtraData["schemaVersion"].(string); ok {
				inf := pluginDetail{
					Name:          name,
					SchemaVersion: schemaVersion}
				plinfoDetails = append(plinfoDetails, inf)
				log.Debugf("plugin %s is version %s, schemaVersion: %s", name, version, schemaVersion)
			}
		}
		if plinfoDetails == nil {
			log.Panicf("No Plugins registered!")
		}

		pgInfo, err := json.Marshal(plinfoDetails)
		if err != nil {
			log.Panicf("Unable to marshal plugin data: %v", err)
		}
		apidPluginDetails = string(pgInfo[:])

		log.Debug("start post plugin init")

		apidTokenManager.start()
		go bootstrap()

		log.Debug("Done post plugin init")
	}
}
