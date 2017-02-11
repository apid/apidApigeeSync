package apidApigeeSync

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/30x/apid"
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
	ApigeeSyncEventSelector   = "ApigeeSync"

	// special value - set by ApigeeSync, not taken from configuration
	configApidInstanceID = "apigeesync_apid_instance_id"
	// This will not be needed once we have plugin
	// handling tokens.
	bearerToken = "apigeesync_bearer_token"
)

var (
	log               apid.LogService
	config            apid.ConfigService
	data              apid.DataService
	events            apid.EventsService
	apidInfo          apidInstanceInfo
	apidPluginDetails string
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

func initDefaults() {
	config.SetDefault(configPollInterval, 120)
	config.SetDefault(configSnapshotProtocol, "json")
	name, errh := os.Hostname()
	if (errh != nil) && (len(config.GetString(configName)) == 0) {
		log.Errorf("Not able to get hostname for kernel. Please set '%s' property in config", configName)
		name = "Undefined"
	}
	config.SetDefault(configName, name)
	log.Debugf("Using %s as display name", config.GetString(configName))
}

func SetLogger(logger apid.LogService) {
	log = logger
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	SetLogger(services.Log().ForModule("apigeeSync"))
	log.Debug("start init")

	config = services.Config()
	initDefaults()

	data = services.Data()
	events = services.Events()

	/* This callback function will get called, once all the plugins are
	 * initialized (not just this plugin). This is needed because,
	 * downloadSnapshots/changes etc have to begin to be processed only
	 * after all the plugins are initialized
	 */
	events.ListenFunc(apid.SystemEventsSelector, postInitPlugins)

	// This callback function will get called after each data event delivery.
	events.ListenFunc(apid.EventDeliveredSelector, postPluginDataDelivery)

	// check for required values
	for _, key := range []string{configProxyServerBaseURI, configConsumerKey, configConsumerSecret,
		configSnapServerBaseURI, configChangeServerBaseURI} {
		if !config.IsSet(key) {
			return pluginData, fmt.Errorf("Missing required config value: %s", key)
		}
	}
	proto := config.GetString(configSnapshotProtocol)
	if proto != "json" && proto != "proto" {
		return pluginData, fmt.Errorf("Illegal value for %s. Must be: 'json' or 'proto'", configSnapshotProtocol)
	}

	// set up default database
	db, err := data.DB()
	if err != nil {
		return pluginData, fmt.Errorf("Unable to access DB: %v", err)
	}
	err = initDB(db)
	if err != nil {
		return pluginData, fmt.Errorf("Unable to access DB: %v", err)
	}
	setDB(db)

	apidInfo, err = getApidInstanceInfo()
	if err != nil {
		return pluginData, fmt.Errorf("Unable to get apid instance info: %v", err)
	}

	if config.IsSet(configApidInstanceID) {
		log.Warnf("ApigeeSync plugin overriding %s.", configApidInstanceID)
	}
	config.Set(configApidInstanceID, apidInfo.InstanceID)

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

		go bootstrap()

		events.Listen(ApigeeSyncEventSelector, &handler{})
		log.Debug("Done post plugin init")
	}
}
