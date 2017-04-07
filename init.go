package apidApigeeSync

import (
	"encoding/json"
	"fmt"
	"os"

	"time"

	"github.com/30x/apid-core"
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
	// This will not be needed once we have plugin handling tokens.
	configBearerToken = "apigeesync_bearer_token"
)

var (
	/* All set during plugin initialization */
	log           apid.LogService
	config        apid.ConfigService
	dataService   apid.DataService
	events        apid.EventsService
	apidInfo      apidInstanceInfo
	newInstanceID bool
	tokenManager  *tokenMan
	changeManager *pollChangeManager
	snapManager   *snapShotManager

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
	config.SetDefault(configSnapshotProtocol, "json")
	name, errh := os.Hostname()
	if (errh != nil) && (len(config.GetString(configName)) == 0) {
		log.Errorf("Not able to get hostname for kernel. Please set '%s' property in config", configName)
		name = "Undefined"
	}
	config.SetDefault(configName, name)
	log.Debugf("Using %s as display name", config.GetString(configName))
}

func initVariables(services apid.Services) error {
	dataService = services.Data()
	events = services.Events()
	//TODO listen for arbitrary commands, these channels can be used to kill polling goroutines
	//also useful for testing
	snapManager = createSnapShotManager()
	changeManager = createChangeManager()

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

func checkForRequiredValues() error {
	// check for required values
	for _, key := range []string{configProxyServerBaseURI, configConsumerKey, configConsumerSecret,
		configSnapServerBaseURI, configChangeServerBaseURI} {
		if !config.IsSet(key) {
			return fmt.Errorf("Missing required config value: %s", key)
		}
	}
	proto := config.GetString(configSnapshotProtocol)
	if proto != "json" && proto != "proto" {
		return fmt.Errorf("Illegal value for %s. Must be: 'json' or 'proto'", configSnapshotProtocol)
	}

	return nil
}

func SetLogger(logger apid.LogService) {
	log = logger
}

/* Idempotent state initialization */
func _initPlugin(services apid.Services) error {
	SetLogger(services.Log().ForModule("apigeeSync"))
	log.Debug("start init")

	config = services.Config()
	initConfigDefaults()

	err := checkForRequiredValues()
	if err != nil {
		return err
	}

	err = initVariables(services)
	if err != nil {
		return err
	}

	return nil
}

func initPlugin(services apid.Services) (apid.PluginData, error) {

	err := _initPlugin(services)
	if err != nil {
		return pluginData, err
	}

	/* This callback function will get called once all the plugins are
	 * initialized (not just this plugin). This is needed because,
	 * downloadSnapshots/changes etc have to begin to be processed only
	 * after all the plugins are initialized
	 */
	events.ListenOnceFunc(apid.SystemEventsSelector, postInitPlugins)

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

		tokenManager = createTokenManager()

		go bootstrap()

		events.Listen(ApigeeSyncEventSelector, &handler{})
		log.Debug("Done post plugin init")
	}
}
