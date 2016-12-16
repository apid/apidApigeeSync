package apidApigeeSync

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/30x/apid"
	"time"
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
)

var (
	log           apid.LogService
	config        apid.ConfigService
	data          apid.DataService
	events        apid.EventsService
	gapidConfigId string
	guuid         string
	ginstName     string
	gpgInfo       string
)

type pluginDetail struct {
	Name          string `json:"name"`
	SchemaVersion string `json:"schemaVer"`
}

/*
 * generates a random uuid (mix of timestamp & crypto random string)
 */
func generate_uuid() string {
	unix32bits := uint32(time.Now().UTC().Unix())
	buff := make([]byte, 12)
	numRead, err := rand.Read(buff)
	if numRead != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}

func init() {
	apid.RegisterPlugin(initPlugin)
}

func postInitPlugins(event apid.Event) {
	var plinfoDetails []pluginDetail
	if pie, ok := event.(apid.PluginsInitializedEvent); ok {

		/*
		 * Store the plugin details in the heap. Needed during
		 * Bearer token generation request
		 */
		for _, plugin := range pie.Plugins {
			name := plugin.Name
			version := plugin.Version
			log.Debugf("plugin %s is version %s, schemaVersion: %s", name, version)
			if schemaVersion, ok := plugin.ExtraData["schemaVersion"].(string); ok {
				inf := pluginDetail{
					Name:          name,
					SchemaVersion: schemaVersion}
				plinfoDetails = append(plinfoDetails, inf)
			}
		}
		if plinfoDetails == nil {
			log.Panicf("No Plugins registered!")
		} else {
			pgInfo, err := json.Marshal(plinfoDetails)
			if err != nil {
				log.Panic("Unable to masrhal plugin data", err)
			}
			gpgInfo = (string(pgInfo[:]))
		}

		log.Debug("start post plugin init")
		/* call to Download Snapshot info */
		go DownloadSnapshots()

		/* Begin Looking for changes periodically */
		log.Debug("starting update goroutine")
		go updatePeriodicChanges()

		events.Listen(ApigeeSyncEventSelector, &handler{})
		log.Debug("Done post plugin init")
	}
}

func initPlugin(services apid.Services) (apid.PluginData, error) {
	log = services.Log().ForModule("apigeeSync")
	log.Debug("start init")

	config = services.Config()
	data = services.Data()
	events = services.Events()
	guuid = findapidConfigInfo("instance_id")
	if guuid == "" {
		guuid = generate_uuid()
	}

	/* If The Instance has no name configured, just re-use UUID */
	ginstName = config.GetString(configName)
	if ginstName == "" {
		ginstName = guuid
	}

	/* This callback function will get called, once all the plugins are
	 * initialized (not just this plugin). This is needed because,
	 * DownloadSnapshots/Changes etc have to begin to be processed only
	 * after all the plugins are initialized
	 */
	events.ListenFunc(apid.SystemEventsSelector, postInitPlugins)

	config.SetDefault(configPollInterval, 120)
	gapidConfigId = config.GetString(configApidClusterId)
	db, err := data.DB()
	if err != nil {
		log.Panic("Unable to access DB", err)
	}

	// check for required values
	for _, key := range []string{configProxyServerBaseURI, configConsumerKey, configConsumerSecret, configSnapServerBaseURI, configChangeServerBaseURI} {
		if !config.IsSet(key) {
			return pluginData, fmt.Errorf("Missing required config value: %s", key)
		}
	}

	var count int
	row := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='apid_cluster' COLLATE NOCASE;")
	if err := row.Scan(&count); err != nil {
		log.Panic("Unable to setup database", err)
	}
	if count == 0 {
		createTables(db)
	}

	log.Debug("end init")

	return pluginData, nil
}

func createTables(db apid.DB) {
	_, err := db.Exec(`
CREATE TABLE apid_cluster (
    id text,
    instance_id text,
    name text,
    description text,
    umbrella_org_app_name text,
    created int64,
    created_by text,
    updated int64,
    updated_by text,
    _change_selector text,
    snapshotInfo text,
    lastSequence text,
    PRIMARY KEY (id)
);
CREATE TABLE data_scope (
    id text,
    apid_cluster_id text,
    scope text,
    org text,
    env text,
    created int64,
    created_by text,
    updated int64,
    updated_by text,
    _change_selector text,
    PRIMARY KEY (id)
);
`)
	if err != nil {
		log.Panic("Unable to initialize DB", err)
	}
}
