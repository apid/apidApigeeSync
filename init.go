package apidApigeeSync

import (
	"database/sql"
	"fmt"
	"github.com/30x/apid"
)

const (
	configPollInterval        = "apigeesync_poll_interval"
	configProxyServerBaseURI  = "apigeesync_proxy_server_base"
	configSnapServerBaseURI   = "apigeesync_snapshot_server_base"
	configChangeServerBaseURI = "apigeesync_change_server_base"
	configConsumerKey         = "apigeesync_consumer_key"
	configConsumerSecret      = "apigeesync_consumer_secret"
	configScopeId             = "apigeesync_bootstrap_id"
	configSnapshotProtocol    = "apigeesync_snapshot_proto"
	configUnitTestMode        = "apigeesync_UnitTest_mode"
	ApigeeSyncEventSelector   = "ApigeeSync"
)

var (
	log    apid.LogService
	config apid.ConfigService
	data   apid.DataService
	events apid.EventsService
)

func init() {
	apid.RegisterPlugin(initPlugin)
}

func postInitPlugins() {

	log.Debug("start post plugin init")
	/* call to Download Snapshot info */
	go DownloadSnapshot()

	/* Begin Looking for changes periodically */
	log.Debug("starting update goroutine")
	go updatePeriodicChanges()

	events.Listen(ApigeeSyncEventSelector, &handler{})
	log.Debug("Done post plugin init")
}

func initPlugin(services apid.Services) error {
	log = services.Log().ForModule("apigeeSync")
	log.Debug("start init")

	config = services.Config()
	data = services.Data()
	events = services.Events()

	events.Listen(apid.PluginsInitializedEvent, postInitPlugins)

	config.SetDefault(configPollInterval, 120)

	db, err := data.DB()
	if err != nil {
		log.Panic("Unable to access DB", err)
	}

	// check for required values
	for _, key := range []string{configProxyServerBaseURI, configConsumerKey, configConsumerSecret, configSnapServerBaseURI, configChangeServerBaseURI} {
		if !config.IsSet(key) {
			return fmt.Errorf("Missing required config value: %s", key)
		}
	}

	var count int
	row := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='apid_config' COLLATE NOCASE;")
	if err := row.Scan(&count); err != nil {
		log.Panic("Unable to setup database", err)
	}
	if count == 0 {
		createTables(db)
	}

	log.Debug("end init")

	return nil
}

func createTables(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE apid_config (
    id text,
    name text,
    description text,
    umbrella_org_app_name text,
    created int64,
    created_by text,
    updated int64,
    updated_by text,
    _apid_scope text,
    snapshotInfo text,
    PRIMARY KEY (id)
);
CREATE TABLE apid_config_scope (
    id text,
    apid_config_id text,
    scope text,
    created int64,
    created_by text,
    updated int64,
    updated_by text,
    _apid_scope text,
    PRIMARY KEY (id)
);
`)
	if err != nil {
		log.Panic("Unable to initialize DB", err)
	}
}
