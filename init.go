package apidApigeeSync

import (
	"fmt"
	"github.com/30x/apid"
)

const (
	configOrganization       = "apigeesync_organization" // todo: how are we supporting multiple orgs?
	configPollInterval       = "apigeesync_poll_interval"
	configProxyServerBaseURI = "apigeesync_proxy_server_base"
	configConsumerKey        = "apigeesync_consumer_key"
	configConsumerSecret     = "apigeesync_consumer_secret"

	ApigeeSyncEventSelector = "ApigeeSync"
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

func initPlugin(services apid.Services) error {
	log = services.Log().ForModule("apigeeSync")
	log.Debug("start init")

	config = services.Config()
	data = services.Data()
	events = services.Events()

	config.SetDefault(configPollInterval, 120)

	// check for required values
	for _, key := range []string{configProxyServerBaseURI, configOrganization, configConsumerKey, configConsumerSecret} {
		if !config.IsSet(key) {
			return fmt.Errorf("Missing required config value: %s", key)
		}
	}

	log.Debug("starting update goroutine")
	go updatePeriodicChanges()

	log.Debug("end init")

	return nil
}
