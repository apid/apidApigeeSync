package dockertests

import (
	"github.com/30x/apidApigeeSync"
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"net/http/httptest"
	"os"
	"github.com/30x/apid-core/factory"
	"github.com/Sirupsen/logrus"
)

const (
	dockerCsPort string = "9000"
	dockerSsPort string = "9001"
	dockerPgPort string = "5432"
	pluginName = "apigeeSyncDockerTest"
	configLogLevel            = "log_level"
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
	services            apid.Services
	log                 apid.LogService
	data                apid.DataService
	config apid.ConfigService
)
/*
 * This test suite acts like a dummy plugin. It listens to events emitted by
 * apidApigeeSync and runs tests.
 */

var _ = Describe("dockerIT", func() {



	BeforeSuite(func() {
		hostname := os.Getenv("APIGEE_SYNC_DOCKER_IP")

		apid.Initialize(factory.DefaultServicesFactory())
		config = apid.Config()

		// Set log level
		config.Set(configLogLevel, logrus.DebugLevel.String())

		// Auth Server
		config.Set(configName, "dockerIT")
		config.Set(configConsumerKey, "dummyKey")
		config.Set(configConsumerSecret, "dummySecret")
		config.Set(configApidClusterId, "testClusterId")
		testServer := initDummyAuthServer()

		// Setup dependencies
		config.Set(configChangeServerBaseURI, hostname+":"+dockerCsPort)
		config.Set(configSnapServerBaseURI, hostname+":"+dockerSsPort)
		config.Set(configProxyServerBaseURI, testServer.URL)

		// init plugin
		apid.RegisterPlugin(initPlugin)
		apid.InitializePlugins("dockerTest")
	})



	Context("Generic Replication", func() {
		var _ = BeforeEach(func() {

		})

		It("should succesfully download table from pg", func() {
			log.Debug("CS: " + config.GetString(configChangeServerBaseURI))
			log.Debug("SS: " + config.GetString(configSnapServerBaseURI))
			log.Debug("Auth: " + config.GetString(configProxyServerBaseURI))
		})
	})
})

func initDummyAuthServer() (testServer *httptest.Server) {
	testRouter := apid.API().Router()
	testServer = httptest.NewServer(testRouter)
	// set up mock server
	mockParms := apidApigeeSync.MockParms{
		ReliableAPI:  true,
		ClusterID:    config.GetString(configApidClusterId),
		TokenKey:     config.GetString(configConsumerKey),
		TokenSecret:  config.GetString(configConsumerSecret),
		Scope:        "dockerTest",
		Organization: "dockerTest",
		Environment:  "prod",
	}
	apidApigeeSync.Mock(mockParms, testRouter)
	return
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	services = s
	log = services.Log().ForModule(pluginName)
	data = services.Data()

	var pluginData = apid.PluginData {
		Name:    pluginName,
		Version: "0.0.1",
		ExtraData: map[string]interface{}{
			"schemaVersion": "0.0.1",
		},
	}


	log.Info(pluginName + " initialized.")
	return pluginData, nil
}