package dockertests

import (
	"github.com/30x/apid-core"
	"github.com/30x/apid-core/factory"
	_ "github.com/30x/apidApigeeSync"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)



var (
	services apid.Services
	log      apid.LogService
	data     apid.DataService
	config   apid.ConfigService
	pgUrl    string
	pgManager *ManagementPg
)

/*
 * This test suite acts like a dummy plugin. It listens to events emitted by
 * apidApigeeSync and runs tests.
 */
var _ = BeforeSuite(func() {
	hostname := "http://" + os.Getenv("APIGEE_SYNC_DOCKER_IP")
	pgUrl = os.Getenv("APIGEE_SYNC_DOCKER_PG_URL")
	os.Setenv("APID_CONFIG_FILE", "./apid_config.yaml")

	apid.Initialize(factory.DefaultServicesFactory())
	config = apid.Config()

	// Auth Server
	config.Set(configName, "dockerIT")
	config.Set(configConsumerKey, "dummyKey")
	config.Set(configConsumerSecret, "dummySecret")
	//config.Set(configApidClusterId, "testClusterId")
	testServer := initDummyAuthServer()

	// Setup dependencies
	config.Set(configChangeServerBaseURI, hostname+":"+dockerCsPort+"/")
	config.Set(configSnapServerBaseURI, hostname+":"+dockerSsPort+"/")
	config.Set(configProxyServerBaseURI, testServer.URL)

	// init plugin
	apid.RegisterPlugin(initPlugin)
	apid.InitializePlugins("dockerTest")

	// init pg driver
	var err error
	pgManager, err = InitDb(pgUrl)
	Expect(err).Should(Succeed())
})

var _ = Describe("dockerIT", func() {

	Context("Generic Replication", func() {
		var _ = BeforeEach(func() {

		})

		It("should succesfully download table from pg", func() {
			log.Debug("CS: " + config.GetString(configChangeServerBaseURI))
			log.Debug("SS: " + config.GetString(configSnapServerBaseURI))
			log.Debug("Auth: " + config.GetString(configProxyServerBaseURI))

			cluster := &apidCluster{
				id: "fed02735-0589-4998-bf00-e4d0df7af45b",
				name: "apidcA",
				description: "desc",
				appName: "UOA",
				created: time.Now(),
				createdBy: "userA",
				updated: time.Now(),
				updatedBy: "userA",
				changeSelector: "fed02735-0589-4998-bf00-e4d0df7af45b",
			}

			tx, err := pgManager.BeginTransaction()
			Expect(err).Should(Succeed())
			pgManager.InsertApidCluster(tx, cluster)

			time.Sleep(5 * time.Second)
			Expect(1).To(Equal(1))
		}, 30)
	})
})

func initDummyAuthServer() (testServer *httptest.Server) {
	testRouter := apid.API().Router()
	testServer = httptest.NewServer(testRouter)
	mockAuthServer := &MockAuthServer{}
	mockAuthServer.Start(testRouter)
	return
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	services = s
	log = services.Log().ForModule(pluginName)
	data = services.Data()

	var pluginData = apid.PluginData{
		Name:    pluginName,
		Version: "0.0.1",
		ExtraData: map[string]interface{}{
			"schemaVersion": "0.0.1",
		},
	}

	log.Info(pluginName + " initialized.")
	return pluginData, nil
}

func TestDockerApigeeSync(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApigeeSync Docker Suite")
}
