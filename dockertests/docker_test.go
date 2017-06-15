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
	"encoding/json"
)

var (
	services  apid.Services
	log       apid.LogService
	data      apid.DataService
	config    apid.ConfigService
	pgUrl     string
	pgManager *ManagementPg
)

/*
 * This test suite acts like a dummy plugin. It listens to events emitted by
 * apidApigeeSync and runs tests.
 */
var _ = BeforeSuite(func() {
	//hostname := "http://" + os.Getenv("APIGEE_SYNC_DOCKER_IP")
	pgUrl = os.Getenv("APIGEE_SYNC_DOCKER_PG_URL") + "?sslmode=disable"
	os.Setenv("APID_CONFIG_FILE", "./apid_config.yaml")

	// init pg driver and data
	var err error
	pgManager, err = InitDb(pgUrl)
	Expect(err).Should(Succeed())
	initPgData()


	apid.Initialize(factory.DefaultServicesFactory())
	config = apid.Config()

	// Auth Server
	config.Set(configName, "dockerIT")
	config.Set(configConsumerKey, "dummyKey")
	config.Set(configConsumerSecret, "dummySecret")
	//config.Set(configApidClusterId, "testClusterId")
	testServer := initDummyAuthServer()

	// Setup dependencies
	//config.Set(configChangeServerBaseURI, hostname+":"+dockerCsPort+"/")
	//config.Set(configSnapServerBaseURI, hostname+":"+dockerSsPort+"/")
	config.Set(configProxyServerBaseURI, testServer.URL)

	// init plugin
	apid.RegisterPlugin(initPlugin)
	apid.InitializePlugins("dockerTest")


})

var _ = Describe("dockerIT", func() {

	Context("Generic Replication", func() {
		var _ = BeforeEach(func() {

		})

		var _ = AfterEach(func() {
			pgManager.Cleanup()
		})

		It("should succesfully download table from pg", func() {
			log.Debug("CS: " + config.GetString(configChangeServerBaseURI))
			log.Debug("SS: " + config.GetString(configSnapServerBaseURI))
			log.Debug("Auth: " + config.GetString(configProxyServerBaseURI))

			time.Sleep(5 * time.Second)
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

func initPgData() {
	clusterId := "4c6bb536-0d64-43ca-abae-17c08f1a7e58"
	scopeId := "ae418890-2c22-4c6a-b218-69e261034b96"
	deploymentId := "633af126-ee79-4a53-bef7-7ba30da8aad6"
	bundleConfigId := "613ce223-6c73-43f4-932c-3c69b0c7c65d"
	bundleConfigName := "good"
	bundleUri := "https://gist.github.com/alexkhimich/843cf70ffd6a8b4d44442876ba0487b7/archive/d74360596ff9a4320775d590b3f5a91bdcdf61d2.zip"
	t := time.Now()

	cluster := &apidClusterRow{
		id:             clusterId,
		name:           "apidcA",
		description:    "desc",
		appName:        "UOA",
		created:        t,
		createdBy:      testInitUser,
		updated:        t,
		updatedBy:      testInitUser,
		changeSelector: clusterId,
	}

	ds := &dataScopeRow{
		id:             scopeId,
		clusterId:      clusterId,
		scope:          "abc1",
		org:            "org1",
		env:            "env1",
		created:        t,
		createdBy:      testInitUser,
		updated:        t,
		updatedBy:      testInitUser,
		changeSelector: clusterId,
	}

	bf := bundleConfigData{
		Id: bundleConfigId,
		Created: t.Format(time.RFC3339),
		CreatedBy: testInitUser,
		Updated: t.Format(time.RFC3339),
		UpdatedBy: testInitUser,
		Name: bundleConfigName,
		Uri: bundleUri,
	}

	jsonBytes, err := json.Marshal(bf)
	Expect(err).Should(Succeed())

	log.Warn(string(jsonBytes))

	bfr := &bundleConfigRow{
		id: bf.Id,
		scopeId: scopeId,
		name: bf.Name,
		uri: bf.Uri,
		checksumType: "",
		checksum: "",
		created: t,
		createdBy: bf.CreatedBy,
		updated: t,
		updatedBy: bf.UpdatedBy,
	}

	d := &deploymentRow{
		id: deploymentId,
		configId: bundleConfigId,
		clusterId: clusterId,
		scopeId: scopeId,
		bundleConfigName: bundleConfigName,
		bundleConfigJson: string(jsonBytes),
		configJson: "{}",
		created: t,
		createdBy: testInitUser,
		updated: t,
		updatedBy: testInitUser,
		changeSelector: clusterId,
	}

	tx, err := pgManager.BeginTransaction()
	defer tx.Rollback()
	Expect(err).Should(Succeed())
	err = pgManager.InsertApidCluster(tx, cluster)
	Expect(err).Should(Succeed())
	err = pgManager.InsertDataScope(tx, ds)
	Expect(err).Should(Succeed())
	err = pgManager.InsertBundleConfig(tx, bfr)
	Expect(err).Should(Succeed())
	err = pgManager.InsertDeployment(tx, d)
	Expect(err).Should(Succeed())
	err = tx.Commit()
	Expect(err).Should(Succeed())
}

func TestDockerApigeeSync(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApigeeSync Docker Suite")
}
