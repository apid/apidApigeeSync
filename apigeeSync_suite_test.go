package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http/httptest"
	"os"
)

var (
	tmpDir     string
	testServer *httptest.Server
	testRouter apid.Router
)

const testScope = "bootstrap"

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())

	config := apid.Config()

	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())
	config.Set("local_storage_path", tmpDir)

	testRouter = apid.API().Router()
	testServer = httptest.NewServer(testRouter)

	config.Set(configProxyServerBaseURI, testServer.URL)
	config.Set(configSnapServerBaseURI, testServer.URL)
	config.Set(configChangeServerBaseURI, testServer.URL)
	config.Set(configApidClusterId, "apid_config_scope_0")
	config.Set(configName, "testhost")

	config.Set(configSnapshotProtocol, "json")
	config.Set(configApidClusterId, testScope)
	config.Set(configConsumerKey, "XXXXXXX")
	config.Set(configConsumerSecret, "YYYYYYY")

	apid.InitializePlugins()

})

var _ = AfterSuite(func() {
	apid.Events().Close()
	if testServer != nil {
		testServer.Close()
	}
	os.RemoveAll(tmpDir)
})

func TestApigeeSync(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApigeeSync Suite")
}
