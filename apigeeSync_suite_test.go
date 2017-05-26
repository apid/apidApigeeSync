package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/30x/apid-core"

	"github.com/30x/apid-core/factory"
)

var (
	tmpDir         string
	testServer     *httptest.Server
	testRouter     apid.Router
	testMock       *MockServer
	wipeDBAferTest bool
)

const dummyConfigValue string = "placeholder"
const expectedClusterId = "bootstrap"

var _ = BeforeSuite(func() {
	wipeDBAferTest = true
})

var _ = BeforeEach(func(done Done) {
	apid.Initialize(factory.DefaultServicesFactory())

	config = apid.Config()

	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())
	config.Set("local_storage_path", tmpDir)

	config.Set(configProxyServerBaseURI, dummyConfigValue)
	config.Set(configSnapServerBaseURI, dummyConfigValue)
	config.Set(configChangeServerBaseURI, dummyConfigValue)
	config.Set(configSnapshotProtocol, "sqlite")
	config.Set(configPollInterval, 10*time.Millisecond)

	config.Set(configName, "testhost")
	config.Set(configApidClusterId, expectedClusterId)
	config.Set(configConsumerKey, "XXXXXXX")
	config.Set(configConsumerSecret, "YYYYYYY")

	block = "0"
	log = apid.Log()

	_initPlugin(apid.AllServices())
	createManagers()
	close(done)
}, 3)

var _ = AfterEach(func() {
	apid.Events().Close()

	lastSequence = ""

	if wipeDBAferTest {
		db, err := dataService.DB()
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec("DELETE FROM APID")
		Expect(err).NotTo(HaveOccurred())
	}
	wipeDBAferTest = true
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
