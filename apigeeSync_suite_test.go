package apidApigeeSync

import (
	"github.com/apigee-labs/transicator/common"
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
	tmpDir     string
	testServer *httptest.Server
	testRouter apid.Router
	testMock   *MockServer
)

var _ = BeforeSuite(func(done Done) {
	apid.Initialize(factory.DefaultServicesFactory())

	config = apid.Config()

	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())
	config.Set("local_storage_path", tmpDir)

	testRouter = apid.API().Router()
	testServer = httptest.NewServer(testRouter)

	config.Set(configProxyServerBaseURI, testServer.URL)
	config.Set(configSnapServerBaseURI, testServer.URL)
	config.Set(configChangeServerBaseURI, testServer.URL)
	config.Set(configSnapshotProtocol, "json")
	config.Set(configPollInterval, 10*time.Millisecond)

	config.Set(configName, "testhost")
	config.Set(configApidClusterId, "bootstrap")
	config.Set(configConsumerKey, "XXXXXXX")
	config.Set(configConsumerSecret, "YYYYYYY")

	block = "0"
	log = apid.Log()

	// set up mock server
	mockParms := MockParms{
		ReliableAPI:  false,
		ClusterID:    config.GetString(configApidClusterId),
		TokenKey:     config.GetString(configConsumerKey),
		TokenSecret:  config.GetString(configConsumerSecret),
		Scope:        "ert452",
		Organization: "att",
		Environment:  "prod",
	}
	testMock = Mock(mockParms, testRouter)

	// This is actually the first test :)
	// Tests that entire bootstrap and set of sync operations work
	var lastSnapshot *common.Snapshot

	expectedSnapshotTables := make(map[string]bool)
	expectedSnapshotTables["kms.company"] = true
	expectedSnapshotTables["edgex.apid_cluster"] = true
	expectedSnapshotTables["edgex.data_scope"] = true

	apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
		defer GinkgoRecover()

		if s, ok := event.(*common.Snapshot); ok {

			//verify that during downloadDataSnapshot, knownTables was correctly populated
			Expect(mapIsSubset(knownTables, expectedSnapshotTables)).To(BeTrue())

			/* After this, we will mock changes for tables not present in the initial snapshot
			* until that is changed in the mock server, we have to spoof the known tables
			 */

			//add apid_cluster and data_scope since those would present if this were a real scenario
			knownTables["kms.app_credential"] = true
			knownTables["kms.app_credential_apiproduct_mapper"] = true
			knownTables["kms.developer"] = true
			knownTables["kms.company_developer"] = true
			knownTables["kms.api_product"] = true
			knownTables["kms.app"] = true

			lastSnapshot = s

			for _, t := range s.Tables {
				switch t.Name {

				case "edgex.apid_cluster":
					Expect(t.Rows).To(HaveLen(1))
					r := t.Rows[0]
					var id string
					r.Get("id", &id)
					Expect(id).To(Equal("bootstrap"))

				case "edgex.data_scope":
					Expect(t.Rows).To(HaveLen(2))
					r := t.Rows[1] // get the non-cluster row

					var id, clusterID, env, org, scope string
					r.Get("id", &id)
					r.Get("apid_cluster_id", &clusterID)
					r.Get("env", &env)
					r.Get("org", &org)
					r.Get("scope", &scope)

					Expect(id).To(Equal("ert452"))
					Expect(scope).To(Equal("ert452"))
					Expect(clusterID).To(Equal("bootstrap"))
					Expect(env).To(Equal("prod"))
					Expect(org).To(Equal("att"))
				}
			}

		} else if cl, ok := event.(*common.ChangeList); ok {

			// ensure that snapshot switched DB versions
			Expect(apidInfo.LastSnapshot).To(Equal(lastSnapshot.SnapshotInfo))
			expectedDB, err := data.DBVersion(lastSnapshot.SnapshotInfo)
			Expect(err).NotTo(HaveOccurred())
			Expect(getDB() == expectedDB).Should(BeTrue())

			Expect(cl.Changes).To(HaveLen(6))

			var tables []string
			for _, c := range cl.Changes {
				tables = append(tables, c.Table)
				Expect(c.NewRow).ToNot(BeNil())

				var tenantID string
				c.NewRow.Get("tenant_id", &tenantID)
				Expect(tenantID).To(Equal("ert452"))
			}

			Expect(tables).To(ContainElement("kms.app_credential"))
			Expect(tables).To(ContainElement("kms.app_credential_apiproduct_mapper"))
			Expect(tables).To(ContainElement("kms.developer"))
			Expect(tables).To(ContainElement("kms.company_developer"))
			Expect(tables).To(ContainElement("kms.api_product"))
			Expect(tables).To(ContainElement("kms.app"))

			events.ListenFunc(apid.EventDeliveredSelector, func(e apid.Event) {
				defer GinkgoRecover()

				// allow other handler to execute to insert last_sequence
				time.Sleep(50 * time.Millisecond)
				var seq string
				err = getDB().
					QueryRow("SELECT last_sequence FROM APID_CLUSTER LIMIT 1;").
					Scan(&seq)

				Expect(err).NotTo(HaveOccurred())
				Expect(seq).To(Equal(cl.LastSequence))

				close(done)
			})
		}
	})

	apid.InitializePlugins()
})

var _ = BeforeEach(func() {
	apid.Events().Close()

	lastSequence = ""

	_, err := getDB().Exec("DELETE FROM APID_CLUSTER")
	Expect(err).NotTo(HaveOccurred())
	_, err = getDB().Exec("DELETE FROM DATA_SCOPE")
	Expect(err).NotTo(HaveOccurred())

	db, err := data.DB()
	Expect(err).NotTo(HaveOccurred())
	_, err = db.Exec("DELETE FROM APID")
	Expect(err).NotTo(HaveOccurred())
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
