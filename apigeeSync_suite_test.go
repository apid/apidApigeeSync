package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"
	"time"
	"github.com/apigee-labs/transicator/common"
)

var (
	tmpDir     string
	testServer *httptest.Server
	testRouter apid.Router
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

	config.Set(configName, "testhost")
	config.Set(configApidClusterId, "bootstrap")
	config.Set(configConsumerKey, "XXXXXXX")
	config.Set(configConsumerSecret, "YYYYYYY")

	registerMockServer(testRouter)

	// This is actually the first test :)
	// Tests that entire bootstrap and set of sync operations work
	apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
		defer GinkgoRecover()

		if s, ok := event.(*common.Snapshot); ok {

			for _, t := range s.Tables {
				switch t.Name {

				case "edgex.apid_cluster":
					Expect(t.Rows).To(HaveLen(1))
					r := t.Rows[0]
					var cs, id string
					r.Get("_change_selector", &cs)
					r.Get("id", &id)

					Expect(cs).To(Equal("bootstrap"))
					Expect(id).To(Equal("bootstrap"))

				case "edgex.data_scope":
					Expect(t.Rows).To(HaveLen(2))
					r := t.Rows[1] // get the non-cluster row

					var cs, id, clusterID, env, org, scope string
					r.Get("_change_selector", &cs)
					r.Get("id", &id)
					r.Get("apid_cluster_id", &clusterID)
					r.Get("env", &env)
					r.Get("org", &org)
					r.Get("scope", &scope)

					Expect(id).To(Equal("ert452"))
					Expect(cs).To(Equal("ert452"))
					Expect(scope).To(Equal("ert452"))
					Expect(clusterID).To(Equal("bootstrap"))
					Expect(env).To(Equal("prod"))
					Expect(org).To(Equal("att"))

				//case "kms.api_product":
				//	Expect(t.Rows).To(HaveLen(0))

				//default:
				//	Fail("invalid table: " + t.Name)
				}
			}

		} else if cl, ok := event.(*common.ChangeList); ok {

			//Expect(cl.LastSequence).To(Equal("lastSeq_01"))
			Expect(cl.Changes).To(HaveLen(1))

			c := cl.Changes[0]
			Expect(c.Table).To(Equal("edgex.data_scope"))
			Expect(c.Operation).To(Equal(common.Insert))

			Expect(c.NewRow).ToNot(BeNil())

			var id, clusterID, env, org, scope string
			c.NewRow.Get("id", &id)
			c.NewRow.Get("apid_cluster_id", &clusterID)
			c.NewRow.Get("env", &env)
			c.NewRow.Get("org", &org)
			c.NewRow.Get("scope", &scope)

			Expect(id).To(Equal("apid_config_scope_id_1"))
			Expect(clusterID).To(Equal("bootstrap"))
			Expect(env).To(Equal("prod"))
			Expect(org).To(Equal("att"))
			Expect(scope).To(Equal("ert452"))

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

	token = ""
	downloadDataSnapshot = false
	downloadBootSnapshot = false
	changeFinished = false
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
