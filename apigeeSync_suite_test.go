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
	"encoding/json"
	"net/http"
	"github.com/apigee-labs/transicator/common"
	"time"
	"strconv"
)

var (
	tmpDir     string
	testServer *httptest.Server
	testRouter apid.Router
)

const testScope = "bootstrap"

var _ = BeforeSuite(func(done Done) {
	var phase int

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

	// mock upstream testServer
	testRouter.HandleFunc("/accesstoken", func(w http.ResponseWriter, req *http.Request) {
		defer GinkgoRecover()

		Expect(req.Method).To(Equal("POST"))
		Expect(req.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded; param=value"))

		err := req.ParseForm()
		// TODO: Test framework cannot handle this assertions and
		// this handler just stops and sends back ""
		// we need to handle it differently
		Expect(err).NotTo(HaveOccurred())
		Expect(req.Form.Get("grant_type")).To(Equal("client_credentials"))
		Expect(req.Header.Get("status")).To(Equal("ONLINE"))
		Expect(req.Header.Get("apid_cluster_Id")).To(Equal("bootstrap"))
		Expect(req.Header.Get("display_name")).To(Equal("testhost"))

		var plugInfo []pluginDetail
		plInfo := []byte(req.Header.Get("plugin_details"))
		err = json.Unmarshal(plInfo, &plugInfo)
		Expect(err).NotTo(HaveOccurred())

		Expect(plugInfo[0].Name).To(Equal("apidApigeeSync"))
		Expect(plugInfo[0].SchemaVersion).To(Equal("0.0.2"))

		res := oauthTokenResp{}
		res.AccessToken = "accesstoken"
		body, err := json.Marshal(res)
		Expect(err).NotTo(HaveOccurred())
		w.Write(body)

	}).Methods("POST")

	testRouter.HandleFunc("/snapshots", func(w http.ResponseWriter, req *http.Request) {
		defer GinkgoRecover()

		q := req.URL.Query()

		if phase == 0 {
			phase = 1
			Expect(q.Get("scope")).To(Equal(testScope))
			Expect(req.Header.Get("apid_cluster_Id")).To(Equal("bootstrap"))

			apidcfgItem := common.Row{}
			apidcfgItems := []common.Row{}
			apidcfgItemCh := common.Row{}
			apidcfgItemsCh := []common.Row{}
			scv := &common.ColumnVal{
				Value: testScope,
				Type:  1,
			}
			apidcfgItem["id"] = scv
			scv = &common.ColumnVal{
				Value: testScope,
				Type:  1,
			}
			apidcfgItem["_change_selector"] = scv
			apidcfgItems = append(apidcfgItems, apidcfgItem)

			scv = &common.ColumnVal{
				Value: "apid_config_scope_id_0",
				Type:  1,
			}
			apidcfgItemCh["id"] = scv

			scv = &common.ColumnVal{
				Value: "apid_config_scope_id_0",
				Type:  1,
			}
			apidcfgItemCh["_change_selector"] = scv

			scv = &common.ColumnVal{
				Value: testScope,
				Type:  1,
			}
			apidcfgItemCh["apid_cluster_id"] = scv

			scv = &common.ColumnVal{
				Value: "ert452",
				Type:  1,
			}
			apidcfgItemCh["scope"] = scv

			{
				scv = &common.ColumnVal{
					Value: "att",
					Type:  1,
				}
				apidcfgItemCh["org"] = scv

			}
			{
				scv = &common.ColumnVal{
					Value: "prod",
					Type:  1,
				}
				apidcfgItemCh["env"] = scv
			}

			apidcfgItemsCh = append(apidcfgItemsCh, apidcfgItemCh)

			res := &common.Snapshot{}
			res.SnapshotInfo = "snapinfo1"

			res.Tables = []common.Table{
				{
					Name: "edgex.apid_cluster",
					Rows: apidcfgItems,
				},
				{
					Name: "edgex.data_scope",
					Rows: apidcfgItemsCh,
				},
			}

			body, err := json.Marshal(res)
			Expect(err).NotTo(HaveOccurred())

			w.Write(body)
			return
		} else {
			phase = 2
			scopes := q["scope"]
			Expect(len(scopes)).Should(Equal(2))
			Expect(scopes).To(ContainElement(testScope))
			Expect(scopes).To(ContainElement("ert452"))
			res := &common.Snapshot{}
			res.SnapshotInfo = "snapinfo1"

			apidcfgItems := []common.Row{}
			res.Tables = []common.Table{
				{
					Name: "kms.api_product",
					Rows: apidcfgItems,
				},
			}

			body, err := json.Marshal(res)
			Expect(err).NotTo(HaveOccurred())

			w.Write(body)
			return
		}

	}).Methods("GET")

	testRouter.HandleFunc("/changes", func(w http.ResponseWriter, req *http.Request) {
		defer GinkgoRecover()

		if req.URL.Query().Get("since") == "lastSeq_01" {
			go func() {
				block, err := strconv.Atoi(req.URL.Query().Get("block"))
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(time.Duration(block) * time.Second)
				w.WriteHeader(http.StatusNotModified)
			}()
			return
		}

		Expect(req.Header.Get("apid_cluster_Id")).To(Equal("bootstrap"))
		q := req.URL.Query()
		Expect(q.Get("snapshot")).To(Equal("snapinfo1"))
		scope := q["scope"]
		Expect(scope).To(ContainElement("ert452"))
		Expect(scope).To(ContainElement(testScope))

		res := &common.ChangeList{}

		res.LastSequence = "lastSeq_01"
		mpItems := common.Row{}

		scv := &common.ColumnVal{
			Value: "apid_config_scope_id_1",
			Type:  1,
		}
		mpItems["id"] = scv

		scv = &common.ColumnVal{
			Value: testScope,
			Type:  1,
		}
		mpItems["apid_cluster_id"] = scv

		scv = &common.ColumnVal{
			Value: "ert452",
			Type:  1,
		}
		mpItems["scope"] = scv
		{
			scv = &common.ColumnVal{
				Value: "att",
				Type:  1,
			}
			mpItems["org"] = scv
		}
		{
			scv = &common.ColumnVal{
				Value: "prod",
				Type:  1,
			}
			mpItems["env"] = scv
		}

		res.Changes = []common.Change{
			{
				Table:     "edgex.data_scope",
				NewRow:    mpItems,
				Operation: 1,
			},
		}
		body, err := json.Marshal(res)
		Expect(err).NotTo(HaveOccurred())
		w.Write(body)

	}).Methods("GET")

	// This is actually the first test :)
	// Tests that entire bootstrap and set of sync operations work
	apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
		defer GinkgoRecover()

		if s, ok := event.(*common.Snapshot); ok {

			Expect(s.SnapshotInfo).Should(Equal("snapinfo1"))

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
					Expect(t.Rows).To(HaveLen(1))
					r := t.Rows[0]

					var cs, id, clusterID, env, org, scope string
					r.Get("_change_selector", &cs)
					r.Get("id", &id)
					r.Get("apid_cluster_id", &clusterID)
					r.Get("env", &env)
					r.Get("org", &org)
					r.Get("scope", &scope)

					Expect(id).To(Equal("apid_config_scope_id_0"))
					Expect(cs).To(Equal("apid_config_scope_id_0"))
					Expect(clusterID).To(Equal("bootstrap"))
					Expect(env).To(Equal("prod"))
					Expect(org).To(Equal("att"))
					Expect(scope).To(Equal("ert452"))

				case "kms.api_product":
					Expect(t.Rows).To(HaveLen(0))

				default:
					Fail("invalid table: " + t.Name)
				}
			}

		} else if cl, ok := event.(*common.ChangeList); ok {

			Expect(cl.LastSequence).To(Equal("lastSeq_01"))
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
				Expect(seq).To(Equal("lastSeq_01"))

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
