package apidApigeeSync

import (
	"encoding/json"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"time"
)

var _ = Describe("api", func() {

	var server *httptest.Server
	var plugInfo []pluginDetail

	BeforeSuite(func() {
		apid.Initialize(factory.DefaultServicesFactory())
	})

	AfterSuite(func() {
		apid.Events().Close()
		server.Close()
	})

	It("perform sync round-trip", func(done Done) {
		scount := 0
		phase := 0
		scope := "bootstrap"
		key := "XXXXXXX"
		secret := "YYYYYYY"

		// mock upstream server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

			// first request is for a token
			if req.URL.Path == "/accesstoken" {
				Expect(req.Method).To(Equal("POST"))
				Expect(req.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded; param=value"))

				err := req.ParseForm()
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Form.Get("grant_type")).To(Equal("client_credentials"))
				Expect(req.Header.Get("status")).To(Equal("ONLINE"))
				Expect(req.Header.Get("apid_cluster_Id")).To(Equal("bootstrap"))
				Expect(req.Header.Get("display_name")).To(Equal("testhost"))
				plinfo := []byte(req.Header.Get("plugin_details"))
				err = json.Unmarshal(plinfo, &plugInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(plugInfo[0].Name).To(Equal("apidApigeeSync"))
				Expect(plugInfo[0].SchemaVersion).To(Equal("0.0.1"))

				res := oauthTokenResp{}
				res.AccessToken = "accesstoken"
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
				return
			}

			// next requests are for changes
			if req.URL.Path == "/snapshots" {
				Expect(req.Method).To(Equal("GET"))
				q := req.URL.Query()

				if phase == 0 {
					phase = 1
					Expect(q.Get("scope")).To(Equal(scope))

					apidcfgItem := common.Row{}
					apidcfgItems := []common.Row{}
					apidcfgItemCh := common.Row{}
					apidcfgItemsCh := []common.Row{}
					scv := &common.ColumnVal{
						Value: scope,
						Type:  1,
					}
					apidcfgItem["id"] = scv
					scv = &common.ColumnVal{
						Value: scope,
						Type:  1,
					}
					apidcfgItem["_apid_scope"] = scv
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
					apidcfgItemCh["_apid_scope"] = scv

					scv = &common.ColumnVal{
						Value: scope,
						Type:  1,
					}
					apidcfgItemCh["apid_config_id"] = scv

					scv = &common.ColumnVal{
						Value: "att~prod",
						Type:  1,
					}
					apidcfgItemCh["scope"] = scv

					apidcfgItemsCh = append(apidcfgItemsCh, apidcfgItemCh)

					res := &common.Snapshot{}
					res.SnapshotInfo = "snapinfo1"

					res.Tables = []common.Table{
						{
							Name: "edgex.apid_config",
							Rows: apidcfgItems,
						},
						{
							Name: "edgex.apid_config_scope",
							Rows: apidcfgItemsCh,
						},
					}

					body, err := json.Marshal(res)
					Expect(err).NotTo(HaveOccurred())
					w.Write(body)
					return
				} else {
					phase = 2
					Expect(q.Get("scope")).To(Equal("att~prod"))
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

			}
			// next requests are for changes
			if req.URL.Path == "/changes" {
				Expect(req.Method).To(Equal("GET"))
				q := req.URL.Query()
				Expect(q.Get("snapshot")).To(Equal("snapinfo1"))
				scparams := q["scope"]
				Expect(scparams).To(ContainElement("att~prod"))
				Expect(scparams).To(ContainElement("bootstrap"))

				res := &common.ChangeList{}

				res.LastSequence = "lastSeq_01"
				mpItems := common.Row{}

				scv := &common.ColumnVal{
					Value: "apid_config_scope_id_1",
					Type:  1,
				}
				mpItems["id"] = scv

				scv = &common.ColumnVal{
					Value: scope,
					Type:  1,
				}
				mpItems["apid_config_id"] = scv

				scv = &common.ColumnVal{
					Value: "att~test",
					Type:  1,
				}
				mpItems["scope"] = scv

				res.Changes = []common.Change{
					{
						Table:     "edgex.apid_config_scope",
						NewRow:    mpItems,
						Operation: 1,
					},
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
				return
			}
			Fail("should not reach")
		}))

		config = apid.Config()
		config.Set(configProxyServerBaseURI, server.URL)
		config.Set(configSnapServerBaseURI, server.URL)
		config.Set(configChangeServerBaseURI, server.URL)
		config.Set(configScopeId, "apid_config_scope_0")
		config.Set(configName, "testhost")

		config.Set(configSnapshotProtocol, "json")
		config.Set(configScopeId, scope)
		config.Set(configConsumerKey, key)
		config.Set(configConsumerSecret, secret)

		// set up temporary test database
		tmpDir, err := ioutil.TempDir("", "apigee_sync_test")
		Expect(err).NotTo(HaveOccurred())
		defer os.RemoveAll(tmpDir)

		config.Set("data_path", tmpDir)

		// start process -  plugin will automatically start polling
		apid.InitializePlugins()

		h := &test_handler{
			"sync data",
			func(event apid.Event) {
				_, ok := event.(*common.Snapshot)
				if ok {
					if phase > 1 {
						db, err := data.DB()
						Expect(err).NotTo(HaveOccurred())
						// verify event data (post snapshot)
						err = db.QueryRow("Select count(scp.id) from apid_config_scope as scp INNER JOIN apid_config as ap WHERE scp.apid_config_id = ap.id").Scan(&scount)
						Expect(err).NotTo(HaveOccurred())
						Expect(scount).Should(Equal(1))
					}
				} else {
					// verify event data (post change)
					// There should be 2 scopes now
					_, ok := event.(*common.ChangeList)
					if ok {
						time.Sleep(200 * time.Millisecond)
						db, err := data.DB()
						Expect(err).NotTo(HaveOccurred())
						err = db.QueryRow("Select count(scp.id) from apid_config_scope as scp INNER JOIN apid_config as ap WHERE scp.apid_config_id = ap.id").Scan(&scount)
						Expect(err).NotTo(HaveOccurred())
						Expect(scount).Should(Equal(2))
						close(done)
					} else {
						Fail("Unexpected event")
					}
				}

			},
		}

		donehandler := func(e apid.Event) {
			if rsp, ok := e.(apid.EventDeliveryEvent); ok {
				Expect(rsp.Description).Should(Equal("event complete"))
			} else {
				Fail("Unexpected event")
			}
		}
		apid.Events().Listen(ApigeeSyncEventSelector, h)
		events.ListenFunc(apid.EventDeliveredSelector, donehandler)

	})
})

type test_handler struct {
	description string
	f           func(event apid.Event)
}

func (t *test_handler) String() string {
	return t.description
}

func (t *test_handler) Handle(event apid.Event) {
	t.f(event)
}
