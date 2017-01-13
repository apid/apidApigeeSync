package apidApigeeSync

import (
	"encoding/json"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
	"time"
)

var _ = Describe("api", func() {

	var phase = 0

	It("should perform all sync phases", func(done Done) {

		// mock upstream testServer
		testRouter.HandleFunc("/accesstoken", func(w http.ResponseWriter, req *http.Request) {
			defer GinkgoRecover()

			Expect(req.Method).To(Equal("POST"))
			Expect(req.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded; param=value"))

			err := req.ParseForm()
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

				log.Debugf("/snapshots writing: %v", body)

				w.Write(body)
				return
			} else {
				phase = 2
				Expect(q.Get("scope")).To(Equal("ert452"))
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

				log.Debugf("/snapshots writing: %v", body)

				w.Write(body)
				return
			}

		}).Methods("GET")

		testRouter.HandleFunc("/changes", func(w http.ResponseWriter, req *http.Request) {
			defer GinkgoRecover()

			Expect(req.Header.Get("apid_cluster_Id")).To(Equal("bootstrap"))
			q := req.URL.Query()
			Expect(q.Get("snapshot")).To(Equal("snapinfo1"))
			scparams := q["scope"]
			Expect(scparams).To(ContainElement("ert452"))
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

		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
			defer GinkgoRecover()
			if _, ok := event.(*common.Snapshot); ok {
				if phase > 1 {
					db := getDB()
					// verify event data (post snapshot)
					var count int
					err := db.QueryRow(`
							Select count(scp.id)
							FROM data_scope AS scp
							INNER JOIN apid_cluster AS ap
							WHERE scp.apid_cluster_id = ap.id
						`).Scan(&count)
					Expect(err).NotTo(HaveOccurred())
					Expect(count).Should(Equal(1))
				}
			} else if _, ok := event.(*common.ChangeList); ok {
				// verify event data (post change)
				// There should be 2 scopes now
				time.Sleep(200 * time.Millisecond)
				db := getDB()
				var count int
				err := db.QueryRow(`
						SELECT count(scp.id)
						FROM data_scope AS scp
						INNER JOIN apid_cluster AS ap
						WHERE scp.apid_cluster_id = ap.id
					`).Scan(&count)
				Expect(err).NotTo(HaveOccurred())
				Expect(count).Should(Equal(2))
				close(done)
			} else {
				Fail("Unexpected event")
			}

		})
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
