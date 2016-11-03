package apidApigeeSync

import (
	"encoding/json"
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
)

var _ = Describe("api", func() {

	var server *httptest.Server

	BeforeSuite(func() {
		apid.Initialize(factory.DefaultServicesFactory())
	})

	AfterSuite(func() {
		apid.Events().Close()
		server.Close()
	})

	It("perform sync round-trip", func(done Done) {
		count := 0
		org := "test-org"
		key := "XXXXXXX"
		secret := "YYYYYYY"

		// mock upstream server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

			// first request is for a token
			if req.URL.Path == "/v1/edgex/accesstoken" {
				Expect(req.Method).To(Equal("POST"))
				Expect(req.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded; param=value"))

				user, pw, ok := req.BasicAuth()
				Expect(ok).To(BeTrue())
				Expect(user).To(Equal(key))
				Expect(pw).To(Equal(secret))

				err := req.ParseForm()
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Form.Get("grant_type")).To(Equal("client_credentials"))
				Expect(req.Form.Get("org")).To(Equal(org))

				res := oauthTokenResp{}
				res.AccessToken = "accesstoken"
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
				return
			}

			// next requests are for changes
			if req.URL.Path == "/v1/edgex/changeagent/changes" {
				Expect(req.Method).To(Equal("GET"))

				// break polling after first time
				if count > 0 {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				count++

				q := req.URL.Query()
				Expect(q.Get("block")).To(Equal("60"))
				Expect(q.Get("since")).To(Equal("0"))
				Expect(q.Get("tag")).To(Equal("org:" + org))

				var res = ChangeSet{}
				res.Changes = []ChangePayload{
					{
						LastMsId: 10,
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
		config.Set(configOrganization, org)
		config.Set(configConsumerKey, key)
		config.Set(configConsumerSecret, secret)

		// set up temporary test database
		tmpDir, err := ioutil.TempDir("", "sync_test")
		Expect(err).NotTo(HaveOccurred())
		defer os.RemoveAll(tmpDir)

		config.Set("data_path", tmpDir)
		db, err := apid.Data().DB()
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Exec("create table change_id (org varchar(255), snapshot_change_id integer, PRIMARY KEY (org))")
		Expect(err).NotTo(HaveOccurred())

		// start process -  plugin will automatically start polling
		apid.InitializePlugins()

		h := &test_handler{
			"sync data",
			func(event apid.Event) {

				// verify event data
				changes := event.(ChangeSet)
				Expect(len(changes.Changes)).Should(Equal(1))
				Expect(changes.Changes[0].LastMsId).Should(Equal(int64(10)))

				// verify database update
				db, err := data.DB()
				Expect(err).ShouldNot(HaveOccurred())
				var lastId int64
				err = db.QueryRow("select snapshot_change_id from change_id where org=?", org).Scan(&lastId)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(lastId).Should(Equal(int64(10)))

				close(done)
			},
		}

		apid.Events().Listen(ApigeeSyncEventSelector, h)
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
