// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apidApigeeSync

/*
 * Unit test of token manager
 */
import (
	"time"

	"net/http"
	"net/http/httptest"

	"encoding/json"

	"database/sql"
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("token", func() {

	Context("oauthToken", func() {

		It("should calculate valid token", func() {
			log.Info("Starting token tests...")

			t := &OauthToken{
				AccessToken: "x",
				ExpiresIn:   120,
				ExpiresAt:   time.Now().Add(2 * time.Minute),
			}
			Expect(t.refreshIn().Seconds()).To(BeNumerically(">", 0))
			Expect(t.needsRefresh()).To(BeFalse())
			Expect(t.isValid()).To(BeTrue())
		}, 3)

		It("should calculate expired token", func() {

			t := &OauthToken{
				AccessToken: "x",
				ExpiresIn:   0,
				ExpiresAt:   time.Now(),
			}
			Expect(t.refreshIn().Seconds()).To(BeNumerically("<", 0))
			Expect(t.needsRefresh()).To(BeTrue())
			Expect(t.isValid()).To(BeFalse())
		}, 3)

		It("should calculate token needing refresh", func() {

			t := &OauthToken{
				AccessToken: "x",
				ExpiresIn:   59,
				ExpiresAt:   time.Now().Add(time.Minute - time.Second),
			}
			Expect(t.refreshIn().Seconds()).To(BeNumerically("<", 0))
			Expect(t.needsRefresh()).To(BeTrue())
			Expect(t.isValid()).To(BeTrue())
		}, 3)

		It("should calculate on empty token", func() {

			t := &OauthToken{}
			Expect(t.refreshIn().Seconds()).To(BeNumerically("<=", 0))
			Expect(t.needsRefresh()).To(BeTrue())
			Expect(t.isValid()).To(BeFalse())
		}, 3)
	})

	Context("tokenMan", func() {

		It("should get a valid token", func() {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				res := OauthToken{
					AccessToken: "ABCD",
					ExpiresIn:   1,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))
			config.Set(configProxyServerBaseURI, ts.URL)
			testedTokenManager := createTokenManager(&dummyDbMan{})
			testedTokenManager.start()
			token := testedTokenManager.getToken()

			Expect(token.AccessToken).ToNot(BeEmpty())
			Expect(token.ExpiresIn > 0).To(BeTrue())
			Expect(token.ExpiresAt).To(BeTemporally(">", time.Now()))

			bToken := testedTokenManager.getBearerToken()
			Expect(bToken).To(Equal(token.AccessToken))
			testedTokenManager.close()
			ts.Close()
		}, 3)

		It("should refresh when forced to", func() {

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				res := OauthToken{
					AccessToken: GenerateUUID(),
					ExpiresIn:   1,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))
			config.Set(configProxyServerBaseURI, ts.URL)

			testedTokenManager := createTokenManager(&dummyDbMan{})
			testedTokenManager.start()
			token := testedTokenManager.getToken()
			Expect(token.AccessToken).ToNot(BeEmpty())

			testedTokenManager.invalidateToken()

			token2 := testedTokenManager.getToken()
			Expect(token).ToNot(Equal(token2))
			Expect(token.AccessToken).ToNot(Equal(token2.AccessToken))
			testedTokenManager.close()
			ts.Close()
		}, 3)

		It("should refresh in refresh interval", func(done Done) {

			finished := make(chan bool, 1)
			start := time.Now()
			count := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				count++
				if count > 1 {
					if start.Add(500).After(time.Now()) {
						Fail("didn't refresh within expected interval")
					}
					finished <- true
				}

				res := OauthToken{
					AccessToken: string(count),
					ExpiresIn:   1,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))

			config.Set(configProxyServerBaseURI, ts.URL)
			testedTokenManager := createTokenManager(&dummyDbMan{})
			testedTokenManager.start()
			testedTokenManager.getToken()

			<-finished

			testedTokenManager.close()
			ts.Close()

			close(done)
		}, 3)

		It("should have created_at_apid first time, update_at_apid after", func(done Done) {
			finished := make(chan bool, 1)
			count := 0

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

				count++
				if count == 1 {
					Expect(r.Header.Get("created_at_apid")).NotTo(BeEmpty())
					Expect(r.Header.Get("updated_at_apid")).To(BeEmpty())
				} else {
					Expect(r.Header.Get("created_at_apid")).To(BeEmpty())
					Expect(r.Header.Get("updated_at_apid")).NotTo(BeEmpty())
					finished <- true
				}
				res := OauthToken{
					AccessToken: string(count),
					ExpiresIn:   200,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))

			config.Set(configProxyServerBaseURI, ts.URL)
			testedTokenManager := createTokenManager(&dummyDbMan{})
			testedTokenManager.start()
			testedTokenManager.getToken()
			testedTokenManager.invalidateToken()
			testedTokenManager.getToken()
			<-finished
			testedTokenManager.close()
			ts.Close()
			close(done)
		}, 3)
	})
})

type dummyDbMan struct{}

func (d *dummyDbMan) initDefaultDb() error {
	return nil
}
func (d *dummyDbMan) setDb(apid.DB) {}
func (d *dummyDbMan) getDb() apid.DB {
	return nil
}
func (d *dummyDbMan) insert(tableName string, rows []common.Row, txn *sql.Tx) bool {
	return true
}
func (d *dummyDbMan) deleteRowsFromTable(tableName string, rows []common.Row, txn *sql.Tx) bool {
	return true
}
func (d *dummyDbMan) update(tableName string, oldRows, newRows []common.Row, txn *sql.Tx) bool {
	return true
}
func (d *dummyDbMan) getPkeysForTable(tableName string) ([]string, error) {
	return nil, nil
}
func (d *dummyDbMan) findScopesForId(configId string) []string {
	return nil
}
func (d *dummyDbMan) getDefaultDb() (apid.DB, error) {
	return nil, nil
}
func (d *dummyDbMan) updateApidInstanceInfo() error {
	return nil
}
func (d *dummyDbMan) getApidInstanceInfo() (info apidInstanceInfo, err error) {
	return
}
func (d *dummyDbMan) getLastSequence() string {
	return ""
}
func (d *dummyDbMan) updateLastSequence(lastSequence string) error {
	return nil
}
