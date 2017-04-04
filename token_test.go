package apidApigeeSync

/*
 * Unit test of token manager
 */
import (
	"time"

	"net/http"
	"net/http/httptest"

	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("token", func() {

	Context("oauthToken", func() {

		It("should calculate valid token", func() {
			log.Info("Starting token tests...")

			t := &oauthToken{
				AccessToken: "x",
				ExpiresIn:   120000,
				ExpiresAt:   time.Now().Add(2 * time.Minute),
			}
			Expect(t.refreshIn().Seconds()).To(BeNumerically(">", 0))
			Expect(t.needsRefresh()).To(BeFalse())
			Expect(t.isValid()).To(BeTrue())
		}, 3)

		It("should calculate expired token", func() {

			t := &oauthToken{
				AccessToken: "x",
				ExpiresIn:   0,
				ExpiresAt:   time.Now(),
			}
			Expect(t.refreshIn().Seconds()).To(BeNumerically("<", 0))
			Expect(t.needsRefresh()).To(BeTrue())
			Expect(t.isValid()).To(BeFalse())
		}, 3)

		It("should calculate token needing refresh", func() {

			t := &oauthToken{
				AccessToken: "x",
				ExpiresIn:   59000,
				ExpiresAt:   time.Now().Add(time.Minute - time.Second),
			}
			Expect(t.refreshIn().Seconds()).To(BeNumerically("<", 0))
			Expect(t.needsRefresh()).To(BeTrue())
			Expect(t.isValid()).To(BeTrue())
		}, 3)

		It("should calculate on empty token", func() {

			t := &oauthToken{}
			Expect(t.refreshIn().Seconds()).To(BeNumerically("<=", 0))
			Expect(t.needsRefresh()).To(BeTrue())
			Expect(t.isValid()).To(BeFalse())
		}, 3)
	})

	Context("tokenMan", func() {

		It("should get a valid token", func() {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer GinkgoRecover()

				res := oauthToken{
					AccessToken: "ABCD",
					ExpiresIn:   1000,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))
			config.Set(configProxyServerBaseURI, ts.URL)
			testedTokenManager := createTokenManager()
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

				res := oauthToken{
					AccessToken: generateUUID(),
					ExpiresIn:   1000,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))
			config.Set(configProxyServerBaseURI, ts.URL)

			testedTokenManager := createTokenManager()
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

				res := oauthToken{
					AccessToken: string(count),
					ExpiresIn:   1000,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))

			config.Set(configProxyServerBaseURI, ts.URL)
			testedTokenManager := createTokenManager()

			testedTokenManager.getToken()

			<-finished

			testedTokenManager.close()
			ts.Close()

			close(done)
		}, 3)

		It("should have created_at_apid first time, update_at_apid after", func(done Done) {
			finished := make(chan bool, 1)
			count := 0

			newInstanceID = true

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

				count++
				if count == 1 {
					Expect(newInstanceID).To(BeTrue())
					Expect(r.Header.Get("created_at_apid")).NotTo(BeEmpty())
					Expect(r.Header.Get("updated_at_apid")).To(BeEmpty())
				} else {
					Expect(newInstanceID).To(BeFalse())
					Expect(r.Header.Get("created_at_apid")).To(BeEmpty())
					Expect(r.Header.Get("updated_at_apid")).NotTo(BeEmpty())
					finished <- true
				}
				res := oauthToken{
					AccessToken: string(count),
					ExpiresIn:   200000,
				}
				body, err := json.Marshal(res)
				Expect(err).NotTo(HaveOccurred())
				w.Write(body)
			}))

			config.Set(configProxyServerBaseURI, ts.URL)
			testedTokenManager := createTokenManager()

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
