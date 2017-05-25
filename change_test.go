
package apidApigeeSync





import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	"net/http/httptest"
	"net/url"
	"errors"
	"os"
)


var _ = Describe("Change Agent", func() {

	Context("Change Agent", func() {
		handler := handler{}

		var createTestDb = func(sqlfile string, dbId string) common.Snapshot {
			initDb(sqlfile, "./mockdb.sqlite3")
			file, err := os.Open("./mockdb.sqlite3")
			if err != nil {
				Fail("Failed to open mock db for test")
			}

			s := common.Snapshot{}
			err = processSnapshotServerFileResponse(dbId, file, &s)
			if err != nil {
				Fail("Error processing test snapshots")
			}
			return s
		}

		BeforeEach(func() {
			event := createTestDb("./sql/init_listener_test_valid_snapshot.sql", "test_snapshot_valid")

			handler.Handle(&event)
		})

		var initializeContext = func() {
			testRouter = apid.API().Router()
			testServer = httptest.NewServer(testRouter)

			// set up mock server
			mockParms := MockParms{
				ReliableAPI:  true,
				ClusterID:    config.GetString(configApidClusterId),
				TokenKey:     config.GetString(configConsumerKey),
				TokenSecret:  config.GetString(configConsumerSecret),
				Scope:        "ert452",
				Organization: "att",
				Environment:  "prod",
			}
			testMock = Mock(mockParms, testRouter)

			config.Set(configProxyServerBaseURI, testServer.URL)
			config.Set(configSnapServerBaseURI, testServer.URL)
			config.Set(configChangeServerBaseURI, testServer.URL)
		}

		var restoreContext = func() {

			testServer.Close()
			config.Set(configProxyServerBaseURI, dummyConfigValue)
			config.Set(configSnapServerBaseURI, dummyConfigValue)
			config.Set(configChangeServerBaseURI, dummyConfigValue)

		}

		It("test change server agent", func() {
			log.Debug("test change server agent")
			testTokenManager := &dummyTokenManager{make(chan bool)}
			apidTokenManager = testTokenManager
			apidTokenManager.start()
			apidSnapshotManager = &dummySnapshotManager{}
			initializeContext()
			testMock.forceAuthFail()
			wipeDBAferTest = true
			apidChangeManager.pollChangeWithBackoff()
			<- testTokenManager.invalidateChan
			log.Debug("closing")
			<- apidChangeManager.close()
			restoreContext()
		}, 5)


	})
})


type dummyTokenManager struct {
	invalidateChan chan bool

}

func (t * dummyTokenManager) getBearerToken() string {
	return ""
}

func (t * dummyTokenManager) invalidateToken() error {
	log.Debug("invalidateToken called")
	testMock.passAuthCheck()
	t.invalidateChan <- true
	return errors.New("invalidate called")
}

func (t * dummyTokenManager) getToken() *oauthToken {
	return nil
}

func (t * dummyTokenManager) close() {
	return
}

func (t * dummyTokenManager) getRetrieveNewTokenClosure(*url.URL) func(chan bool) error {
	return func(chan bool) error{
		return nil
	}
}

func (* dummyTokenManager) start() {

}

type dummySnapshotManager struct {

}

func (* dummySnapshotManager) close() <-chan bool {
	closeChan := make(chan bool)
	close(closeChan)
	return closeChan
}

func (* dummySnapshotManager) downloadBootSnapshot() {

}

func (* dummySnapshotManager) storeBootSnapshot(snapshot *common.Snapshot) {

}

func (* dummySnapshotManager) downloadDataSnapshot(){

}

func (* dummySnapshotManager) storeDataSnapshot(snapshot *common.Snapshot) {

}

func (* dummySnapshotManager) downloadSnapshot(scopes []string, snapshot *common.Snapshot) error{
	return nil
}