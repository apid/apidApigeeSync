package apidApigeeSync

import (
	"github.com/apigee-labs/transicator/common"
	"net/url"
)

type tokenManager interface {
	getBearerToken() string
	invalidateToken() error
	getToken() *oauthToken
	close()
	getRetrieveNewTokenClosure(*url.URL) func(chan bool) error
	start()
}

type snapShotManager interface {
	close() <-chan bool
	downloadBootSnapshot()
	storeBootSnapshot(snapshot *common.Snapshot)
	downloadDataSnapshot()
	storeDataSnapshot(snapshot *common.Snapshot)
	downloadSnapshot(scopes []string, snapshot *common.Snapshot) error
}

type changeManager interface {
	close() <-chan bool
	pollChangeWithBackoff()
}
