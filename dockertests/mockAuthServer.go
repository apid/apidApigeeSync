package dockertests

import (
	"encoding/json"
	"github.com/30x/apid-core"
	"github.com/30x/apidApigeeSync"
	"net/http"
)

const oauthExpiresIn = 2 * 60

type MockAuthServer struct {
}

func (m *MockAuthServer) sendToken(w http.ResponseWriter, req *http.Request) {
	oauthToken := apidApigeeSync.GenerateUUID()
	res := apidApigeeSync.OauthToken{
		AccessToken: oauthToken,
		ExpiresIn:   oauthExpiresIn,
	}
	body, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	w.Write(body)
}

func (m *MockAuthServer) Start(router apid.Router) {
	router.HandleFunc("/accesstoken", m.sendToken).Methods("POST")
}
