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

package dockertests

import (
	"encoding/json"
	"github.com/apid/apid-core"
	"github.com/apid/apidApigeeSync"
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
