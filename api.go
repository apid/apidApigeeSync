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

import (
	"encoding/json"
	"github.com/apid/apid-core"
	"net/http"
	"strconv"
	"time"
)

const tokenEndpoint = "/accesstoken"

type ApiManager struct {
	tokenMan tokenManager
}

func (a *ApiManager) InitAPI(api apid.APIService) {
	api.HandleFunc(tokenEndpoint, a.getAccessToken).Methods("GET")
}

func (a *ApiManager) getAccessToken(w http.ResponseWriter, r *http.Request) {
	b := r.URL.Query().Get("block")
	var timeout int
	if b != "" {
		var err error
		timeout, err = strconv.Atoi(b)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad block value, must be number of seconds")
			return
		}
	}
	log.Debugf("api timeout: %d", timeout)
	ifNoneMatch := r.Header.Get("If-None-Match")

	if a.tokenMan.getBearerToken() != ifNoneMatch {
		w.Write([]byte(a.tokenMan.getBearerToken()))
		return
	}

	select {
	case <-a.tokenMan.getTokenReadyChannel():
		w.Write([]byte(a.tokenMan.getBearerToken()))
	case <-time.After(time.Duration(timeout) * time.Second):
		w.WriteHeader(http.StatusNotModified)
	}
}

func writeError(w http.ResponseWriter, status int, reason string) {
	w.WriteHeader(status)
	e := errorResponse{
		ErrorCode: status,
		Reason:    reason,
	}
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Errorf("unable to marshal errorResponse: %v", err)
	} else {
		w.Write(bytes)
	}
	log.Debugf("sending %d error to client: %s", status, reason)
}

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}
