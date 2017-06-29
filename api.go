package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"net/http"
	"encoding/json"
	"strconv"
	"time"
)

const tokenEndpoint = "/accesstoken"

func InitAPI(services apid.Services) {
	services.API().HandleFunc(tokenEndpoint, getAccessToken).Methods("GET")
}

func getAccessToken(w http.ResponseWriter, r *http.Request) {
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

	if apidTokenManager.getBearerToken() != ifNoneMatch {
		w.Write([]byte(apidTokenManager.getBearerToken()))
		return
	}

	select {
	case <-apidTokenManager.getTokenReadyChannel():
		w.Write([]byte(apidTokenManager.getBearerToken()))
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