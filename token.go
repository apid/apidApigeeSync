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
	"bytes"
	"encoding/json"
	"github.com/apid/apid-core/util"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"
	"time"
)

var (
	refreshFloatTime = time.Minute
)

/*
Usage:
   man := createTokenManager()
   bearer := man.getBearerToken()
   // will automatically update config(configBearerToken) for other modules
   // optionally, when done...
   man.close()
*/

func createApidTokenManager(isNewInstance bool) *apidTokenManager {
	isClosedInt := int32(0)

	t := &apidTokenManager{
		quitPollingForToken: make(chan bool, 1),
		closed:              make(chan bool),
		getTokenChan:        make(chan bool),
		invalidateTokenChan: make(chan bool),
		returnTokenChan:     make(chan *OauthToken),
		invalidateDone:      make(chan bool),
		tokenUpdatedChan:    make(chan bool, 1),
		isClosed:            &isClosedInt,
		isNewInstance:       isNewInstance,
	}
	return t
}

type apidTokenManager struct {
	token               *OauthToken
	isClosed            *int32
	quitPollingForToken chan bool
	closed              chan bool
	getTokenChan        chan bool
	invalidateTokenChan chan bool
	refreshTimer        <-chan time.Time
	returnTokenChan     chan *OauthToken
	invalidateDone      chan bool
	tokenUpdatedChan    chan bool
	isNewInstance       bool
}

func (t *apidTokenManager) start() {
	t.retrieveNewToken()
	t.refreshTimer = time.After(t.token.refreshIn())
	go t.maintainToken()
}

func (t *apidTokenManager) getBearerToken() string {
	return t.getToken().AccessToken
}

func (t *apidTokenManager) maintainToken() {
	for {
		select {
		case <-t.closed:
			return
		case <-t.refreshTimer:
			log.Debug("auto refresh token")
			t.retrieveNewToken()
			t.refreshTimer = time.After(t.token.refreshIn())
		case <-t.getTokenChan:
			token := t.token
			t.returnTokenChan <- token
		case <-t.invalidateTokenChan:
			t.retrieveNewToken()
			t.refreshTimer = time.After(t.token.refreshIn())
			t.invalidateDone <- true
		}
	}
}

// will block until valid
func (t *apidTokenManager) invalidateToken() {
	log.Debug("invalidating token")
	t.invalidateTokenChan <- true
	<-t.invalidateDone
}

func (t *apidTokenManager) getToken() *OauthToken {
	//has been closed
	if atomic.LoadInt32(t.isClosed) == int32(1) {
		log.Debug("TokenManager: getToken() called on closed tokenManager")
		return nil
	}
	t.getTokenChan <- true
	return <-t.returnTokenChan
}

/*
 * blocking close() of tokenMan
 */

func (t *apidTokenManager) close() {
	//has been closed
	if atomic.SwapInt32(t.isClosed, 1) == int32(1) {
		log.Panic("TokenManager: close() has been called before!")
		return
	}
	log.Debug("close token manager")
	t.quitPollingForToken <- true
	// sending instead of closing, to make sure it enters the t.doRefresh branch
	t.closed <- true
	close(t.closed)
	log.Debug("token manager closed")
}

// don't call externally. will block until success.
func (t *apidTokenManager) retrieveNewToken() {

	log.Debug("Getting OAuth token...")
	uriString := config.GetString(configProxyServerBaseURI)
	uri, err := url.Parse(uriString)
	if err != nil {
		log.Panicf("unable to parse uri config '%s' value: '%s': %v", configProxyServerBaseURI, uriString, err)
	}
	uri.Path = path.Join(uri.Path, "/accesstoken")

	pollWithBackoff(t.quitPollingForToken, t.getRetrieveNewTokenClosure(uri), func(err error) { log.Errorf("Error getting new token : %v", err) })
}

func (t *apidTokenManager) getRetrieveNewTokenClosure(uri *url.URL) func(chan bool) error {
	return func(_ chan bool) error {
		form := url.Values{}
		form.Set("grant_type", "client_credentials")
		form.Add("client_id", config.GetString(configConsumerKey))
		form.Add("client_secret", config.GetString(configConsumerSecret))
		req, err := http.NewRequest("POST", uri.String(), bytes.NewBufferString(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
		req.Header.Set("display_name", apidInfo.InstanceName)
		req.Header.Set("apid_instance_id", apidInfo.InstanceID)
		req.Header.Set("apid_cluster_Id", apidInfo.ClusterID)
		req.Header.Set("status", "ONLINE")
		req.Header.Set("plugin_details", apidPluginDetails)

		if t.isNewInstance {
			req.Header.Set("created_at_apid", time.Now().Format(time.RFC3339))
			t.isNewInstance = false
		} else {
			req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
		}

		client := &http.Client{
			Transport: util.Transport(config.GetString(util.ConfigfwdProxyPortURL)),
			Timeout:   httpTimeout,
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Errorf("Unable to Connect to Edge Proxy Server: %v", err)
			return err
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Errorf("Unable to read EdgeProxy Sever response: %v", err)
			return err
		}

		if resp.StatusCode != 200 {
			log.Errorf("Oauth Request Failed with Resp Code: %d. Body: %s", resp.StatusCode, string(body))
			return expected200Error
		}

		var token OauthToken
		err = json.Unmarshal(body, &token)
		if err != nil {
			log.Errorf("unable to unmarshal JSON response '%s': %v", string(body), err)
			return err
		}

		if token.ExpiresIn > 0 {
			token.ExpiresAt = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
		} else {
			// no expiration, arbitrarily expire about a year from now
			token.ExpiresAt = time.Now().Add(365 * 24 * time.Hour)
		}

		log.Debugf("Got new token: %#v", token)
		t.token = &token
		config.Set(configBearerToken, token.AccessToken)

		//don't block on the buffered channel.  that means there is already a signal to serve new token
		//TODO: This assumes apid-gateway is 1-1 mapping. Make use of generic long-polling provided by apid-core
		select {
		case t.tokenUpdatedChan <- true:
		default:
			log.Debug("Token refresh notification already sent")
		}

		return nil
	}
}

func (t *apidTokenManager) getTokenReadyChannel() <-chan bool {
	return t.tokenUpdatedChan
}

type OauthToken struct {
	IssuedAt    int64    `json:"issuedAt"`
	AppName     string   `json:"applicationName"`
	Scope       string   `json:"scope"`
	Status      string   `json:"status"`
	ApiProdList []string `json:"apiProductList"`
	// in seconds
	ExpiresIn      int64  `json:"expiresIn"`
	DeveloperEmail string `json:"developerEmail"`
	TokenType      string `json:"tokenType"`
	ClientId       string `json:"clientId"`
	AccessToken    string `json:"accessToken"`
	RefreshExpIn   int64  `json:"refreshTokenExpiresIn"`
	RefreshCount   int64  `json:"refreshCount"`
	ExpiresAt      time.Time
}

var noTime time.Time

func (t *OauthToken) isValid() bool {
	if t == nil || t.AccessToken == "" {
		return false
	}
	return t.AccessToken != "" && time.Now().Before(t.ExpiresAt)
}

func (t *OauthToken) refreshIn() time.Duration {
	if t == nil || t.ExpiresAt == noTime {
		return time.Duration(0)
	}
	return t.ExpiresAt.Sub(time.Now()) - refreshFloatTime
}

func (t *OauthToken) needsRefresh() bool {
	if t == nil || t.ExpiresAt == noTime {
		return true
	}
	return time.Now().Add(refreshFloatTime).After(t.ExpiresAt)
}
