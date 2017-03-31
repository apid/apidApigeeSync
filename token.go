package apidApigeeSync

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync"
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

func createTokenManager() *tokenMan {

	t := &tokenMan{}
	t.doRefresh = make(chan bool, 1)
	t.quitPollingForToken = make(chan bool, 1)
	t.tokenRefreshed = make(chan bool)
	t.closed = make(chan bool)
	t.retrieveNewToken()
	t.maintainToken()
	return t
}

type tokenMan struct {
	sync.Mutex
	token     *oauthToken
	doRefresh chan bool
	quitPollingForToken chan bool
	tokenRefreshed chan bool
	closed chan bool
}

func (t *tokenMan) getBearerToken() string {
	return t.getToken().AccessToken
}

func (t *tokenMan) maintainToken() {
	go func() {
		for {
			t.Lock()
			token := t.token
			t.Unlock()
			select {

			case _, ok := <-t.doRefresh:
				if !ok {
					log.Debug("closed tokenMan")
					t.closed <- true
					return
				}
				log.Debug("force token refresh")
				t.retrieveNewToken()
				t.tokenRefreshed <- true
			case <-time.After(token.refreshIn()):
				log.Debug("auto refresh token")
				t.retrieveNewToken()

			}
		}
	}()
}

func (t *tokenMan) invalidateToken() {
	log.Debug("invalidating token")
	t.Lock()
	t.token = nil
	t.Unlock()
	t.doRefresh <- true
	//ensure refresh signal has been received
	<-t.tokenRefreshed
}

// will block until valid
//assumption is that if we can get the lock, then it's valid
func (t *tokenMan) getToken() *oauthToken {
	t.Lock()
	defer t.Unlock()
	return t.token
}

func (t *tokenMan) close() {
	log.Debug("close token manager")
	t.quitPollingForToken <- true
	close(t.doRefresh)
	//block until close signal has been received by maintenance routine
	<-t.closed
}

// don't call externally. will block until success.
func (t *tokenMan) retrieveNewToken() {

	log.Debug("Getting OAuth token...")
	uriString := config.GetString(configProxyServerBaseURI)
	uri, err := url.Parse(uriString)
	if err != nil {
		log.Panicf("unable to parse uri config '%s' value: '%s': %v", configProxyServerBaseURI, uriString, err)
	}
	uri.Path = path.Join(uri.Path, "/accesstoken")

	pollWithBackoff(t.quitPollingForToken, t.getRetrieveNewTokenClosure(uri), func(err error) {log.Errorf("Error getting new token : ", err)})
}

func (t *tokenMan) getRetrieveNewTokenClosure(uri *url.URL) func(chan bool) error {
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

		if newInstanceID {
			req.Header.Set("created_at_apid", time.Now().Format(time.RFC3339))
		} else {
			req.Header.Set("updated_at_apid", time.Now().Format(time.RFC3339))
		}

		client := &http.Client{Timeout: httpTimeout}
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
			return expected200Error{}
		}

		var token oauthToken
		err = json.Unmarshal(body, &token)
		if err != nil {
			log.Errorf("unable to unmarshal JSON response '%s': %v", string(body), err)
			return err
		}

		if token.ExpiresIn > 0 {
			token.ExpiresAt = time.Now().Add(time.Duration(token.ExpiresIn) * time.Millisecond)
		} else {
			// no expiration, arbitrarily expire about a year from now
			token.ExpiresAt = time.Now().Add(365 * 24 * time.Hour)
		}

		log.Debugf("Got new token: %#v", token)

		if newInstanceID {
			newInstanceID = false
			err = updateApidInstanceInfo()
			if err != nil {
				log.Errorf("unable to unmarshal update apid instance info : %v", string(body), err)
				return err

			}
		}
		t.Lock()
		t.token = &token
		t.Unlock()
		config.Set(configBearerToken, token.AccessToken)

		return nil
	}
}

type oauthToken struct {
	IssuedAt       int64    `json:"issuedAt"`
	AppName        string   `json:"applicationName"`
	Scope          string   `json:"scope"`
	Status         string   `json:"status"`
	ApiProdList    []string `json:"apiProductList"`
	ExpiresIn      int64    `json:"expiresIn"`
	DeveloperEmail string   `json:"developerEmail"`
	TokenType      string   `json:"tokenType"`
	ClientId       string   `json:"clientId"`
	AccessToken    string   `json:"accessToken"`
	RefreshExpIn   int64    `json:"refreshTokenExpiresIn"`
	RefreshCount   int64    `json:"refreshCount"`
	ExpiresAt      time.Time
}

var noTime time.Time

func (t *oauthToken) isValid() bool {
	if t == nil || t.AccessToken == "" {
		return false
	}
	return t.AccessToken != "" && time.Now().Before(t.ExpiresAt)
}

func (t *oauthToken) refreshIn() time.Duration {
	if t == nil || t.ExpiresAt == noTime {
		return time.Duration(0)
	}
	return t.ExpiresAt.Sub(time.Now()) - refreshFloatTime
}

func (t *oauthToken) needsRefresh() bool {
	if t == nil || t.ExpiresAt == noTime {
		return true
	}
	return time.Now().Add(refreshFloatTime).After(t.ExpiresAt)
}
