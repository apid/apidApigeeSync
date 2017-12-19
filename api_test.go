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
	"fmt"
	"github.com/apid/apid-core"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	apiTestUrl = "http://127.0.0.1:9000"
)

var _ = Describe("API Manager", func() {
	testCount := 0
	var testApiMan *ApiManager
	var dummyTokenMan *dummyTokenManager
	var client *http.Client
	BeforeEach(func() {
		testCount++
		dummyTokenMan = &dummyTokenManager{
			token:          fmt.Sprintf("test_token_%d", testCount),
			tokenReadyChan: make(chan bool, 1),
		}
		testApiMan = &ApiManager{
			endpoint: tokenEndpoint + strconv.Itoa(testCount),
			tokenMan: dummyTokenMan,
		}
		testApiMan.InitAPI(apid.API())
		time.Sleep(100 * time.Millisecond)
		client = &http.Client{}
	})

	clientGet := func(path string, pars map[string][]string, header map[string][]string) (int, []byte) {
		uri, err := url.Parse(apiTestUrl + path)
		Expect(err).Should(Succeed())
		query := url.Values(pars)
		uri.RawQuery = query.Encode()
		httpReq, err := http.NewRequest("GET", uri.String(), nil)
		httpReq.Header = http.Header(header)
		Expect(err).Should(Succeed())
		res, err := client.Do(httpReq)
		Expect(err).Should(Succeed())
		defer res.Body.Close()
		responseBody, err := ioutil.ReadAll(res.Body)
		Expect(err).Should(Succeed())
		return res.StatusCode, responseBody
	}

	It("should get token without long-polling", func() {
		code, res := clientGet(testApiMan.endpoint, nil, nil)
		Expect(code).Should(Equal(http.StatusOK))
		Expect(string(res)).Should(Equal(dummyTokenMan.token))
	})

	It("should get bad request for invalid timeout", func() {
		code, _ := clientGet(testApiMan.endpoint, map[string][]string{
			parBlock: {"invalid"},
		}, map[string][]string{
			parTag: {dummyTokenMan.getBearerToken()},
		})
		Expect(code).Should(Equal(http.StatusBadRequest))

		code, _ = clientGet(testApiMan.endpoint, map[string][]string{
			parBlock: {"-1"},
		}, map[string][]string{
			parTag: {dummyTokenMan.getBearerToken()},
		})
		Expect(code).Should(Equal(http.StatusBadRequest))
	})

	It("should get token immediately if mismatch", func() {
		code, res := clientGet(testApiMan.endpoint, map[string][]string{
			parBlock: {"10"},
		}, map[string][]string{
			parTag: {"mismatch"},
		})
		Expect(code).Should(Equal(http.StatusOK))
		Expect(string(res)).Should(Equal(dummyTokenMan.token))
	}, 3)

	It("should get StatusNotModified if timeout", func() {
		code, _ := clientGet(testApiMan.endpoint, map[string][]string{
			parBlock: {"1"},
		}, map[string][]string{
			parTag: {dummyTokenMan.getBearerToken()},
		})
		Expect(code).Should(Equal(http.StatusNotModified))
	}, 3)

	It("should do long-polling", func() {
		go func() {
			time.Sleep(1)
			dummyTokenMan.token = "new_token"
			dummyTokenMan.tokenReadyChan <- true
		}()
		code, res := clientGet(testApiMan.endpoint, map[string][]string{
			parBlock: {"10"},
		}, map[string][]string{
			parTag: {dummyTokenMan.getBearerToken()},
		})
		Expect(code).Should(Equal(http.StatusOK))
		Expect(string(res)).Should(Equal(dummyTokenMan.getBearerToken()))
	}, 3)

})
