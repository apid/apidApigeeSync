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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/apid/apid-core"

	"github.com/apid/apid-core/factory"
)

var (
	tmpDir string
)

const dummyConfigValue string = "placeholder"
const expectedClusterId = "bootstrap"

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())
	config = apid.Config()
	dataService = apid.Data()
	eventService = apid.Events()
	log = apid.Log().ForModule("apigeeSync")
	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())
	config.Set("local_storage_path", tmpDir)
	config.Set(configProxyServerBaseURI, dummyConfigValue)
	config.Set(configSnapServerBaseURI, dummyConfigValue)
	config.Set(configChangeServerBaseURI, dummyConfigValue)
	config.Set(configSnapshotProtocol, "sqlite")
	config.Set(configPollInterval, 10*time.Millisecond)
	config.Set(configDiagnosticMode, false)
	config.Set(configConsumerKey, "XXXXXXX")
	config.Set(configConsumerSecret, "YYYYYYY")
	config.Set(configApidInstanceID, "YYYYYYY")
}, 3)

var _ = BeforeEach(func() {
	config.Set(configName, "testhost")
	config.Set(configApidClusterId, expectedClusterId)
	apidInfo.ClusterID = expectedClusterId
	apidInfo.InstanceID = "YYYYYYY"
	apidInfo.LastSnapshot = ""
	apidInfo.IsNewInstance = true
})

var _ = AfterEach(func() {
})

var _ = AfterSuite(func() {
	apid.Events().Close()
	os.RemoveAll(tmpDir)
})

func TestApigeeSync(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApigeeSync Suite")
}
