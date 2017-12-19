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
	"github.com/apid/apid-core/factory"
	"github.com/apid/apidApigeeSync"
	_ "github.com/apid/apidApigeeSync"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

var (
	services            apid.Services
	log                 apid.LogService
	dataService         apid.DataService
	config              apid.ConfigService
	pgUrl               string
	pgManager           *ManagementPg
	clusterIdFromConfig string
)

/*
 * This test suite acts like a dummy plugin. It listens to events emitted by
 * apidApigeeSync and runs tests.
 */
var _ = BeforeSuite(func() {
	defer GinkgoRecover()
	//hostname := "http://" + os.Getenv("APIGEE_SYNC_DOCKER_IP")
	pgUrl = os.Getenv("APIGEE_SYNC_DOCKER_PG_URL") + "?sslmode=disable"
	os.Setenv("APID_CONFIG_FILE", "./apid_config.yaml")

	apid.Initialize(factory.DefaultServicesFactory())
	config = apid.Config()

	localStorage := config.GetString(configLocalStoragePath)
	err := os.RemoveAll(localStorage)
	Expect(err).Should(Succeed())
	err = os.MkdirAll(localStorage, 0700)
	Expect(err).Should(Succeed())

	// init pg driver and data
	pgManager, err = InitDb(pgUrl)
	Expect(err).Should(Succeed())
	initPgData()

	// Auth Server
	config.Set(configName, "dockerIT")
	config.Set(configConsumerKey, "dummyKey")
	config.Set(configConsumerSecret, "dummySecret")
	testServer := initDummyAuthServer()

	initDone := make(chan bool)
	handler := &waitSnapshotHandler{initDone}

	// hang until snapshot received
	apid.Events().Listen(ApigeeSyncEventSelector, handler)

	config.Set(configProxyServerBaseURI, testServer.URL)

	// init plugin
	apid.RegisterPlugin(initPlugin, apidApigeeSync.PluginData)
	apid.InitializePlugins("dockerTest")

	<-initDone
}, 5)

var _ = AfterSuite(func() {
	err := pgManager.CleanupAll()
	Expect(err).Should(Succeed())
})

var _ = Describe("dockerIT", func() {

	/*
	 * Isolation between tests is not perfect.
	 * If in a test you listen to any event, please make sure you stop listening to it,
	 * and don't let it mess up later tests.
	 */
	Context("Generic Replication", func() {
		var _ = BeforeEach(func() {

		})

		var _ = AfterEach(func() {
			err := pgManager.CleanupTest()
			Expect(err).Should(Succeed())
		})

		It("should succesfully download new table from pg", func(done Done) {
			tableName := "docker_test_download"
			targetTablename := "edgex_" + tableName
			handler := &newTableHandler{
				targetTablename: targetTablename,
				done:            done,
				verifyFunc:      verifyTestTableExist,
			}

			apid.Events().Listen(ApigeeSyncEventSelector, handler)
			createTestTable(tableName)

		}, 1)

		It("should get data according to data scope", func(done Done) {
			tableName := "docker_test_scope"
			targetTablename := "edgex_" + tableName
			handler := &newTableHandler{
				targetTablename: targetTablename,
				done:            done,
				verifyFunc:      verifyTestTableData,
			}

			apid.Events().Listen(ApigeeSyncEventSelector, handler)
			createTestTableWithData(tableName)

		}, 1)

		It("should replicate ENUM type of pg correctly", func(done Done) {
			tableName := "docker_test_enum"
			targetTablename := "edgex_" + tableName
			handler := &newTableHandler{
				targetTablename: targetTablename,
				done:            done,
				verifyFunc:      verifyTestTableEnum,
			}

			apid.Events().Listen(ApigeeSyncEventSelector, handler)
			createTestTableWithEnum(tableName)

		}, 1)
	})
})

func createTestTable(tableName string) {
	tx, err := pgManager.BeginTransaction()
	Expect(err).Should(Succeed())
	defer tx.Rollback()
	_, err = tx.Exec("CREATE TABLE edgex." + tableName + " (id varchar primary key, val integer, _change_selector varchar);")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("ALTER TABLE edgex." + tableName + " replica identity full;")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('three', 3, '" + clusterIdFromConfig + "');")
	Expect(err).Should(Succeed())
	tx.Commit()
}

func createTestTableWithData(tableName string) {
	tx, err := pgManager.BeginTransaction()
	Expect(err).Should(Succeed())
	defer tx.Rollback()
	_, err = tx.Exec("CREATE TABLE edgex." + tableName + " (id varchar primary key, val integer, _change_selector varchar);")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("ALTER TABLE edgex." + tableName + " replica identity full;")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('one', 1, 'foo');")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('two', 2, 'bar');")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('three', 3, '" + clusterIdFromConfig + "');")
	Expect(err).Should(Succeed())
	tx.Commit()
}

func createTestTableWithEnum(tableName string) {
	tx, err := pgManager.BeginTransaction()
	Expect(err).Should(Succeed())
	defer tx.Rollback()
	_, err = tx.Exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("CREATE TABLE edgex." + tableName + " (id varchar primary key, val mood, _change_selector varchar);")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("ALTER TABLE edgex." + tableName + " replica identity full;")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('one', 'sad', 'foo');")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('two', 'ok', 'bar');")
	Expect(err).Should(Succeed())
	_, err = tx.Exec("INSERT INTO edgex." + tableName + " values ('three', 'happy', '" + clusterIdFromConfig + "');")
	Expect(err).Should(Succeed())
	tx.Commit()
}

func dropTestTable(targetTableName string, sqliteDb apid.DB) {
	tx, err := pgManager.BeginTransaction()
	Expect(err).Should(Succeed())
	_, err = tx.Exec("DROP TABLE IF EXISTS edgex." + targetTableName + ";")
	Expect(err).Should(Succeed())
}

func initDummyAuthServer() (testServer *httptest.Server) {
	testRouter := apid.API().Router()
	testServer = httptest.NewServer(testRouter)
	mockAuthServer := &MockAuthServer{}
	mockAuthServer.Start(testRouter)
	return
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	services = s
	log = services.Log().ForModule(pluginName)
	dataService = services.Data()

	var pluginData = apid.PluginData{
		Name:    pluginName,
		Version: "0.0.1",
		ExtraData: map[string]interface{}{
			"schemaVersion": "0.0.1",
		},
	}

	log.Info(pluginName + " initialized.")
	return pluginData, nil
}

func initPgData() {
	clusterIdFromConfig = config.GetString(configApidClusterId) //"4c6bb536-0d64-43ca-abae-17c08f1a7e58"
	clusterId := clusterIdFromConfig
	scopeId := "ae418890-2c22-4c6a-b218-69e261034b96"
	deploymentId := "633af126-ee79-4a53-bef7-7ba30da8aad6"
	bundleConfigId := "613ce223-6c73-43f4-932c-3c69b0c7c65d"
	bundleConfigName := "good"
	bundleUri := "https://gist.github.com/alexkhimich/843cf70ffd6a8b4d44442876ba0487b7/archive/d74360596ff9a4320775d590b3f5a91bdcdf61d2.zip"
	t := time.Now()

	cluster := &apidClusterRow{
		id:             clusterId,
		name:           "apidcA",
		description:    "desc",
		appName:        "UOA",
		created:        t,
		createdBy:      testInitUser,
		updated:        t,
		updatedBy:      testInitUser,
		changeSelector: clusterId,
	}

	ds := &dataScopeRow{
		id:             scopeId,
		clusterId:      clusterId,
		scope:          "abc1",
		org:            "org1",
		env:            "env1",
		created:        t,
		createdBy:      testInitUser,
		updated:        t,
		updatedBy:      testInitUser,
		changeSelector: clusterId,
		orgScope:       "org1",
		envScope:       "env1",
	}

	bf := bundleConfigData{
		Id:        bundleConfigId,
		Created:   t.Format(time.RFC3339),
		CreatedBy: testInitUser,
		Updated:   t.Format(time.RFC3339),
		UpdatedBy: testInitUser,
		Name:      bundleConfigName,
		Uri:       bundleUri,
	}

	jsonBytes, err := json.Marshal(bf)
	Expect(err).Should(Succeed())

	bfr := &bundleConfigRow{
		id:           bf.Id,
		scopeId:      scopeId,
		name:         bf.Name,
		uri:          bf.Uri,
		checksumType: "",
		checksum:     "",
		created:      t,
		createdBy:    bf.CreatedBy,
		updated:      t,
		updatedBy:    bf.UpdatedBy,
	}

	d := &deploymentRow{
		id:               deploymentId,
		configId:         bundleConfigId,
		clusterId:        clusterId,
		scopeId:          scopeId,
		bundleConfigName: bundleConfigName,
		bundleConfigJson: string(jsonBytes),
		configJson:       "{}",
		created:          t,
		createdBy:        testInitUser,
		updated:          t,
		updatedBy:        testInitUser,
		changeSelector:   clusterId,
	}

	tx, err := pgManager.BeginTransaction()
	Expect(err).Should(Succeed())
	defer tx.Rollback()
	err = pgManager.InsertApidCluster(tx, cluster)
	Expect(err).Should(Succeed())
	err = pgManager.InsertDataScope(tx, ds)
	Expect(err).Should(Succeed())
	err = pgManager.InsertBundleConfig(tx, bfr)
	Expect(err).Should(Succeed())
	err = pgManager.InsertDeployment(tx, d)
	Expect(err).Should(Succeed())
	err = tx.Commit()
	Expect(err).Should(Succeed())
}

type waitSnapshotHandler struct {
	initDone chan bool
}

func (w *waitSnapshotHandler) Handle(event apid.Event) {
	if _, ok := event.(*common.Snapshot); ok {
		apid.Events().StopListening(ApigeeSyncEventSelector, w)
		w.initDone <- true
	}
}

type newTableHandler struct {
	targetTablename string
	done            Done
	verifyFunc      func(string, apid.DB)
}

func (n *newTableHandler) Handle(event apid.Event) {
	if s, ok := event.(*common.Snapshot); ok {
		defer GinkgoRecover()
		sqliteDb, err := dataService.DBVersion(s.SnapshotInfo)
		Expect(err).Should(Succeed())
		n.verifyFunc(n.targetTablename, sqliteDb)
		apid.Events().StopListening(ApigeeSyncEventSelector, n)
		close(n.done)
	}
}

func verifyTestTableExist(targetTableName string, sqliteDb apid.DB) {
	rows, err := sqliteDb.Query("SELECT DISTINCT tableName FROM _transicator_tables;")
	Expect(err).Should(Succeed())
	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		Expect(err).Should(Succeed())

		if tableName == targetTableName {
			return
		}
	}
	Fail("Table " + targetTableName + " doesn'r exist!")
}

func verifyTestTableData(targetTableName string, sqliteDb apid.DB) {
	rows, err := sqliteDb.Query("SELECT id FROM " + targetTableName + ";")
	Expect(err).Should(Succeed())
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		Expect(err).Should(Succeed())
		Expect(id).To(Equal("three"))
		count += 1
	}
	Expect(count).To(Equal(1))
}

func verifyTestTableEnum(targetTableName string, sqliteDb apid.DB) {
	rows, err := sqliteDb.Query("SELECT val FROM " + targetTableName + ";")
	Expect(err).Should(Succeed())
	defer rows.Close()
	count := 0
	for rows.Next() {
		var val string
		err = rows.Scan(&val)
		Expect(err).Should(Succeed())
		Expect(val).To(Equal("happy"))
		count += 1
	}
	Expect(count).To(Equal(1))
}

func TestDockerApigeeSync(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApigeeSync Docker Suite")
}
