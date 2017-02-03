package apidApigeeSync

import (
	"encoding/json"
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/gomega"
	"net/http"
	"strconv"
	"time"
	"fmt"
	. "github.com/onsi/ginkgo"
)
/*
Currently limited to: 1 cluster, 1 data_scope, 1 organization, 1 env, 1 company

Notes:
cluster_id is static per params
_change_selector == data_scope
data_scope == org + env
tenant_id == org

company => * developer
developer => * app
application => * app_credential
product => * app_credential
*/

type MockParms struct {
	ReliableAPI                 bool
	ClusterID                   string
	TokenKey                    string
	TokenSecret                 string
	Scope                       string
	Organization                string
	Environment                 string
	NumDevelopers               int
	NumApplicationsPerDeveloper int
	AddDeveloperEvery           time.Duration
	UpdateDeveloperEvery        time.Duration
	NumDeployments              int
	ReplaceDeploymentEvery      time.Duration
}

func Mock(params MockParms, router apid.Router) *MockServer {
	m := &MockServer{}
	m.params = params

	m.init()
	m.registerRoutes(router)
	return m
}

func registerMockServer(testRouter apid.Router) {

	p := MockParms{
		ReliableAPI:                 true,
		ClusterID:                   config.GetString(configApidClusterId),
		TokenKey:                    config.GetString(configConsumerKey),
		TokenSecret:                 config.GetString(configConsumerSecret),
		Scope:                       "ert452",
		Organization:                "att",
		Environment:                 "prod",
		NumDevelopers:               5,
		NumApplicationsPerDeveloper: 1,
		UpdateDeveloperEvery:        1 * time.Second,
		NumDeployments:              100,
		ReplaceDeploymentEvery:      3 * time.Second,
	}

	m := MockServer{}
	m.params = p

	m.init()
	m.registerRoutes(testRouter)
}

// table name -> common.Row
type tableRowMap map[string]common.Row

type MockServer struct {
	params            MockParms
	oauthToken        string
	snapshotID        string
	snapshotTables    map[string][]common.Table	// key = scopeID
	sequenceID        string
	changeChannel     chan []byte
}

func (m *MockServer) init() {
	m.changeChannel = make(chan []byte)
	go m.developerGenerator()

	// cluster "scope"
	cluster := m.newRow(map[string]string{
		"id":               m.params.ClusterID,
		"_change_selector": m.params.ClusterID,
	})

	// data scopes
	var dataScopes []common.Row
	dataScopes = append(dataScopes, cluster)
	dataScopes = append(dataScopes, m.newRow(map[string]string{
		"id":               m.params.Scope,
		"scope":            m.params.Scope,
		"org":              m.params.Organization,
		"env":              m.params.Environment,
		"apid_cluster_id":  m.params.ClusterID,
		"_change_selector": m.params.Scope,
	}))

	// cluster & data_scope snapshot tables
	m.snapshotTables = map[string][]common.Table{}
	m.snapshotTables[m.params.ClusterID] = []common.Table{
		{
			Name: "edgex.apid_cluster",
			Rows: []common.Row{cluster},
		},
		{
			Name: "edgex.data_scope",
			Rows: dataScopes,
		},
	}

	var snapshotTableRows []tableRowMap

	// generate one company
	companyID := m.params.Organization
	tenantID := m.params.Organization
	changeSelector := m.params.Scope
	company := tableRowMap{
		"kms.company": m.newRow(map[string]string{
			"id": companyID,
			"status": "Active",
			"tenant_id": tenantID,
			"_change_selector": changeSelector,
			"name": companyID,
			"display_name": companyID,
		}),
	}
	snapshotTableRows = append(snapshotTableRows, company)

	// generate a bunch of developers
	for i := 0; i < m.params.NumDevelopers; i++ {
		developerID := generateUUID()
		devRows := m.createDeveloper(developerID)
		productID := generateUUID()
		productRows := m.createProduct(productID)
		appRows := m.createApplication(developerID, productID)

		developer := m.mergeTableRowMaps(devRows, productRows, appRows)
		snapshotTableRows = append(snapshotTableRows, developer)
	}

	m.snapshotTables[m.params.Scope] = m.concatTableRowMaps(snapshotTableRows...)
}

func (m *MockServer) registerRoutes(router apid.Router) {

	router.HandleFunc("/accesstoken", m.unreliable(m.sendToken)).Methods("POST")
	router.HandleFunc("/snapshots", m.unreliable(m.auth(m.sendSnapshot))).Methods("GET")
	router.HandleFunc("/changes", m.unreliable(m.auth(m.sendChanges))).Methods("GET")
}

func (m *MockServer) sendToken(w http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()
	m.registerFailHandler(w)

	Expect(req.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded; param=value"))

	err := req.ParseForm()
	Expect(err).NotTo(HaveOccurred())
	Expect(req.Form.Get("grant_type")).To(Equal("client_credentials"))
	Expect(req.Header.Get("status")).To(Equal("ONLINE"))
	Expect(req.Header.Get("apid_cluster_Id")).To(Equal(m.params.ClusterID))
	Expect(req.Header.Get("display_name")).ToNot(BeEmpty())

	Expect(req.Form.Get("client_id")).To(Equal(m.params.TokenKey))
	Expect(req.Form.Get("client_secret")).To(Equal(m.params.TokenSecret))

	var plugInfo []pluginDetail
	plInfo := []byte(req.Header.Get("plugin_details"))
	err = json.Unmarshal(plInfo, &plugInfo)
	Expect(err).NotTo(HaveOccurred())

	Expect(plugInfo[0].Name).To(Equal("apidApigeeSync"))
	Expect(plugInfo[0].SchemaVersion).NotTo(BeEmpty())

	m.oauthToken = generateUUID()
	res := oauthTokenResp{
		AccessToken: m.oauthToken,
	}
	body, err := json.Marshal(res)
	Expect(err).NotTo(HaveOccurred())
	w.Write(body)
}

func (m *MockServer) sendSnapshot(w http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()
	m.registerFailHandler(w)

	q := req.URL.Query()
	scopes := q["scope"]

	Expect(scopes).To(ContainElement(m.params.ClusterID))

	m.snapshotID = generateUUID()
	snapshot := &common.Snapshot{
		SnapshotInfo: m.snapshotID,
	}

	// Note: if/when we support multiple scopes, we'd have to do a merge of table rows
	for _, scope := range scopes {
		tables := m.snapshotTables[scope]
		for _, table := range tables {
			snapshot.AddTables(table)
		}
	}

	body, err := json.Marshal(snapshot)
	Expect(err).NotTo(HaveOccurred())

	w.Write(body)
}

// todo: does "since" have any value?
func (m *MockServer) sendChanges(w http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()
	m.registerFailHandler(w)

	q := req.URL.Query()
	scopes := q["scope"]
	block, err := strconv.Atoi(req.URL.Query().Get("block"))
	Expect(err).NotTo(HaveOccurred())
	since := req.URL.Query().Get("since")

	Expect(req.Header.Get("apid_cluster_Id")).To(Equal(m.params.ClusterID))
	Expect(q.Get("snapshot")).To(Equal(m.snapshotID))

	Expect(scopes).To(ContainElement(m.params.ClusterID))
	Expect(scopes).To(ContainElement(m.params.Scope))

	if block > 0 && since == m.sequenceID && since != "" {
		m.sendChange(w, time.Duration(block) * time.Second)
		return
	}

	// todo: This is just legacy for the existing test in apigeeSync_suite_test
	m.sequenceID = generateUUID()
	res := &common.ChangeList{
		LastSequence: m.sequenceID,
	}

	apidDataScopeRow := m.newRow(map[string]string{
		"id":              "apid_config_scope_id_1", // adding a new scope
		"scope":           m.params.Scope,
		"org":             m.params.Organization,
		"env":             m.params.Environment,
		"apid_cluster_id": m.params.ClusterID,
	})

	res.Changes = []common.Change{
		{
			Table:     "edgex.data_scope",
			Operation: common.Insert,
			NewRow:    apidDataScopeRow,
		},
	}

	body, err := json.Marshal(res)
	Expect(err).NotTo(HaveOccurred())

	w.Write(body)
}

func (m *MockServer) developerGenerator() {

	tick := time.Tick(m.params.AddDeveloperEvery)
	for range tick {

		// generate a random developer w/ product and app
		developerID := generateUUID()
		devRows := m.createDeveloper(developerID)
		productID := generateUUID()
		productRows := m.createProduct(productID)
		appRows := m.createApplication(developerID, productID)

		developer := m.mergeTableRowMaps(devRows, productRows, appRows)
		changeList := m.createInsertChange(developer)

		m.sequenceID = generateUUID()
		changeList.LastSequence = m.sequenceID

		body, err := json.Marshal(changeList)
		if err != nil {
			fmt.Printf("Error generating developer!\n%v\n", err)
		}

		fmt.Println("adding developer")
		fmt.Println(string(body))
		m.changeChannel <- body
	}
}

func (m *MockServer) sendChange(w http.ResponseWriter, timeout time.Duration) {
	select {
	case change := <-m.changeChannel:
		fmt.Println("sending change to client")
		w.Write(change)
	case <-time.After(timeout):
		fmt.Println("change request timeout")
		w.WriteHeader(http.StatusNotModified)
	}
}

// enforces handler auth
func (m *MockServer) auth(target http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		auth := req.Header.Get("Authorization")

		if auth != fmt.Sprintf("Bearer %s", m.oauthToken) {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			target(w, req)
		}
	}
}

// make a handler unreliable
func (m *MockServer) unreliable(target http.HandlerFunc) http.HandlerFunc {
	if m.params.ReliableAPI {
		return target
	}

	var fail bool
	return func(w http.ResponseWriter, req *http.Request) {
		fail = !fail
		if fail {
			w.WriteHeader(500)
		} else {
			target(w, req)
		}
	}
}

func (m *MockServer) registerFailHandler(w http.ResponseWriter) {
	RegisterFailHandler(func(message string, callerSkip ...int) {
		w.WriteHeader(400)
		w.Write([]byte(message))
		fmt.Printf("sending error: %#v\n", message)
		panic(GINKGO_PANIC)
	})
}


func (m *MockServer) newRow(keyAndVals map[string]string) (row common.Row) {

	row = common.Row{}
	for k, v := range keyAndVals {
		row[k] = &common.ColumnVal{
			Value: v,
			Type:  1,
		}
	}
	return
}

func (m *MockServer) createDeveloper(developerID string) tableRowMap {

	companyID := m.params.Organization
	tenantID := m.params.Organization
	changeSelector := m.params.Scope

	rows := tableRowMap{}

	rows["kms.developer"] = m.newRow(map[string]string{
		"id": developerID,
		"status": "Active",
		"tenant_id": tenantID,
		"_change_selector": changeSelector,
	})

	// map developer onto to existing company
	rows["kms.company_developer"] = m.newRow(map[string]string{
		"id": developerID,
		"tenant_id": tenantID,
		"_change_selector": changeSelector,
		"company_id": companyID,
		"developer_id": developerID,
	})

	return rows
}

func (m *MockServer) createProduct(productID string) tableRowMap {

	tenantID := m.params.Organization
	changeSelector := m.params.Scope

	rows := tableRowMap{}
	rows["kms.product"] = m.newRow(map[string]string{
		"id": productID,
		"api_resources": "{}",
		"environments": "{Env_0, Env_1}",
		"tenant_id": tenantID,
		"_change_selector": changeSelector,
	})
	return rows
}

func (m *MockServer) createApplication(developerID, productID string) tableRowMap {

	tenantID := m.params.Organization
	changeSelector := m.params.Scope

	rows := tableRowMap{}

	applicationID := generateUUID()
	credentialID := generateUUID()

	rows["kms.app"] = m.newRow(map[string]string{
		"id": applicationID,
		"developer_id": developerID,
		"status": "Approved",
		"tenant_id": tenantID,
		"_change_selector": changeSelector,
	})

	rows["kms.app_credential"] = m.newRow(map[string]string{
		"id": credentialID,
		"app_id": applicationID,
		"tenant_id": tenantID,
		"status": "Approved",
		"_change_selector": changeSelector,
	})

	rows["kms.app_credential_apiproduct_mapper"] = m.newRow(map[string]string{
		"apiprdt_id": productID,
		"app_id": applicationID,
		"appcred_id": credentialID,
		"status": "Approved",
		"_change_selector": changeSelector,
		"tenant_id": tenantID,
	})

	return rows
}

func (m *MockServer) createInsertChange(newRows tableRowMap) common.ChangeList {

	var changeList = common.ChangeList{}
	for table, row := range newRows {
		change := common.Change{
			Table: table,
			NewRow: row,
			Operation: common.Insert,
		}

		changeList.Changes = append(changeList.Changes, change)
	}
	return changeList
}

func (m *MockServer) createDeleteChange(oldRows tableRowMap) common.ChangeList {

	var changeList = common.ChangeList{}
	for table, row := range oldRows {
		change := common.Change{
			Table: table,
			OldRow: row,
			Operation: common.Delete,
		}

		changeList.Changes = append(changeList.Changes, change)
	}
	return changeList
}

func (m *MockServer) createUpdateChange(oldRows, newRows tableRowMap) common.ChangeList {

	var changeList = common.ChangeList{}
	for table, oldRow := range oldRows {
		change := common.Change{
			Table: table,
			OldRow: oldRow,
			NewRow: newRows[table],
			Operation: common.Update,
		}

		changeList.Changes = append(changeList.Changes, change)
	}
	return changeList
}

// create one tableRowMap from various tableRowMap - tables must be unique
func (m *MockServer) mergeTableRowMaps(maps ...tableRowMap) tableRowMap {
	merged := tableRowMap{}
	for _, m := range maps {
		for name, row := range m {
			if _, ok:= merged[name]; ok {
				panic(fmt.Sprintf("bad merge. name: %#v, row: %#v", name, row))
			}
			merged[name] = row
		}
	}
	return merged
}

// create []common.Table from array of tableRowMaps
func (m *MockServer) concatTableRowMaps(maps ...tableRowMap) []common.Table {
	tableMap := map[string]*common.Table{}
	for _, m := range maps {
		for name, row := range m {
			if _, ok:= tableMap[name]; !ok {
				tableMap[name] = &common.Table{
					Name: name,
				}
			}
			tableMap[name].AddRowstoTable(row)
		}
	}
	result := []common.Table{}
	for _, v := range tableMap {
		result = append(result, *v)
	}
	return result
}
