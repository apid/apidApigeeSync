package apidApigeeSync

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"net"

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*
Currently limited to:
  1 cluster, 1 scope, 1 org, 1 env, 1 company
  1 app & 1 product per developer

Notes:
  Scope ~= org + env
  tenant_id == Scope for our purposes
  (technically, data_scope.scope = tenant_id)

Relations:
  company => * developer
  developer => * app
  application => * app_credential
  product => * app_credential
*/

const oauthExpiresIn = 2 * 60 // 2 minutes

type MockParms struct {
	ReliableAPI            bool
	ClusterID              string
	TokenKey               string
	TokenSecret            string
	Scope                  string
	Organization           string
	Environment            string
	NumDevelopers          int
	AddDeveloperEvery      time.Duration
	UpdateDeveloperEvery   time.Duration
	NumDeployments         int
	ReplaceDeploymentEvery time.Duration
	BundleURI              string
}

func Mock(params MockParms, router apid.Router) *MockServer {
	m := &MockServer{}
	m.params = params

	m.init()
	m.registerRoutes(router)
	return m
}

// table name -> common.Row
type tableRowMap map[string]common.Row

type MockServer struct {
	params          MockParms
	oauthToken      string
	snapshotID      string
	snapshotTables  map[string][]common.Table // key = scopeID
	changeChannel   chan []byte
	sequenceID      *int64
	maxDevID        *int64
	deployIDMutex   sync.RWMutex
	minDeploymentID *int64
	maxDeploymentID *int64
	newSnap         *int32
}

func (m *MockServer) forceNewSnapshot() {
	atomic.SwapInt32(m.newSnap, 1)
}

func (m *MockServer) lastSequenceID() string {
	return strconv.FormatInt(atomic.LoadInt64(m.sequenceID), 10)
}

func (m *MockServer) nextSequenceID() string {
	return strconv.FormatInt(atomic.AddInt64(m.sequenceID, 1), 10)
}

func (m *MockServer) nextDeveloperID() string {
	return strconv.FormatInt(atomic.AddInt64(m.maxDevID, 1), 10)
}

func (m *MockServer) randomDeveloperID() string {
	return strconv.FormatInt(rand.Int63n(atomic.LoadInt64(m.maxDevID)), 10)
}

func (m *MockServer) nextDeploymentID() string {
	return strconv.FormatInt(atomic.AddInt64(m.maxDeploymentID, 1), 10)
}

func (m *MockServer) popDeploymentID() string {
	newMinID := atomic.AddInt64(m.minDeploymentID, 1)
	return strconv.FormatInt(newMinID-1, 10)
}

func (m *MockServer) init() {
	defer GinkgoRecover()
	RegisterFailHandler(func(message string, callerSkip ...int) {
		log.Errorf("Expect error: %s", message)
		panic(message)
	})

	m.sequenceID = new(int64)
	m.maxDevID = new(int64)
	m.changeChannel = make(chan []byte)
	m.minDeploymentID = new(int64)
	*m.minDeploymentID = 1
	m.maxDeploymentID = new(int64)
	m.newSnap = new(int32)

	go m.developerGenerator()
	go m.developerUpdater()
	go m.deploymentReplacer()

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
	tenantID := m.params.Scope
	changeSelector := m.params.Scope
	company := tableRowMap{
		"kms.company": m.newRow(map[string]string{
			"id":               companyID,
			"status":           "Active",
			"tenant_id":        tenantID,
			"name":             companyID,
			"display_name":     companyID,
			"_change_selector": changeSelector,
		}),
	}
	snapshotTableRows = append(snapshotTableRows, company)

	// generate snapshot developers
	for i := 0; i < m.params.NumDevelopers; i++ {
		developer := m.createDeveloperWithProductAndApp()
		snapshotTableRows = append(snapshotTableRows, developer)
	}
	log.Infof("created %d developers", m.params.NumDevelopers)

	// generate snapshot deployments
	for i := 0; i < m.params.NumDeployments; i++ {
		deployment := m.createDeployment()
		snapshotTableRows = append(snapshotTableRows, deployment)
	}
	log.Infof("created %d deployments", m.params.NumDeployments)

	m.snapshotTables[m.params.Scope] = m.concatTableRowMaps(snapshotTableRows...)

	if m.params.NumDevelopers < 10 && m.params.NumDeployments < 10 {
		log.Debugf("snapshotTables: %v", m.snapshotTables[m.params.Scope])
	}
}

// developer, product, application, credential will have the same ID (developerID)
func (m *MockServer) createDeveloperWithProductAndApp() tableRowMap {

	developerID := m.nextDeveloperID()

	devRows := m.createDeveloper(developerID)
	productRows := m.createProduct(developerID)
	appRows := m.createApplication(developerID, developerID, developerID, developerID)

	return m.mergeTableRowMaps(devRows, productRows, appRows)
}

func (m *MockServer) registerRoutes(router apid.Router) {

	router.HandleFunc("/accesstoken", m.unreliable(m.gomega(m.sendToken))).Methods("POST")
	router.HandleFunc("/snapshots", m.unreliable(m.gomega(m.auth(m.sendSnapshot)))).Methods("GET")
	router.HandleFunc("/changes", m.unreliable(m.gomega(m.auth(m.sendChanges)))).Methods("GET")
	router.HandleFunc("/bundles/{id}", m.sendDeploymentBundle).Methods("GET")
	router.HandleFunc("/analytics", m.sendAnalyticsURL).Methods("GET")
	router.HandleFunc("/analytics", m.putAnalyticsData).Methods("PUT")
}

func (m *MockServer) sendAnalyticsURL(w http.ResponseWriter, req *http.Request) {
	uri := fmt.Sprintf("http://%s%s", req.Host, req.RequestURI)
	w.Write([]byte(fmt.Sprintf("{ \"url\": \"%s\" }", uri)))
}

func (m *MockServer) putAnalyticsData(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
}

func (m *MockServer) sendDeploymentBundle(w http.ResponseWriter, req *http.Request) {
	vars := apid.API().Vars(req)
	w.Write([]byte("/bundles/" + vars["id"]))
}

func (m *MockServer) sendToken(w http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()

	Expect(req.Header.Get("Content-Type")).To(Equal("application/x-www-form-urlencoded; param=value"))

	err := req.ParseForm()
	Expect(err).NotTo(HaveOccurred())

	Expect(req.Form.Get("grant_type")).To(Equal("client_credentials"))
	Expect(req.Header.Get("status")).To(Equal("ONLINE"))
	Expect(req.Header.Get("apid_cluster_Id")).To(Equal(m.params.ClusterID))
	Expect(req.Header.Get("display_name")).ToNot(BeEmpty())

	if req.Header.Get("created_at_apid") != "" {
		Expect(req.Header.Get("updated_at_apid")).To(BeEmpty())
	} else {
		Expect(req.Header.Get("updated_at_apid")).ToNot(BeEmpty())
	}

	Expect(req.Form.Get("client_id")).To(Equal(m.params.TokenKey))
	Expect(req.Form.Get("client_secret")).To(Equal(m.params.TokenSecret))

	var plugInfo []pluginDetail
	plInfo := []byte(req.Header.Get("plugin_details"))
	err = json.Unmarshal(plInfo, &plugInfo)
	Expect(err).NotTo(HaveOccurred())

	m.oauthToken = generateUUID()
	res := oauthToken{
		AccessToken: m.oauthToken,
		ExpiresIn:   oauthExpiresIn,
	}
	body, err := json.Marshal(res)
	Expect(err).NotTo(HaveOccurred())
	w.Write(body)
}

func (m *MockServer) sendSnapshot(w http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()

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

	log.Infof("sending snapshot: %s", m.snapshotID)
	if len(body) < 10000 {
		log.Debugf("snapshot: %#v", string(body))
	}

	w.Write(body)
}

func (m *MockServer) sendChanges(w http.ResponseWriter, req *http.Request) {
	defer GinkgoRecover()

	val := atomic.SwapInt32(m.newSnap, 0)
	if val > 0 {
		w.WriteHeader(http.StatusBadRequest)
		apiErr := changeServerError{
			Code: "SNAPSHOT_TOO_OLD",
		}
		bytes, err := json.Marshal(apiErr)
		Expect(err).NotTo(HaveOccurred())
		w.Write(bytes)
		return
	}

	q := req.URL.Query()

	scopes := q["scope"]
	block, err := strconv.Atoi(q.Get("block"))
	Expect(err).NotTo(HaveOccurred())
	since := q.Get("since")

	Expect(req.Header.Get("apid_cluster_Id")).To(Equal(m.params.ClusterID))
	//Expect(q.Get("snapshot")).To(Equal(m.snapshotID))

	Expect(scopes).To(ContainElement(m.params.ClusterID))
	//Expect(scopes).To(ContainElement(m.params.Scope))

	if since != "" {
		m.sendChange(w, time.Duration(block)*time.Second)
		return
	}

	// todo: the following is just legacy for the existing test in apigeeSync_suite_test
	developer := m.createDeveloperWithProductAndApp()
	changeList := m.createInsertChange(developer)
	body, err := json.Marshal(changeList)
	if err != nil {
		log.Errorf("Error generating developer: %v", err)
	}
	w.Write(body)
}

// generate developers w/ product and app
func (m *MockServer) developerGenerator() {

	for range time.Tick(m.params.AddDeveloperEvery) {

		developer := m.createDeveloperWithProductAndApp()
		changeList := m.createInsertChange(developer)

		body, err := json.Marshal(changeList)
		if err != nil {
			log.Errorf("Error adding developer: %v", err)
		}

		log.Info("adding developer")
		log.Debugf("body: %#v", string(body))
		m.changeChannel <- body
	}
}

// update random developers - set username
func (m *MockServer) developerUpdater() {

	for range time.Tick(m.params.UpdateDeveloperEvery) {

		developerID := m.randomDeveloperID()

		oldDev := m.createDeveloper(developerID)
		delete(oldDev, "kms.company_developer")
		newDev := m.createDeveloper(developerID)
		delete(newDev, "kms.company_developer")

		newRow := newDev["kms.developer"]
		newRow["username"] = m.stringColumnVal("i_am_not_a_number")

		changeList := m.createUpdateChange(oldDev, newDev)

		body, err := json.Marshal(changeList)
		if err != nil {
			log.Errorf("Error updating developer: %v", err)
		}

		log.Info("updating developer")
		log.Debugf("body: %#v", string(body))
		m.changeChannel <- body
	}
}

func (m *MockServer) deploymentReplacer() {

	for range time.Tick(m.params.ReplaceDeploymentEvery) {

		// delete
		oldDep := tableRowMap{}
		oldDep["edgex.deployment"] = m.newRow(map[string]string{
			"id": m.popDeploymentID(),
		})
		deleteChange := m.createDeleteChange(oldDep)

		// insert
		newDep := m.createDeployment()
		insertChange := m.createInsertChange(newDep)

		changeList := m.concatChangeLists(deleteChange, insertChange)

		body, err := json.Marshal(changeList)
		if err != nil {
			log.Errorf("Error replacing deployment: %v", err)
		}

		log.Info("replacing deployment")
		log.Debugf("body: %#v", string(body))
		m.changeChannel <- body
	}
}

// todo: we could debounce this if necessary
func (m *MockServer) sendChange(w http.ResponseWriter, timeout time.Duration) {
	select {
	case change := <-m.changeChannel:
		log.Info("sending change to client")
		w.Write(change)
	case <-time.After(timeout):
		log.Info("change request timeout")
		w.WriteHeader(http.StatusNotModified)
	}
}

// enables GoMega handling
func (m *MockServer) gomega(target http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		errors := InterceptGomegaFailures(func() {
			target(w, req)
		})
		if len(errors) > 0 {
			log.Errorf("assertion errors for %s:\nheaders:%v\n%v", req.URL, req.Header, errors)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("assertion errors:\n%v", errors)))
		}
	}
}

// enforces handler auth
func (m *MockServer) auth(target http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		auth := req.Header.Get("Authorization")

		expectedAuth := fmt.Sprintf("Bearer %s", m.oauthToken)
		if auth != expectedAuth {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("Bad auth token. Is: %s, should be: %s", auth, expectedAuth)))
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

func (m *MockServer) newRow(keyAndVals map[string]string) (row common.Row) {

	row = common.Row{}
	for k, v := range keyAndVals {
		row[k] = m.stringColumnVal(v)
	}

	// todo: remove this once apidVerifyAPIKey can deal with not having the field
	row["_change_selector"] = m.stringColumnVal(m.params.Scope)

	return
}

func (m *MockServer) stringColumnVal(v string) *common.ColumnVal {
	return &common.ColumnVal{
		Value: v,
		Type:  1,
	}
}

func (m *MockServer) createDeployment() tableRowMap {

	deploymentID := m.nextDeploymentID()
	bundleID := generateUUID()

	listen := apid.Config().GetString("api_listen")
	_, port, err := net.SplitHostPort(listen)
	Expect(err).NotTo(HaveOccurred())

	urlString := m.params.BundleURI
	if urlString == "" {
		urlString = fmt.Sprintf("http://localhost:%s/bundles/%s", port, bundleID)
	}

	uri, err := url.Parse(urlString)
	Expect(err).NotTo(HaveOccurred())
	hashWriter := crc32.NewIEEE()
	hashWriter.Write([]byte(uri.Path))
	checkSum := hex.EncodeToString(hashWriter.Sum(nil))

	type bundleConfigJson struct {
		Name         string `json:"name"`
		URI          string `json:"uri"`
		ChecksumType string `json:"checksumType"`
		Checksum     string `json:"checksum"`
	}

	bundleJson, err := json.Marshal(bundleConfigJson{
		Name:         uri.Path,
		URI:          urlString,
		ChecksumType: "crc-32",
		Checksum:     checkSum,
	})
	Expect(err).ShouldNot(HaveOccurred())

	rows := tableRowMap{}
	rows["edgex.deployment"] = m.newRow(map[string]string{
		"id":                 deploymentID,
		"bundle_config_id":   bundleID,
		"apid_cluster_id":    m.params.ClusterID,
		"data_scope_id":      m.params.Scope,
		"bundle_config_json": string(bundleJson),
		"config_json":        "{}",
	})

	return rows
}

func (m *MockServer) createDeveloper(developerID string) tableRowMap {

	companyID := m.params.Organization
	tenantID := m.params.Scope

	rows := tableRowMap{}

	rows["kms.developer"] = m.newRow(map[string]string{
		"id":        developerID,
		"status":    "Active",
		"tenant_id": tenantID,
	})

	// map developer onto to existing company
	rows["kms.company_developer"] = m.newRow(map[string]string{
		"id":           developerID,
		"tenant_id":    tenantID,
		"company_id":   companyID,
		"developer_id": developerID,
	})

	return rows
}

func (m *MockServer) createProduct(productID string) tableRowMap {

	tenantID := m.params.Scope

	environments := fmt.Sprintf("{%s}", m.params.Environment)
	resources := fmt.Sprintf("{%s}", "/") // todo: what should be here?

	rows := tableRowMap{}
	rows["kms.api_product"] = m.newRow(map[string]string{
		"id":            productID,
		"api_resources": resources,
		"environments":  environments,
		"tenant_id":     tenantID,
	})
	return rows
}

func (m *MockServer) createApplication(developerID, productID, applicationID, credentialID string) tableRowMap {

	tenantID := m.params.Scope

	rows := tableRowMap{}

	rows["kms.app"] = m.newRow(map[string]string{
		"id":           applicationID,
		"developer_id": developerID,
		"status":       "Approved",
		"tenant_id":    tenantID,
	})

	rows["kms.app_credential"] = m.newRow(map[string]string{
		"id":        credentialID,
		"app_id":    applicationID,
		"tenant_id": tenantID,
		"status":    "Approved",
	})

	rows["kms.app_credential_apiproduct_mapper"] = m.newRow(map[string]string{
		"apiprdt_id": productID,
		"app_id":     applicationID,
		"appcred_id": credentialID,
		"status":     "Approved",
		"tenant_id":  tenantID,
	})

	return rows
}

func (m *MockServer) createInsertChange(newRows tableRowMap) common.ChangeList {

	var changeList = common.ChangeList{}
	changeList.FirstSequence = m.lastSequenceID()
	changeList.LastSequence = m.nextSequenceID()
	for table, row := range newRows {
		change := common.Change{
			Table:     table,
			NewRow:    row,
			Operation: common.Insert,
		}

		changeList.Changes = append(changeList.Changes, change)
	}
	return changeList
}

func (m *MockServer) createDeleteChange(oldRows tableRowMap) common.ChangeList {

	var changeList = common.ChangeList{}
	changeList.FirstSequence = m.lastSequenceID()
	changeList.LastSequence = m.nextSequenceID()
	for table, row := range oldRows {
		change := common.Change{
			Table:     table,
			OldRow:    row,
			Operation: common.Delete,
		}

		changeList.Changes = append(changeList.Changes, change)
	}
	return changeList
}

func (m *MockServer) createUpdateChange(oldRows, newRows tableRowMap) common.ChangeList {

	var changeList = common.ChangeList{}
	changeList.FirstSequence = m.lastSequenceID()
	changeList.LastSequence = m.nextSequenceID()
	for table, oldRow := range oldRows {
		change := common.Change{
			Table:     table,
			OldRow:    oldRow,
			NewRow:    newRows[table],
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
			if _, ok := merged[name]; ok {
				panic(fmt.Sprintf("overwrite. name: %#v, row: %#v", name, row))
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
			if _, ok := tableMap[name]; !ok {
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

// create []common.Table from array of tableRowMaps
func (m *MockServer) concatChangeLists(changeLists ...common.ChangeList) common.ChangeList {
	result := common.ChangeList{}
	if len(changeLists) > 0 {
		result.FirstSequence = changeLists[0].FirstSequence
		result.LastSequence = changeLists[len(changeLists)-1].LastSequence
	}
	for _, cl := range changeLists {
		for _, c := range cl.Changes {
			result.Changes = append(result.Changes, c)
		}
	}
	return result
}
