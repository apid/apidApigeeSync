package apidApigeeSync

import (
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"strconv"
	"time"
)

var _ = Describe("listener", func() {

	It("should bootstrap from local DB if present", func(done Done) {

		expectedTables := make(map[string]bool)
		expectedTables["kms.company"] = true
		expectedTables["edgex.apid_cluster"] = true
		expectedTables["edgex.data_scope"] = true

		Expect(apidInfo.LastSnapshot).NotTo(BeEmpty())

		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
			defer GinkgoRecover()

			if s, ok := event.(*common.Snapshot); ok {

				//verify that the knownTables array has been properly populated from existing DB
				Expect(mapIsSubset(knownTables, expectedTables)).To(BeTrue())

				Expect(s.SnapshotInfo).Should(Equal(apidInfo.LastSnapshot))
				Expect(s.Tables).To(BeNil())

				close(done)
			}
		})

		bootstrap()
	})

	It("should correctly identify non-proper subsets with respect to maps", func() {

		//test b proper subset of a
		Expect(mapIsSubset(map[string]bool{"a": true, "b": true}, map[string]bool{"b": true})).To(BeTrue())

		//test a == b
		Expect(mapIsSubset(map[string]bool{"a": true, "b": true}, map[string]bool{"a": true, "b": true})).To(BeTrue())

		//test b superset of a
		Expect(mapIsSubset(map[string]bool{"a": true, "b": true}, map[string]bool{"a": true, "b": true, "c": true})).To(BeFalse())

		//test b not subset of a
		Expect(mapIsSubset(map[string]bool{"a": true, "b": true}, map[string]bool{"c": true})).To(BeFalse())

		//test b empty
		Expect(mapIsSubset(map[string]bool{"a": true, "b": true}, map[string]bool{})).To(BeTrue())

		//test a empty
		Expect(mapIsSubset(map[string]bool{}, map[string]bool{"b": true})).To(BeFalse())
	})

	// todo: disabled for now -
	// there is precondition I haven't been able to track down that breaks this test on occasion
	XIt("should process a new snapshot when change server requires it", func(done Done) {
		oldSnap := apidInfo.LastSnapshot
		apid.Events().ListenFunc(ApigeeSyncEventSelector, func(event apid.Event) {
			defer GinkgoRecover()

			if s, ok := event.(*common.Snapshot); ok {
				Expect(s.SnapshotInfo).NotTo(Equal(oldSnap))
				close(done)
			}
		})
		testMock.forceNewSnapshot()
	})

	It("Verify the Sequence Number Logic works as expected", func() {
		Expect(getChangeStatus("1.1.1", "1.1.2")).To(Equal(1))
		Expect(getChangeStatus("1.1.1", "1.2.1")).To(Equal(1))
		Expect(getChangeStatus("1.2.1", "1.2.1")).To(Equal(0))
		Expect(getChangeStatus("1.2.1", "1.2.2")).To(Equal(1))
		Expect(getChangeStatus("2.2.1", "1.2.2")).To(Equal(-1))
		Expect(getChangeStatus("2.2.1", "2.2.0")).To(Equal(-1))
	})

	/*
	 * XAPID-869, there should not be any panic if received duplicate snapshots during bootstrap
	 */
	It("Should be able to handle duplicate snapshot during bootstrap", func() {
		scopes := []string{apidInfo.ClusterID}
		snapshot := downloadSnapshot(scopes)
		storeBootSnapshot(snapshot)
		storeDataSnapshot(snapshot)
	})

	/*
	 * in-mem cache test
	 */
	It("Test In-mem cache", func() {
		testCache := &DatascopeCache{requestChan: make(chan *cacheOperationRequest), readDoneChan: make(chan []string)}
		go testCache.datascopeCacheManager()
		testCache.clearAndInitCache("test-version")
		countChan := make(chan int)
		base := 10
		rand.Seed(time.Now().Unix())
		num := base + rand.Intn(base)
		scopeMap := make(map[string]bool)
		// async update
		for i := 0; i < num; i++ {
			id := strconv.Itoa(i)
			scopeStr := strconv.Itoa(i % base)
			scope := &dataDataScope{ID: id, Scope: scopeStr}
			scopeMap[scope.Scope] = true
			go func(scope *dataDataScope) {
				testCache.updateCache(scope)
				countChan <- 1
			}(scope)
		}

		// wait until update done
		for i := 0; i < num; i++ {
			<-countChan
		}

		// verify update
		retrievedScopes := testCache.readAllScope()
		Expect(len(scopeMap)).To(Equal(len(retrievedScopes)))
		for _, s := range retrievedScopes {
			// verify each retrieved scope is valid
			Expect(scopeMap[s]).To(BeTrue())
			// no duplicate scopes
			scopeMap[s] = true
		}

		// remove all the datascopes with odd scope
		count := 0
		for i := 0; i < num; i++ {
			if (i%base)%2 == 1 {
				count += 1
				id := strconv.Itoa(i)
				scopeStr := strconv.Itoa(i % base)
				scope := &dataDataScope{ID: id, Scope: scopeStr}
				go func(scope *dataDataScope) {
					testCache.removeCache(scope)
					countChan <- 1
				}(scope)
			}
		}

		for i := 0; i < count; i++ {
			<-countChan
		}

		// all retrieved scopes should be even
		retrievedScopes = testCache.readAllScope()
		for _, s := range retrievedScopes {
			scopeNum, _ := strconv.Atoi(s)
			Expect(scopeNum % 2).To(BeZero())
		}

		// async remove all datascopes
		for i := 0; i < num; i++ {
			id := strconv.Itoa(i)
			scopeStr := strconv.Itoa(i % base)
			scope := &dataDataScope{ID: id, Scope: scopeStr}
			go func(scope *dataDataScope) {
				testCache.removeCache(scope)
				countChan <- 1
			}(scope)
		}

		for i := 0; i < num; i++ {
			<-countChan
		}
		retrievedScopes = testCache.readAllScope()
		Expect(len(retrievedScopes)).To(Equal(0))
	})

})
