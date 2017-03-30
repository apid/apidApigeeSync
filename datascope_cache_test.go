package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"strconv"
	"time"
)

var _ = Describe("datascope cache", func() {
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

		testCache.closeCache()
	})

})
