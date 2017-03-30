package apidApigeeSync

const (
	readCache int = iota
	updateCache
	removeCache
	clearAndInit
)

/*
 * structs for DatascopeCache
 */

type cacheOperationRequest struct {
	Operation int
	Scope     *dataDataScope
	version   string
}

// maintain an in-mem cache of datascope
type DatascopeCache struct {
	requestChan  chan *cacheOperationRequest
	readDoneChan chan []string
	scopeMap     map[string]*dataDataScope
	version      string
}

var scopeCache *DatascopeCache

func (cache *DatascopeCache) datascopeCacheManager() {
	for request := range cache.requestChan {
		switch request.Operation {
		case readCache:
			log.Debug("datascopeCacheManager: readCache")
			scopes := make([]string, 0, len(cache.scopeMap))
			for _, ds := range cache.scopeMap {
				scopes = append(scopes, ds.Scope)
			}
			cache.readDoneChan <- scopes
		case updateCache:
			log.Debug("datascopeCacheManager: updateCache")
			cache.scopeMap[request.Scope.ID] = request.Scope
		case removeCache:
			log.Debug("datascopeCacheManager: removeCache")
			delete(cache.scopeMap, request.Scope.ID)
		case clearAndInit:
			log.Debug("datascopeCacheManager: clearAndInit")
			if cache.version != request.version {
				cache.scopeMap = make(map[string]*dataDataScope)
				cache.version = request.version
			}
		}
	}

	//chan closed
	cache.scopeMap = nil
	close(cache.requestChan)
}

/*
 * The output of readAllScope() should be identical to findScopesForId(apidInfo.ClusterID)
 */

func (cache *DatascopeCache) readAllScope() []string {
	cache.requestChan <- &cacheOperationRequest{readCache, nil, ""}
	scopes := <-cache.readDoneChan
	// eliminate duplicates
	tmpMap := make(map[string]bool)
	for _, scope := range scopes {
		tmpMap[scope] = true
	}
	scopes = make([]string, 0)
	for scope := range tmpMap {
		scopes = append(scopes, scope)
	}
	return scopes
}

func (cache *DatascopeCache) removeCache(scope *dataDataScope) {
	cache.requestChan <- &cacheOperationRequest{removeCache, scope, ""}
}

func (cache *DatascopeCache) updateCache(scope *dataDataScope) {
	cache.requestChan <- &cacheOperationRequest{updateCache, scope, ""}
}

func (cache *DatascopeCache) clearAndInitCache(version string) {
	cache.requestChan <- &cacheOperationRequest{clearAndInit, nil, version}
}

func (cache *DatascopeCache) closeCache() {
	close(cache.requestChan)
}
