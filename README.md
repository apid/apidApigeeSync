# apidApigeeSync

[![Build Status](https://travis-ci.org/apid/apidApigeeSync.svg)](https://travis-ci.org/apid/apidApigeeSync) [![GoDoc](https://godoc.org/github.com/apid/apidApigeeSync?status.svg)](https://godoc.org/github.com/apid/apidApigeeSync) [![Go Report Card](https://goreportcard.com/badge/github.com/apid/apidApigeeSync)](https://goreportcard.com/report/github.com/apid/apidApigeeSync)

This core plugin for [apid](http://github.com/apid/apid) connects to the Apigee Change Agent and publishes the data
changes events onto the apid Event service. It also coordinates DB initialization for plugins on startup.

### Configuration

| name                         | description              |
|------------------------------|--------------------------|
| apigeesync_poll_interval     | int. seconds. default: 5 |
| apigeesync_proxy_server_base | string. url. required.   |
| apigeesync_consumer_key      | string. required.        |
| apigeesync_consumer_secret   | string. required.        |
| apigeesync_instance_name     | string. optional. Display Name for UI        |

This plugin also populates a configuration item for dependant plugins that may need it:

| name                         | description              |
|------------------------------|--------------------------|
| apigeesync_apid_instance_id  | string                   |

### Event Generated

* Selector: "ApigeeSync"
* Data: [payload.go](payload.go)

### Startup Procedure

#### ApigeeSync
1. Read DB version (Snapshot.SnapshotInfo) from default DB
2. If version found, emit Snapshot event (using Snapshot.SnapshotInfo, no data)
3. Ask server for Snapshot
4. Each time a Snapshot is received
    1. Verify Snapshot.SnapshotInfo is different than current
    2. Stop processing change events
    3. Remove or clean new DB version if it exists
    4. Emit Snapshot event
    5. Wait for plugins to finish processing
    6. Save Snapshot.SnapshotInfo in default DB
    7. Release old DB version
    8. Start processing change events

#### ApigeeSync-dependent plugins
1. Initialization
    1. Until receiving first Snapshot message, ApigeeSync-dependent APIs must either:
         1. not register (endpoint will return a 404 by default) 
         2. return a 503 until DB is initialized
2. Upon receiving a snapshot notification (this is a HOT DB upgrade)
    1. Get DB for version (use Snapshot.SnapshotInfo as version)
    2. Create plugin's tables, if needed
    3. Insert any snapshot data into plugin's tables
    4. Set reference to new DB for all data access
    5. If db-dependent services are not exposed yet, expose them

Example plugin code:

    var (
      log      apid.LogService   // set in initPlugin
      data     apid.DataService
      unsafeDB apid.DB
      dbMux    sync.RWMutex
    )
    
    func init() {
      apid.RegisterPlugin(initPlugin)
    }
    
    func initPlugin(services apid.Services) error {
      log = services.Log().ForModule("examplePlugin")
      log.Debug("start init")
      data = services.Data()
      return nil
    }
    
    // always use getDB() to safely access current DB
    func getDB() apid.DB {
      dbMux.RLock()
      db := unsafeDB
      dbMux.RUnlock()
      return db
    }
    
    func setDB(db apid.DB) {
      dbMux.Lock()
      if unsafeDB == nil { // init API on DB initialization (optional)
        go initAPI()
      }
      unsafeDB = db
      dbMux.Unlock()
    }

    func processSnapshot(snapshot *common.Snapshot) {
    
      log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)
    
      db, err := data.DBVersion(snapshot.SnapshotInfo)
      if err != nil {
        log.Panicf("Unable to access database: %v", err)
      }

      // init DB as needed (note: DB may exist, use 'CREATE TABLE IF NOT EXISTS' if not explicitly checking)
      initDB(db)
    
      for _, table := range snapshot.Tables {
        // populate tables from snapshot...
      }
    
      // switch to new database 
      setDB(db)
      log.Debug("Snapshot processed")
    }

### Docker Test:
Make sure you have transicator in your $GOPATH/src/github.com/apigee-labs/transicator before you run docker tests.

    make dockertest

This docker test assumes:

If you're running it on Macbook, your docker host is localhost.
If you're running it on Goobuntu, your docker ip is 192.168.9.1 (if you follow go/installdocker).



