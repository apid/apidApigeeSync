# apidApigeeSync Mock Server

## Overview

This Mock Server is used during unit tests of apidApigeeSync and has been designed to be run standalone
for stand-alone development use as well as performance and load testing. 

## Build

From the apidApigeeSync base dir: 
 
    glide install

From apidApigeeSync/cmd/mockServer:

    go build
    
You should now have an executable named "mockServer". 

## Execute

Execute with the -h flag to see flags: 

    ./mockServer -h
    Usage of ./mockServer:
      -addDevEach duration
            add a developer each duration (default 0s)
      -bundleURI string
            a URI to a valid deployment bundle (default '')
      -numDeps int
            number of deployments in snapshot (default 2)
      -numDevs int
            number of developers in snapshot (default 2)
      -reliable
            if false, server will often send 500 errors (default true)
      -upDepEach duration
            update (replace) a deployment each duration (default 0s)
      -upDevEach duration
            update a developer each duration (default 0s)         

Note: Nothing is required. 

The following are the values used by default by the Mock Server: 

    ReliableAPI: true
    ClusterID: "cluster"
    TokenKey: "key"
    TokenSecret: "secret"
    Scope: "scope"
    Organization: "org"
    Environment: "test"
    NumDevelopers: 2
    AddDeveloperEvery: 0
    UpdateDeveloperEvery: 0
    NumDeployments: 2
    ReplaceDeploymentEvery: 0
    Port: 9001

## Put it to use

Set your apid configuration to point toward the Mock Server and have correct cluster, key, and secret values. 

For example:

    api_port: 9000
    api_expvar_path: /expvar
    events_buffer_size: 5
    log_level: debug
    apigeesync_proxy_server_base: http://localhost:9001
    apigeesync_snapshot_server_base: http://localhost:9001
    apigeesync_change_server_base: http://localhost:9001
    apigeesync_consumer_key: key
    apigeesync_consumer_secret: secret
    apigeesync_cluster_id: cluster
    #data_trace_log_level: debug
    data_source: file:%s?_busy_timeout=20000

Now start apid. It should download the snapshot and changes as you configured for the Mock Server.

Try out a couple of APIs to verify:

    curl -i -d "action=verify&key=1&uriPath=/&scopeuuid=scope" :9000/verifiers/apikey
    
    curl -i :9000/deployments

## Notes

Under high loads (eg. a large snapshot), apid may get timeout errors from sqlite. 
If you see this, you can work around it by increasing the _busy_timeout by adding a config item to your apid config:

    data_source: file:%s?_busy_timeout=10000

The _busy_timeout value is in milliseconds, so the above value is 10s.
