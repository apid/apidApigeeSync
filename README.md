# apidApigeeSync

This core plugin for [apid](http://github.com/30x/apid) connects to the Apigee Change Agent and publishes the data
changes events onto the apid Event service.

### Configuration

#### apigeesync_poll_interval

int. seconds. default: 5

#### apigeesync_organization

string. name. required.

#### apigeesync_proxy_server_base

string. url. required.

#### apigeesync_consumer_key

string. required.

#### apigeesync_consumer_secret

string. required.


### Event

Selector:

    apidApigeeSync.ApigeeSyncEventSelector


Data:

See: [payload.go](payload.go)
