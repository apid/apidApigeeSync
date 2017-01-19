package apidApigeeSync_test;

import (
	"testing"
	"github.com/30x/apid"
	"github.com/30x/apidApigeeSync"
	"github.com/30x/apid/config"
	"github.com/30x/apid/logger"
	"os"
)

func TestInitDefaultsDefaultHostname(t *testing.T) {
	var cs apid.ConfigService
	cs = config.GetConfig()
	apidApigeeSync.SetLogger(logger.NewLogger("init_test.go","DEBUG"))
	apidApigeeSync.InitDefaults(cs)
	name, _ := os.Hostname()
	if cs.Get("apigeesync_instance_name") != name {
		t.Errorf("got %s, expected %s", cs.Get("apigeesync_instance_name"),name)
	}
}
func TestInitDefaultsNameFromFile(t *testing.T) {
	var cs apid.ConfigService
	cs = config.GetConfig()
	cs.Set("apigeesync_instance_name","myname")
	apidApigeeSync.SetLogger(logger.NewLogger("init_test.go","DEBUG"))
	apidApigeeSync.InitDefaults(cs)
	if cs.Get("apigeesync_instance_name") != "myname" {
		t.Errorf("got %s, expected %s", cs.Get("apigeesync_instance_name"),"myname")
	}
}
