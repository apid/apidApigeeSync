package main

import (
	"flag"

	"os"

	"time"

	"github.com/30x/apid-core"
	"github.com/30x/apid-core/factory"
	"github.com/30x/apidApigeeSync"
)

// runs a mock server standalone
func main() {
	// create new flag to avoid displaying all the Ginkgo flags
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	bundleURI := f.String("bundleURI", "", "a URI to a valid deployment bundle (default '')")

	reliable := f.Bool("reliable", true, "if false, server will often send 500 errors")

	numDevs := f.Int("numDevs", 2, "number of developers in snapshot")
	addDevEach := f.Duration("addDevEach", 0*time.Second, "add a developer each duration (default 0s)")
	upDevEach := f.Duration("upDevEach", 0*time.Second, "update a developer each duration (default 0s)")

	numDeps := f.Int("numDeps", 2, "number of deployments in snapshot")
	upDepEach := f.Duration("upDepEach", 0*time.Second, "update (replace) a deployment each duration (default 0s)")

	f.Parse(os.Args[1:])

	// set listener binding before apid.Initialize()
	os.Setenv("APID_API_LISTEN", ":9001")

	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()
	log.Debug("initializing...")
	apidApigeeSync.SetLogger(log)

	config := apid.Config()
	router := apid.API().Router()

	params := apidApigeeSync.MockParms{
		ReliableAPI:            *reliable,
		ClusterID:              "cluster",
		TokenKey:               "key",
		TokenSecret:            "secret",
		Scope:                  "scope",
		Organization:           "org",
		Environment:            "test",
		NumDevelopers:          *numDevs,
		AddDeveloperEvery:      *addDevEach,
		UpdateDeveloperEvery:   *upDevEach,
		NumDeployments:         *numDeps,
		ReplaceDeploymentEvery: *upDepEach,
		BundleURI:              *bundleURI,
	}

	log.Printf("Params: %#v\n", params)

	apidApigeeSync.Mock(params, router)

	// print the base url to the console
	listener := config.GetString("api_listen")
	log.Print()
	log.Printf("API is bound to: %s", listener)
	log.Print()

	// start client API listener
	api := apid.API()
	err := api.Listen()
	if err != nil {
		log.Print(err)
	}
}
