package main

import (
	"flag"

	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"github.com/30x/apidApigeeSync"
)

// runs a mock server standalone
func main() {
	reliable := *flag.Bool("reliable", true, "if false, server will often send 500 errors [true]")

	numDevs := *flag.Int("numDevs", 2, "number of developers in snapshot [2]")
	addDevEach := *flag.Duration("addDevEach", 0, "add a developer each duration [0s]")
	upDevEach := *flag.Duration("upDevEach", 0, "update a developer each duration [0s]")

	numDeps := *flag.Int("numDeps", 2, "number of deployments in snapshot [2]")
	upDepEach := *flag.Duration("upDepEach", 0, "update (replace) a deployment each duration [0s]")

	flag.Parse()

	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()
	log.Debug("initializing...")

	config := apid.Config()
	config.SetDefault("api_port", "9001")

	router := apid.API().Router()

	params := apidApigeeSync.MockParms{
		ReliableAPI:            reliable,
		ClusterID:              "cluster",
		TokenKey:               "key",
		TokenSecret:            "secret",
		Scope:                  "scope",
		Organization:           "org",
		Environment:            "test",
		NumDevelopers:          numDevs,
		AddDeveloperEvery:      addDevEach,
		UpdateDeveloperEvery:   upDevEach,
		NumDeployments:         numDeps,
		ReplaceDeploymentEvery: upDepEach,
	}

	log.Printf("Params: %#v\n", params)

	apidApigeeSync.Mock(params, router)

	// print the base url to the console
	port := config.GetString("api_port")
	log.Print()
	log.Printf("API is at: http://localhost:%s", port)
	log.Print()

	// start client API listener
	api := apid.API()
	err := api.Listen()
	if err != nil {
		log.Print(err)
	}
}
