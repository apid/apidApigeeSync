package main

import (
	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"time"
	"github.com/30x/apidApigeeSync"
)

// runs a mock server standalone
func main() {
	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()
	log.Debug("initializing...")

	config := apid.Config()
	config.SetDefault("api_port", "9001")

	router := apid.API().Router()

	params := apidApigeeSync.MockParms{
		ReliableAPI:                 true,
		ClusterID:                   "ZZZ",
		TokenKey:                    "XXX",
		TokenSecret:                 "YYY",
		Scope:                       "ert452",
		Organization:                "att",
		Environment:                 "prod",
		NumDevelopers:               5,
		AddDeveloperEvery:           3 * time.Second,
		UpdateDeveloperEvery:        1 * time.Second,
		NumDeployments:              100,
		ReplaceDeploymentEvery:      3 * time.Second,
	}

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
