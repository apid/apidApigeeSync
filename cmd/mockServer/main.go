// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"

	"os"

	"github.com/apid/apid-core"
	"github.com/apid/apid-core/factory"
	"github.com/apid/apidApigeeSync"
)

// runs a mock server standalone
func main() {
	// create new flag to avoid displaying all the Ginkgo flags
	f := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	bundleURI := f.String("bundleURI", "", "a URI to a valid deployment bundle (default '')")

	reliable := f.Bool("reliable", true, "if false, server will often send 500 errors")

	numDevs := f.Int("numDevs", 2, "number of developers in snapshot")

	numDeps := f.Int("numDeps", 2, "number of deployments in snapshot")

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
		ReliableAPI:    *reliable,
		ClusterID:      "cluster",
		TokenKey:       "key",
		TokenSecret:    "secret",
		Scope:          "scope",
		Organization:   "org",
		Environment:    "test",
		NumDevelopers:  *numDevs,
		NumDeployments: *numDeps,
		BundleURI:      *bundleURI,
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
