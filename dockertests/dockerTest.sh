#!/usr/bin/env bash
source ./dockertests/dockerSetup.sh
go test ./dockertests/*.go
./dockertests/dockerCleanup.sh