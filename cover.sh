#!/bin/bash -eu
#
# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash

set -e
echo "mode: atomic" > coverage.txt

go test -coverprofile=profile.out -covermode=atomic github.com/apid/apidApigeeSync
if [ -f profile.out ]; then
    tail -n +2 profile.out >> coverage.txt
    rm profile.out
fi
go tool cover -html=coverage.txt -o cover.html
