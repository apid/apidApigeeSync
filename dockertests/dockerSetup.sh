#!/usr/bin/env bash

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

WORK_DIR=$(pwd)
TRANSICATOR_DIR="$GOPATH/src/github.com/apigee-labs/transicator"
DOCKER_IP="192.168.9.1"
if [ "$(uname)" == Darwin ];
then
    DOCKER_IP="localhost"
fi
TEST_PG_BASE=postgres://postgres:changeme@$DOCKER_IP:5432
TEST_PG_URL=postgres://postgres:changeme@$DOCKER_IP:5432/edgex
echo ${TEST_PG_URL}

export APIGEE_SYNC_DOCKER_PG_URL=${TEST_PG_URL}
export APIGEE_SYNC_DOCKER_IP=${DOCKER_IP}

pgnum=$(docker images "apigeelabs/transicator-postgres" | wc -l)
ssnum=$(docker images "apigeelabs/transicator-snapshot" | wc -l)
csnum=$(docker images "apigeelabs/transicator-changeserver" | wc -l)


if (( !(pgnum>1 && ssnum>1 && csnum>1) ))
then
    cd ${TRANSICATOR_DIR}
    make
    make docker
    cd ${WORK_DIR}
fi

echo "Starting Transicator docker"
pgname=apidSync_test_pg
ssname=apidSync_test_ss
csname=apidSync_test_cs

# run PG
docker run --name ${pgname} -p 5432:5432 -d -e POSTGRES_PASSWORD=changeme apigeelabs/transicator-postgres

# Wait for PG to be up -- it takes a few seconds
while `true`
do
  sleep 1
  psql -q -c 'select * from now()' ${TEST_PG_BASE}
  if [ $? -eq 0 ]
  then
    break
  fi
done

# init pg
psql -f ${WORK_DIR}/dockertests/create-db.sql ${TEST_PG_BASE}
psql -f ${WORK_DIR}/dockertests/master-schema.sql ${TEST_PG_URL}
psql -f ${WORK_DIR}/dockertests/user-setup.sql ${TEST_PG_URL}

# run SS and CS
docker run --name ${ssname} -d -p 9001:9001 apigeelabs/transicator-snapshot -p 9001 -u ${TEST_PG_URL}
docker run --name ${csname} -d -p 9000:9000 apigeelabs/transicator-changeserver -p 9000 -u ${TEST_PG_URL} -s testslot

# Wait for SS to be up
while `true`
do
  sleep 1
  response=$(curl -i http://${DOCKER_IP}:9001/snapshots?selector=foo | head -n 1)
  if [[ $response == *303* ]]
  then
    break
  fi
done

# Wait for CS to be up
while `true`
do
  sleep 1
  response=$(curl -i http://${DOCKER_IP}:9000/changes | head -n 1)
  if [[ $response == *200* ]]
  then
    break
  fi
done

apid_config=`cat <<EOF
apigeesync_instance_name: SQLLITAPID
apigeesync_snapshot_server_base: http://${DOCKER_IP}:9001/
apigeesync_change_server_base: http://${DOCKER_IP}:9000/
apigeesync_snapshot_proto: sqlite
log_level: Debug
apigeesync_consumer_key: 33f39JNLosF1mDOXJoCfbauchVzPrGrl
apigeesync_consumer_secret: LAolGShAx6H3vfNF
apigeesync_cluster_id: 4c6bb536-0d64-43ca-abae-17c08f1a7e58
local_storage_path: ${WORK_DIR}/tmp/sqlite
EOF
`
rm -f ${WORK_DIR}/dockertests/apid_config.yaml
echo "$apid_config" >> ${WORK_DIR}/dockertests/apid_config.yaml
