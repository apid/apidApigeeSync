-- Copyright 2017 Google Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE _transicator_metadata
(key varchar primary key,
value varchar);
INSERT INTO "_transicator_metadata" VALUES('snapshot','unused_in_listener_unit_tests');
CREATE TABLE _transicator_tables
(tableName varchar not null,
columnName varchar not null,
typid integer,
primaryKey bool);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','id',1043,1);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','name',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','description',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','umbrella_org_app_name',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','created',1114,0);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','created_by',25,1);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','updated',1114,0);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','updated_by',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_apid_cluster','_change_selector',25,1);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','id',1043,1);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','apid_cluster_id',1043,1);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','scope',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','org',25,1);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','env',25,1);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','org_scope',1043,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','env_scope',1043,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','created',1114,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','created_by',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','updated',1114,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','updated_by',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','_change_selector',25,1);

CREATE TABLE "edgex_apid_cluster" (id text,name text,description text,umbrella_org_app_name text,created blob,created_by text,updated blob,updated_by text,_change_selector text, primary key (id,created_by,_change_selector));
INSERT INTO "edgex_apid_cluster" VALUES('i','n','d','o', 'c', 'c', 'u','u', 'i');

CREATE TABLE "edgex_data_scope" (id text,apid_cluster_id text,scope text,org text,env text,org_scope text,env_scope text,created blob,created_by text,updated blob,updated_by text,_change_selector text, primary key (id,apid_cluster_id,apid_cluster_id,org,env,_change_selector));
INSERT INTO "edgex_data_scope" VALUES('i','a','s1','o','e1','org_scope_1','env_scope_1','c','c','u','u','a');
INSERT INTO "edgex_data_scope" VALUES('i','a','s1','o','e2','org_scope_1','env_scope_2','c','c','u','u','a');
INSERT INTO "edgex_data_scope" VALUES('k','a','s2','o','e3','org_scope_1','env_scope_3','c','c','u','u','a');

COMMIT;
