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
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','created',1114,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','created_by',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','updated',1114,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','updated_by',25,0);
INSERT INTO "_transicator_tables" VALUES('edgex_data_scope','_change_selector',25,1);
CREATE TABLE "edgex_apid_cluster" (id text,name text,description text,umbrella_org_app_name text,created blob,created_by text,updated blob,updated_by text,_change_selector text, primary key (id,created_by,_change_selector));
INSERT INTO "edgex_apid_cluster" VALUES('bootstrap','mitch-gcp-cluster','','X-5NF3iDkQLtQt6uPp4ELYhuOkzL5BbSMgf3Gx','2017-02-27 07:39:22.179+00:00','fierrom@google.com','2017-02-27 07:39:22.179+00:00','fierrom@google.com','bootstrap');
INSERT INTO "edgex_apid_cluster" VALUES('bootstrap2','mitch-gcp-cluster','','X-5NF3iDkQLtQt6uPp4ELYhuOkzL5BbSMgf3Gx','2017-02-27 07:39:22.179+00:00','fierrom@google.com','2017-02-27 07:39:22.179+00:00','fierrom@google.com','bootstrap');
CREATE TABLE "edgex_data_scope" (id text,apid_cluster_id text,scope text,org text,env text,created blob,created_by text,updated blob,updated_by text,_change_selector text, primary key (id,apid_cluster_id,apid_cluster_id,org,env,_change_selector));
INSERT INTO "edgex_data_scope" VALUES('dataScope1','bootstrap','43aef41d','edgex_gcp1','test','2017-02-27 07:40:25.094+00:00','fierrom@google.com','2017-02-27 07:40:25.094+00:00','fierrom@google.com','bootstrap');
INSERT INTO "edgex_data_scope" VALUES('dataScope2','bootstrap','43aef41d','edgex_gcp1','test','2017-02-27 07:40:25.094+00:00','fierrom@google.com','2017-02-27 07:40:25.094+00:00','sendtofierro@gmail.com','bootstrap');
COMMIT;
