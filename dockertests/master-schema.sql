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

CREATE SCHEMA IF NOT EXISTS edgex;
ALTER DATABASE edgex SET search_path TO edgex;
SET search_path TO edgex;

CREATE TABLE apid_cluster (
    id character varying(36) NOT NULL,
    name text NOT NULL,
    description text,
    umbrella_org_app_name text NOT NULL,
    created timestamp without time zone,
    created_by text,
    updated timestamp without time zone,
    updated_by text,
    _change_selector text,
    org_scope character varying(36),
    env_scope character varying(36),
    CONSTRAINT apid_cluster_pkey PRIMARY KEY (id)
);

CREATE INDEX apid_cluster___change_selector_idx ON apid_cluster USING btree (_change_selector);
CREATE INDEX apid_cluster_created_by_idx ON apid_cluster USING btree (created_by);

CREATE TABLE data_scope (
    id character varying(36) NOT NULL,
    apid_cluster_id character varying(36) NOT NULL,
    scope text NOT NULL,
    org text,
    env text,
    created timestamp without time zone,
    created_by text,
    updated timestamp without time zone,
    updated_by text,
    _change_selector text,
    org_scope character varying(36) NOT NULL,
    env_scope character varying(36) NOT NULL,
    CONSTRAINT data_scope_pkey PRIMARY KEY (id),
    CONSTRAINT data_scope_apid_cluster_id_fk FOREIGN KEY (apid_cluster_id)
          REFERENCES apid_cluster (id)
          ON UPDATE NO ACTION ON DELETE CASCADE
);
CREATE INDEX apid_cluster_scope__change_selector_idx ON data_scope USING btree (_change_selector);
CREATE INDEX apid_cluster_scope_apid_cluster_id_idx ON data_scope USING btree (apid_cluster_id);
CREATE UNIQUE INDEX apid_cluster_scope_apid_cluster_id_org_env_idx ON data_scope USING btree (apid_cluster_id, org, env);
CREATE INDEX data_scope_created_by_idx ON apid_cluster USING btree (created_by);


CREATE TABLE bundle_config (
    id character varying(36) NOT NULL,
    data_scope_id character varying(36) NOT NULL,
    name text NOT NULL,
    uri text NOT NULL,
    checksumType text,
    checksum text,
    created timestamp without time zone,
    created_by text,
    updated timestamp without time zone,
    updated_by text,
    CONSTRAINT bundle_config_pkey PRIMARY KEY (id),
    CONSTRAINT bundle_config_data_scope_id_fk FOREIGN KEY (data_scope_id)
          REFERENCES data_scope (id)
          ON UPDATE NO ACTION ON DELETE CASCADE
);

CREATE INDEX bundle_config_data_scope_id_idx ON bundle_config USING btree (data_scope_id);
CREATE INDEX bundle_config_created_by_idx ON apid_cluster USING btree (created_by);

CREATE TABLE deployment (
    id character varying(36) NOT NULL,
    bundle_config_id character varying(36) NOT NULL,
    apid_cluster_id character varying(36) NOT NULL,
    data_scope_id character varying(36) NOT NULL,
    bundle_config_name text NOT NULL,
    bundle_config_json text NOT NULL,
    config_json text NOT NULL,
    created timestamp without time zone,
    created_by text,
    updated timestamp without time zone,
    updated_by text,
    _change_selector text,
    CONSTRAINT deployment_pkey PRIMARY KEY (id),
    CONSTRAINT deployment_bundle_config_id_fk FOREIGN KEY (bundle_config_id)
        REFERENCES bundle_config (id)
        ON UPDATE NO ACTION ON DELETE CASCADE
);

CREATE TABLE deployment_history (
        id character varying(36) NOT NULL,
        deployment_id character varying(36) NOT NULL,
        action text NOT NULL,
        bundle_config_id character varying(36),
        apid_cluster_id character varying(36) NOT NULL,
        data_scope_id character varying(36) NOT NULL,
        bundle_config_json text NOT NULL,
        config_json text NOT NULL,
        created timestamp without time zone,
        created_by text,
        updated timestamp without time zone,
        updated_by text,
        CONSTRAINT deployment_history_pkey PRIMARY KEY (id)
);

CREATE INDEX deployment__change_selector_idx ON deployment USING btree (_change_selector);
CREATE INDEX deployment_apid_cluster_id_idx ON deployment USING btree (apid_cluster_id);
CREATE INDEX deployment_bundle_config_id_idx ON deployment USING btree (bundle_config_id);
CREATE INDEX deployment_data_scope_id_idx ON deployment USING btree (data_scope_id);
CREATE INDEX deployment_created_by_idx ON apid_cluster USING btree (created_by);

CREATE TABLE configuration (
    id character varying(36) NOT NULL,
    body text NOT NULL DEFAULT '{}',
    created timestamp without time zone,
    created_by text,
    updated timestamp without time zone,
    updated_by text,
    CONSTRAINT configuration_pkey PRIMARY KEY (id)
);

ALTER TABLE apid_cluster REPLICA IDENTITY FULL;
ALTER TABLE data_scope REPLICA IDENTITY FULL;
ALTER TABLE deployment REPLICA IDENTITY FULL;
