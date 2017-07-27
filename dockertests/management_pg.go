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

package dockertests

import (
	"database/sql"
	_ "github.com/lib/pq"
)

var (
	basicTables = map[string]bool{
		"deployment_history": true,
		"deployment":         true,
		"bundle_config":      true,
		"configuration":      true,
		"apid_cluster":       true,
		"data_scope":         true,
	}
)

type ManagementPg struct {
	url string
	pg  *sql.DB
}

func InitDb(dbUrl string) (*ManagementPg, error) {
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		return nil, err
	}

	return &ManagementPg{
		url: dbUrl,
		pg:  db,
	}, nil
}

func (m *ManagementPg) InsertApidCluster(tx *sql.Tx, cluster *apidClusterRow) error {
	stmt, err := tx.Prepare(`INSERT INTO edgex.apid_cluster(
			id,
			name,
			description,
			umbrella_org_app_name,
			created,
			created_by,
			updated,
			updated_by,
			_change_selector
			)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		cluster.id,
		cluster.name,
		cluster.description,
		cluster.appName,
		cluster.created,
		cluster.createdBy,
		cluster.updated,
		cluster.updatedBy,
		cluster.changeSelector,
	)

	return err
}

func (m *ManagementPg) InsertDataScope(tx *sql.Tx, ds *dataScopeRow) error {
	stmt, err := tx.Prepare(`INSERT INTO edgex.data_scope (
			id,
			apid_cluster_id,
			scope,
			org,
			env,
			created,
			created_by,
			updated,
			updated_by,
			_change_selector
			)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		ds.id,
		ds.clusterId,
		ds.scope,
		ds.org,
		ds.env,
		ds.created,
		ds.createdBy,
		ds.updated,
		ds.updatedBy,
		ds.changeSelector,
	)

	return err
}

func (m *ManagementPg) InsertBundleConfig(tx *sql.Tx, bf *bundleConfigRow) error {
	stmt, err := tx.Prepare(`INSERT INTO edgex.bundle_config (
			id,
			data_scope_id,
			name,
			uri,
			checksumtype,
			checksum,
			created,
			created_by,
			updated,
			updated_by
			)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		bf.id,
		bf.scopeId,
		bf.name,
		bf.uri,
		bf.checksumType,
		bf.checksum,
		bf.created,
		bf.createdBy,
		bf.updated,
		bf.updatedBy,
	)

	return err
}

func (m *ManagementPg) InsertDeployment(tx *sql.Tx, d *deploymentRow) error {
	stmt, err := tx.Prepare(`INSERT INTO edgex.deployment (
			id,
			bundle_config_id,
			apid_cluster_id,
			data_scope_id,
			bundle_config_name,
			bundle_config_json,
			config_json,
			created,
			created_by,
			updated,
			updated_by,
			_change_selector
			)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		d.id,
		d.configId,
		d.clusterId,
		d.scopeId,
		d.bundleConfigName,
		d.bundleConfigJson,
		d.configJson,
		d.created,
		d.createdBy,
		d.updated,
		d.updatedBy,
		d.changeSelector,
	)

	return err
}

func (m *ManagementPg) BeginTransaction() (*sql.Tx, error) {
	tx, err := m.pg.Begin()
	return tx, err
}

/*
 * Delete all new tables or rows created by a test from pg.
 * Only test data for the whole suite will remain in the pg.
 */
func (m *ManagementPg) CleanupTest() error {

	// clean tables
	tablesToDelete := make([]string, 0)
	rows, err := m.pg.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='edgex';")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}

		if !basicTables[tableName] {
			tablesToDelete = append(tablesToDelete, tableName)
		}
	}

	for _, tableName := range tablesToDelete {
		cleanupSql := "DROP TABLE edgex." + tableName + ";"
		_, err := m.pg.Exec(cleanupSql)
		if err != nil {
			return err
		}
	}
	cleanupSql := "DELETE FROM edgex.apid_cluster WHERE created_by!='" + testInitUser + "';"
	_, err = m.pg.Exec(cleanupSql)
	if err != nil {
		return err
	}

	// clean enum types
	typesToDelete := make([]string, 0)
	typeRows, err := m.pg.Query("SELECT DISTINCT pg_type.typname AS enumtype FROM pg_type JOIN pg_enum ON pg_enum.enumtypid = pg_type.oid;")
	if err != nil {
		return err
	}
	defer typeRows.Close()
	for typeRows.Next() {
		var typeName string
		err = typeRows.Scan(&typeName)
		if err != nil {
			return err
		}
		typesToDelete = append(typesToDelete, typeName)
	}

	for _, typeName := range typesToDelete {
		cleanupSql := "DROP TYPE edgex." + typeName + ";"
		_, err := m.pg.Exec(cleanupSql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ManagementPg) CleanupAll() error {
	cleanupSql := "DELETE FROM edgex.apid_cluster;"
	_, err := m.pg.Exec(cleanupSql)
	return err
}
