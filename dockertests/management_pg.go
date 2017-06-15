package dockertests

import (
	"database/sql"
	_ "github.com/lib/pq"
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
			_change_selector,
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

func (m *ManagementPg) Cleanup() error {
	cleanupSql := "DELETE FROM edgex.apid_cluster WHERE created_by!='" + testInitUser + "';"
	_, err := m.pg.Exec(cleanupSql)
	return err
}
