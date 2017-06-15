package dockertests

import (
	"database/sql"
	_ "github.com/lib/pq"
	"time"
)

type ManagementPg struct {
	url string
	pg  *sql.DB
}

type apidCluster struct {
	id          string
	name        string
	description string
	appName     string
	created     time.Time
	createdBy   string
	updated     time.Time
	updatedBy   string
	changeSelector string
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

func (m *ManagementPg) InsertApidCluster(tx *sql.Tx, cluster *apidCluster) error {
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

func (m *ManagementPg) BeginTransaction() (*sql.Tx, error) {
	tx, err := m.pg.Begin()
	return tx, err
}

