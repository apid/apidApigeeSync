package apidApigeeSync

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/apigee-labs/transicator/common"
)

var _ = Describe("listener", func() {

	handler := handler{}

	Context("ApigeeSync snapshot event", func() {

		It("should set DB to appropriate version", func() {

			event := common.Snapshot{
				SnapshotInfo: "test_snapshot",
				Tables:       []common.Table{},
			}

			handler.Handle(&event)

			Expect(apidInfo.LastSnapshot).To(Equal(event.SnapshotInfo))

			expectedDB, err := data.DBVersion(event.SnapshotInfo)
			Expect(err).NotTo(HaveOccurred())

			Expect(getDB() == expectedDB).Should(BeTrue())
		})

		It("should fail if more than one apid_cluster rows", func() {

			event := common.Snapshot{
				SnapshotInfo: "test_snapshot_fail",
				Tables: []common.Table{
					{
						Name: LISTENER_TABLE_APID_CLUSTER,
						Rows: []common.Row{{}, {}},
					},
				},
			}

			Expect(func() { handler.Handle(&event) }).To(Panic())
		})

		It("should process a valid Snapshot", func() {

			event := common.Snapshot{
				SnapshotInfo: "test_snapshot_valid",
				Tables: []common.Table{
					{
						Name: LISTENER_TABLE_APID_CLUSTER,
						Rows: []common.Row{
							{
								"id":                    &common.ColumnVal{Value: "i"},
								"_change_selector":      &common.ColumnVal{Value: "c"},
								"name":                  &common.ColumnVal{Value: "n"},
								"umbrella_org_app_name": &common.ColumnVal{Value: "o"},
								"created":               &common.ColumnVal{Value: "c"},
								"created_by":            &common.ColumnVal{Value: "c"},
								"updated":               &common.ColumnVal{Value: "u"},
								"updated_by":            &common.ColumnVal{Value: "u"},
								"description":           &common.ColumnVal{Value: "d"},
							},
						},
					},
					{
						Name: LISTENER_TABLE_DATA_SCOPE,
						Rows: []common.Row{
							{
								"id":               &common.ColumnVal{Value: "i"},
								"_change_selector": &common.ColumnVal{Value: "c"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
							},
						},
					},
				},
			}

			handler.Handle(&event)

			info, err := getApidInstanceInfo()
			Expect(err).NotTo(HaveOccurred())

			Expect(info.LastSnapshot).To(Equal(event.SnapshotInfo))

			db := getDB()

			// apid Cluster
			var dcs []dataApidCluster

			rows, err := db.Query(`
			SELECT id, name, description, umbrella_org_app_name,
				created, created_by, updated, updated_by,
				_change_selector
			FROM APID_CLUSTER`)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			c := dataApidCluster{}
			for rows.Next() {
				rows.Scan(&c.ID, &c.Name, &c.Description, &c.OrgAppName,
					&c.Created, &c.CreatedBy, &c.Updated, &c.UpdatedBy,
					&c.ChangeSelector)
				dcs = append(dcs, c)
			}

			Expect(len(dcs)).To(Equal(1))
			dc := dcs[0]

			Expect(dc.ID).To(Equal("i"))
			Expect(dc.Name).To(Equal("n"))
			Expect(dc.Description).To(Equal("d"))
			Expect(dc.OrgAppName).To(Equal("o"))
			Expect(dc.Created).To(Equal("c"))
			Expect(dc.CreatedBy).To(Equal("c"))
			Expect(dc.Updated).To(Equal("u"))
			Expect(dc.UpdatedBy).To(Equal("u"))
			Expect(dc.ChangeSelector).To(Equal("c"))

			// Data Scope
			var dds []dataDataScope

			rows, err = db.Query(`
			SELECT id, apid_cluster_id, scope, org,
				env, created, created_by, updated,
				updated_by, _change_selector
			FROM DATA_SCOPE`)
			Expect(err).NotTo(HaveOccurred())
			defer rows.Close()

			d := dataDataScope{}
			for rows.Next() {
				rows.Scan(&d.ID, &d.ClusterID, &d.Scope, &d.Org,
					&d.Env, &d.Created, &d.CreatedBy, &d.Updated,
					&d.UpdatedBy, &d.ChangeSelector)
				dds = append(dds, d)
			}

			Expect(len(dds)).To(Equal(1))
			ds := dds[0]

			Expect(ds.ID).To(Equal("i"))
			Expect(ds.Org).To(Equal("o"))
			Expect(ds.Env).To(Equal("e"))
			Expect(ds.Scope).To(Equal("s"))
			Expect(ds.Created).To(Equal("c"))
			Expect(ds.CreatedBy).To(Equal("c"))
			Expect(ds.Updated).To(Equal("u"))
			Expect(ds.UpdatedBy).To(Equal("u"))
			Expect(ds.ChangeSelector).To(Equal("c"))
		})
	})

	Context("ApigeeSync change event", func() {

		Context(LISTENER_TABLE_APID_CLUSTER, func() {

			It("insert event should panic", func() {

				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_APID_CLUSTER,
						},
					},
				}

				Expect(func() { handler.Handle(&event) }).To(Panic())
			})

			It("update event should panic", func() {

				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Update,
							Table:     LISTENER_TABLE_APID_CLUSTER,
						},
					},
				}

				Expect(func() { handler.Handle(&event) }).To(Panic())
			})

			PIt("delete event should kill all the things!")
		})

		Context(LISTENER_TABLE_DATA_SCOPE, func() {

			It("insert event should add", func() {
				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							NewRow: common.Row{
								"id":               &common.ColumnVal{Value: "i"},
								"_change_selector": &common.ColumnVal{Value: "c"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
							},
						},
					},
				}

				handler.Handle(&event)

				var dds []dataDataScope

				rows, err := getDB().Query(`
				SELECT id, apid_cluster_id, scope, org,
					env, created, created_by, updated,
					updated_by, _change_selector
				FROM DATA_SCOPE`)
				Expect(err).NotTo(HaveOccurred())
				defer rows.Close()

				d := dataDataScope{}
				for rows.Next() {
					rows.Scan(&d.ID, &d.ClusterID, &d.Scope, &d.Org,
						&d.Env, &d.Created, &d.CreatedBy, &d.Updated,
						&d.UpdatedBy, &d.ChangeSelector)
					dds = append(dds, d)
				}

				Expect(len(dds)).To(Equal(1))
				ds := dds[0]

				Expect(ds.ID).To(Equal("i"))
				Expect(ds.Org).To(Equal("o"))
				Expect(ds.Env).To(Equal("e"))
				Expect(ds.Scope).To(Equal("s"))
				Expect(ds.Created).To(Equal("c"))
				Expect(ds.CreatedBy).To(Equal("c"))
				Expect(ds.Updated).To(Equal("u"))
				Expect(ds.UpdatedBy).To(Equal("u"))
				Expect(ds.ChangeSelector).To(Equal("c"))
			})

			It("delete event should delete", func() {
				insert := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							NewRow: common.Row{
								"id":               &common.ColumnVal{Value: "i"},
								"_change_selector": &common.ColumnVal{Value: "c"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
							},
						},
					},
				}

				handler.Handle(&insert)

				delete := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Delete,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							OldRow:    insert.Changes[0].NewRow,
						},
					},
				}

				handler.Handle(&delete)

				var nRows int
				err := getDB().QueryRow("SELECT count(id) FROM DATA_SCOPE").Scan(&nRows)
				Expect(err).NotTo(HaveOccurred())

				Expect(nRows).To(Equal(0))
			})

			It("update event should panic", func() {

				event := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Update,
							Table:     LISTENER_TABLE_DATA_SCOPE,
						},
					},
				}

				Expect(func() { handler.Handle(&event) }).To(Panic())
			})

		})

	})
})
