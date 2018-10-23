package bqmigrate

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// BQMigrate bigquery schema control manage
type BQMigrate struct {
	client   *bigquery.Client
	dataset  *bigquery.Dataset
	option   *MigrateOption
	versions []*MigrateVersion
	ctx      context.Context
}

// MigrateFunc function of the bigquery schema migrate
type MigrateFunc func(*bigquery.Client) error

// MigrateVersion represent the biqquery schema version
type MigrateVersion struct {
	ID      string
	Migrate MigrateFunc
}

// MigrateOption selection for bigquery schema migrate
type MigrateOption struct {
	TableName  string
	ColumnName string
}

// DefaultOption for bigquery schema migrate
var DefaultOption = MigrateOption{
	TableName:  "MigrateVersion",
	ColumnName: "ID",
}

// New instance BQMigrate
func New(client *bigquery.Client,
	dataset *bigquery.Dataset,
	option *MigrateOption,
	versions []*MigrateVersion) *BQMigrate {
	return &BQMigrate{
		client:   client,
		dataset:  dataset,
		option:   option,
		versions: versions,
		ctx:      context.Background(),
	}
}

// Migrate execute schema
func (m *BQMigrate) Migrate() (err error) {
	versionMap, err := m.getVersion()
	if err != nil {
		fmt.Println(err)
		if err = m.initMigrate(); err != nil {
			return
		}
		// sometimes first one insert will lost for unknown reason
		// here to insert a meaningless data to prevent this condition
		// if err = m.insertVersion("init"); err != nil {
		// 	return
		// }
		if versionMap, err = m.getVersion(); err != nil {
			return
		}
	}

	for _, v := range m.versions {
		if versionMap[v.ID] {
			continue
		}

		fmt.Printf("Migrate version: %s\n", v.ID)

		if err = v.Migrate(m.client); err != nil {
			return fmt.Errorf("Version %s migrate error: %s", v.ID, err)
		}
		if err = m.insertVersion(v.ID); err != nil {
			return fmt.Errorf("Insert version failed: %s", err)
		}
		fmt.Printf("%s complete\n", v.ID)
	}

	return
}

func (m *BQMigrate) initMigrate() error {
	fmt.Println("Init MigrateVersion")
	metaData := &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: m.option.ColumnName, Type: bigquery.StringFieldType},
		},
	}
	tableRef := m.dataset.Table(m.option.TableName)
	return tableRef.Create(m.ctx, metaData)
}
