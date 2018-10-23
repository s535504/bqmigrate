package bqmigrate

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func (m *BQMigrate) insertVersion(data interface{}) error {
	saver := &bigquery.ValuesSaver{
		Schema: bigquery.Schema{
			{Name: m.option.ColumnName, Type: bigquery.StringFieldType},
		},
		Row: []bigquery.Value{data},
	}
	table := m.dataset.Table(m.option.TableName)
	u := table.Uploader()
	return u.Put(m.ctx, saver)
}

func (m *BQMigrate) getVersion() (map[string]bool, error) {
	var err error

	queryStr := fmt.Sprintf(`SELECT * FROM %s.%s`, m.dataset.DatasetID, m.option.TableName)
	q := m.client.Query(queryStr)

	it, err := m.run(q)
	if err != nil {
		return nil, err
	}

	versionMap := make(map[string]bool)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		versionMap[row[0].(string)] = true
	}
	return versionMap, err
}

// executes a query then return row iterator
func (m *BQMigrate) run(q *bigquery.Query) (it *bigquery.RowIterator, err error) {
	job, err := q.Run(m.ctx)
	if err != nil {
		return
	}
	status, err := job.Wait(m.ctx)
	if err != nil {
		return
	}
	if err = status.Err(); err != nil {
		return
	}
	return job.Read(m.ctx)
}
