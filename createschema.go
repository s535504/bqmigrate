package bqmigrate

import (
	"context"
	"reflect"

	"cloud.google.com/go/bigquery"
)

// CreateSchema create bigquery table schema by infer struct
func CreateSchema(set *bigquery.Dataset, st interface{}) (err error) {
	ctx := context.Background()
	schema, err := bigquery.InferSchema(st)
	if err != nil {
		return
	}

	table := set.Table(structName(st))
	return table.Create(ctx, &bigquery.TableMetadata{Schema: schema})
}

func structName(data interface{}) (name string) {
	if reflect.TypeOf(data).Kind() == reflect.Ptr {
		name = reflect.TypeOf(data).Elem().Name()
	} else {
		name = reflect.ValueOf(data).Type().Name()
	}
	return
}
