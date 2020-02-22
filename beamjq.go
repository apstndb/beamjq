package beamjq

import (
	"encoding/json"
	"github.com/itchyny/gojq"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*jqFilter)(nil)).Elem())
}

type jqFilter struct {
	filter string      `json:"filter"`
	query  *gojq.Query `json:"query"`
}

func JqFilter(filter string) *jqFilter {
	query, err := gojq.Parse(filter)
	if err != nil {
		panic(err)
	}
	return &jqFilter{filter: filter, query: query}
}

func (f *jqFilter) ProcessElement(row []byte, emit func([]byte)) error {
	var input interface{}
	err := json.Unmarshal(row, &input)
	if err != nil {
		log.Fatalln(err)
	}
	iter := f.query.Run(input) // or query.RunWithContext
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return err
		}
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		emit(b)
	}
	return nil
}

type jqFilterString struct {
	*jqFilter
}

func JqFilterString(filter string) *jqFilterString {
	return &jqFilterString{JqFilter(filter)}
}

func (f jqFilterString) ProcessElement(row string, emit func(string)) error {
	return f.jqFilter.ProcessElement([]byte(row), func(v []byte) {
		emit(string(v))
	})
}
