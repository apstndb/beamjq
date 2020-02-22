package beamjq

import (
	"encoding/json"
	"log"
	"reflect"

	"github.com/itchyny/gojq"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*jqFilterFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*jqFilterStringFn)(nil)).Elem())
}

type jqFilterFn struct {
	Filter string `json:"Filter"`
	query  *gojq.Query
}

func JqFilter(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	s = s.Scope("JqFilter: " + filter)
	return beam.ParDo(s, JqFilterFn(filter), input)
}

func JqFilterString(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	s = s.Scope("JqFilterString: " + filter)
	return beam.ParDo(s, JqFilterStringFn(filter), input)
}

func JqFilterFn(filter string) *jqFilterFn {
	return &jqFilterFn{Filter: filter}
}

func (f *jqFilterFn) Setup() {
	query, err := gojq.Parse(f.Filter)
	if err != nil {
		panic(err)
	}
	f.query = query
}

func (f *jqFilterStringFn) Setup() {
	query, err := gojq.Parse(f.Filter)
	if err != nil {
		panic(err)
	}
	f.query = query
}

func (f *jqFilterFn) ProcessElement(row []byte, emit func([]byte)) error {
	return ProcessElementImpl(f.query, row, emit)
}

func ProcessElementImpl(query *gojq.Query, row []byte, emit func([]byte)) error {
	var input interface{}
	err := json.Unmarshal(row, &input)
	if err != nil {
		log.Fatalln(err)
	}
	iter := query.Run(input) // or query.RunWithContext
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

type jqFilterStringFn struct {
	Filter string `json:"Filter"`
	query  *gojq.Query
}


func JqFilterStringFn(filter string) *jqFilterStringFn {
	return &jqFilterStringFn{Filter: filter}
}

func (f jqFilterStringFn) ProcessElement(row string, emit func(string)) error {
	return ProcessElementImpl(f.query, []byte(row), func(v []byte) {
		emit(string(v))
	})
}
