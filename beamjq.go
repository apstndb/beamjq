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
	filter string      `json:"filter"`
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
	return &jqFilterFn{filter: filter}
}

func (f *jqFilterFn) Setup() {
	query, err := gojq.Parse(f.filter)
	if err != nil {
		panic(err)
	}
	f.query = query
}

func (f *jqFilterStringFn) Setup() {
	f.jqFilterFn.Setup()
}

func (f *jqFilterFn) ProcessElement(row []byte, emit func([]byte)) error {
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

type jqFilterStringFn struct {
	*jqFilterFn
}

func JqFilterStringFn(filter string) *jqFilterStringFn {
	return &jqFilterStringFn{JqFilterFn(filter)}
}

func (f jqFilterStringFn) ProcessElement(row string, emit func(string)) error {
	return f.jqFilterFn.ProcessElement([]byte(row), func(v []byte) {
		emit(string(v))
	})
}
