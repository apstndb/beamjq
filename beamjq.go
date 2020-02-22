package beamjq

import (
	"encoding/json"
	"log"
	"reflect"

	"github.com/itchyny/gojq"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*jqFilterBinaryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*jqFilterStringFn)(nil)).Elem())
}

func JqFilterBinary(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	s = s.Scope("JqFilterBinary: " + filter)
	return beam.ParDo(s, JqFilterBinaryFn(filter), input)
}

func JqFilterString(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	s = s.Scope("JqFilterString: " + filter)
	return beam.ParDo(s, JqFilterStringFn(filter), input)
}

type jqFilterBinaryFn struct {
	Filter string `json:"Filter"`
	query  *gojq.Query
}

func JqFilterBinaryFn(filter string) *jqFilterBinaryFn {
	return &jqFilterBinaryFn{Filter: filter}
}

func (f *jqFilterBinaryFn) Setup() {
	query, err := gojq.Parse(f.Filter)
	if err != nil {
		panic(err)
	}
	f.query = query
}


func (f *jqFilterBinaryFn) ProcessElement(row []byte, emit func([]byte)) error {
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
	jqFilterBinaryFn
}


func JqFilterStringFn(filter string) *jqFilterStringFn {
	return &jqFilterStringFn{jqFilterBinaryFn{Filter: filter}}
}

func (f jqFilterStringFn) ProcessElement(row string, emit func(string)) error {
	return f.jqFilterBinaryFn.ProcessElement([]byte(row), func(v []byte) {
		emit(string(v))
	})
}

func (f *jqFilterStringFn) Setup() {
	f.jqFilterBinaryFn.Setup()
}

