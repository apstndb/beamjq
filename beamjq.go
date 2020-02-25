package beamjq

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/itchyny/gojq"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*jqFilterBinaryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*jqFilterStringFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*jqFilterOutputTypedFn)(nil)).Elem())
}

func JqFilterBinary(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	s = s.Scope("JqFilterBinary: " + filter)
	return beam.ParDo(s, JqFilterBinaryFn(filter), input)
}

func JqFilterString(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	s = s.Scope("JqFilterString: " + filter)
	return beam.ParDo(s, JqFilterStringFn(filter), input)
}

func JqFilterOutputTyped(s beam.Scope, filter string, input beam.PCollection, t reflect.Type) beam.PCollection {
	s = s.Scope("JqFilterOutputTyped: " + filter)
	return beam.ParDo(s, &jqFilterOutputTypedFn{Filter: filter, Type: beam.EncodedType{t}},
	input, beam.TypeDefinition{Var: beam.XType, T: t})
}

type jqFilterOutputTypedFn struct {
	Filter    string           `json:"filter"`
	Type      beam.EncodedType `json:"type"`
	query     *gojq.Query
}

func (f *jqFilterOutputTypedFn) Setup() {
	query, err := gojq.Parse(f.Filter)
	if err != nil {
		panic(err)
	}
	f.query = query
}


func (f *jqFilterOutputTypedFn) ProcessElement(row []byte, emit func(beam.X)) error {
	var input interface{}
	err := json.Unmarshal(row, &input)
	if err != nil {
		return err
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
		val, err := jsonRoundTrip(v, f.Type.T)
		if err != nil {
			return err
		}
		emit(val)
	}
	return nil
}

func jsonRoundTrip(input interface{}, t reflect.Type) (interface{}, error) {
	val := reflect.New(t).Interface()
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &val)
	if err != nil {
		return nil, err
	}
	return reflect.ValueOf(val).Elem().Interface(), nil
}

func jsonRoundTripToInterface(input interface{}) (interface{}, error) {
	var val interface{}
	b, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &val)
	if err != nil {
		return nil, err
	}
	return val, nil
}


func JqFilterReadTyped(s beam.Scope, filter string, input beam.PCollection) beam.PCollection {
	// t := input.Type().Type()
	s = s.Scope("JqFilterOutputTyped: " + filter)
	pre := beam.AddFixedKey(s, input)
	post := beam.GroupByKey(s, pre)
	return beam.ParDo(s, &jqFilterReadTypedFn{Filter: filter}, post)
}

type jqFilterReadTypedFn struct {
	Filter string           `json:"filter"`
	query  *gojq.Query
}

func (f *jqFilterReadTypedFn) Setup() {
	query, err := gojq.Parse(f.Filter)
	if err != nil {
		panic(err)
	}
	f.query = query
}


func (f *jqFilterReadTypedFn) ProcessElement(ctx context.Context, _ int, iter func(*beam.X) bool, emit func([]byte)) error {
	var val beam.X
	if iter(&val) {
		val2, err := jsonRoundTripToInterface(val)
		if err != nil {
			log.Warnln(ctx, "roundtrip failed")
			return err
		}
		log.Warnf(ctx, "%#v\n", val2)
		iter := f.query.Run(val2) // or query.RunWithContext
		for {
			v, ok := iter.Next()
			if !ok {
				log.Warnln(ctx, "iter.Next() failed")
				break
			}
			if err, ok := v.(error); ok {
				log.Warnln(ctx, "v is error", err)
				return err
			}
			b, err := json.Marshal(v)
			if err != nil {
				log.Warnln(ctx, "unmarshall v", err)
				return err
			}
			emit(b)
		}
	}
	return nil
}


type jqFilterBinaryFn struct {
	Filter string `json:"filter"`
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

func (f *jqFilterBinaryFn) ProcessElement(ctx context.Context, row []byte, emit func([]byte)) error {
	var input interface{}
	err := json.Unmarshal(row, &input)
	if err != nil {
		return err
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

func (f jqFilterStringFn) ProcessElement(ctx context.Context, row string, emit func(string)) error {
	return f.jqFilterBinaryFn.ProcessElement(ctx, []byte(row), func(v []byte) {
		emit(string(v))
	})
}

func (f *jqFilterStringFn) Setup() {
	f.jqFilterBinaryFn.Setup()
}
