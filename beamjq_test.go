package beamjq_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apstndb/beamjq"
)

func TestJqFilterString(t *testing.T) {
	type T struct {
		I int `json:"i"`
		S string `json:"s"`
	}
	pipeline, s := beam.NewPipelineWithRoot()
	pcol := beam.Create(s, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	passert.Equals(s, pcol, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	jsons := beam.ParDo(s,
		func(val T, emit func (string)) error {b, err := json.Marshal(val); if err != nil {return err}; emit(string(b));return nil}, pcol)
	filteredS := beamjq.JqFilterString(s, "select(.i == 1) | .s", jsons)
	passert.Equals(s, filteredS, `"foo"`)
	if err := ptest.Run(pipeline); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestJqFilterBinary(t *testing.T) {
	type T struct {
		I int `json:"i"`
		S string `json:"s"`
	}
	pipeline, s := beam.NewPipelineWithRoot()
	pcol := beam.Create(s, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	passert.Equals(s, pcol, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	jsonb := beam.ParDo(s,
		func(val T, emit func ([]byte)) error {b, err := json.Marshal(val); if err != nil {return err}; emit(b);return nil}, pcol)
	filteredB := beamjq.JqFilterBinary(s, "select(.i == 1) | .s", jsonb)
	passert.Equals(s, filteredB, []byte(`"foo"`))
	if err := ptest.Run(pipeline); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestJqFilterTyped(t *testing.T) {
	type T struct {
		I int `json:"i"`
		S string `json:"s"`
	}
	pipeline, s := beam.NewPipelineWithRoot()
	pcol := beam.Create(s, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	passert.Equals(s, pcol, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	jsonb := beam.ParDo(s,
		func(val T, emit func ([]byte)) error {b, err := json.Marshal(val); if err != nil {return err}; emit(b);return nil}, pcol)
	filteredB := beamjq.JqFilterOutputTyped(s, "select(.i == 1)", jsonb, reflect.TypeOf(T{}))
	passert.Equals(s, filteredB, T{I: 1, S: "foo"})
	if err := ptest.Run(pipeline); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestJqFilterReadTyped(t *testing.T) {
	type T struct {
		I int `json:"i"`
		S string `json:"s"`
	}
	pipeline, s := beam.NewPipelineWithRoot()
	pcol := beam.Create(s, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	passert.Equals(s, pcol, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	_ = beamjq.JqFilterReadTyped(s, "select(.i == 1) | .s", pcol)
	// passert.Equals(s, filteredB, []byte("foo"))
	if err := ptest.Run(pipeline); err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}
