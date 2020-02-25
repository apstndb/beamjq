package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apstndb/beamjq"
)

var (
	output = flag.String("output", "", "Output file (required).")
)

func init() {
}

func main() {
	type T struct {
		I int `json:"i"`
		S string `json:"s"`
	}

	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	pcol0 := beam.Create(s, T{I: 1, S: "foo"}, T{I: 2, S: "bar"})
	pcol1 := beamjq.JqFilterReadTyped(s, "select(.i == 1)", pcol0)
	pcol2 := beamjq.JqFilterOutputTyped(s, ".i = .i + 100", pcol1, reflect.TypeOf(T{}))
	pcol3 := beam.ParDo(s, func(v T, emit func(string)) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		emit(string(b))
		return nil
	}, pcol2)
	textio.Write(s, *output, pcol3)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
