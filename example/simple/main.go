package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apstndb/beamjq"
)

var (
	output = flag.String("output", "", "Output file (required).")

	filter = flag.String("filter", "", "jq filter (required).")
)

func main() {
	flag.Parse()
	beam.Init()

	if *filter == "" {
		log.Fatal("No filter provided")
	}

	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	input, err := json.Marshal(map[string]interface{}{"foo": []interface{}{1, 2, 3}})
	if err != nil {
		log.Fatalln(err)
	}
	lines := beam.Create(s, input)
	filtered := beam.ParDo(s, beamjq.JqFilterBinaryFn(*filter), lines)
	formatted := beam.ParDo(s, func(b []byte) string {
		return string(b)
	}, filtered)
	textio.Write(s, *output, formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

