package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apstndb/beamjq"
)

var (
	input = flag.String("input", "", "Input file (required).")

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

	lines := textio.Read(s, *input)
	filtered := beam.ParDo(s, beamjq.JqFilterStringFn(*filter), lines)
	textio.Write(s, *output, filtered)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
