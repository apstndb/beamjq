package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apstndb/beamjq"
)

var (
	input = flag.String("input", "", "Input file (required).")

	output = flag.String("output", "", "Output file (required).")

	filters Strings
)

type Strings []string

func (s *Strings) String() string {
	return fmt.Sprintf("%v", []string(*s))
}

func (s *Strings) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func init() {
	flag.Var(&filters, "filter", "jq filter (required).")
}

func main() {
	flag.Parse()
	beam.Init()

	if len(filters) == 0 {
		log.Fatal("No filter provided")
	}

	if *output == "" {
		log.Fatal("No output provided")
	}

	p := beam.NewPipeline()
	s := p.Root()

	lines := textio.Read(s, *input)
	pc := lines
	for _, filter := range filters {
		pc = beamjq.JqFilterString(s, filter, pc)
	}
	textio.Write(s, *output, pc)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
