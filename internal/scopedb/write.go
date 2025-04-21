package scopedb

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/prometheus/prometheus/prompb"
)

// Define the Arrow schema for storing Prometheus metrics
// Adjust types as needed based on ScopeDB capabilities (e.g., timestamp precision)
var metricSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false},
		{Name: "metric_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "labels", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	},
	nil,
)

func (s *ScopeDBAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, metricSchema)
	defer b.Release()

	tsBuilder := b.Field(0).(*array.TimestampBuilder)
	nameBuilder := b.Field(1).(*array.StringBuilder)
	mapBuilder := b.Field(2).(*array.MapBuilder)
	keyBuilder := mapBuilder.KeyBuilder().(*array.StringBuilder)
	valueBuilder := mapBuilder.ValueBuilder().(*array.StringBuilder)
	valBuilder := b.Field(3).(*array.Float64Builder)

	count := 0

	for _, ts := range req.Timeseries {
		var metricName string
		labels := make(map[string]string)
		hasLabels := false

		// Extract metric name and labels
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				metricName = l.Value
			} else {
				labels[l.Name] = l.Value
				hasLabels = true
			}
		}

		// Skip timeseries without a metric name (shouldn't happen often)
		if metricName == "" {
			continue
		}

		// Append samples to the Arrow record builder
		for _, sample := range ts.Samples {
			tsBuilder.Append(arrow.Timestamp(sample.Timestamp))
			nameBuilder.Append(metricName)

			// Build the map for labels
			if hasLabels {
				mapBuilder.Append(true) // Indicate this map entry is valid
				// Add key-value pairs for this map entry
				// MapBuilder handles offsets internally
				for k, v := range labels {
					keyBuilder.Append(k)
					valueBuilder.Append(v)
				}
			} else {
				mapBuilder.AppendNull() // No labels for this sample
			}

			valBuilder.Append(sample.Value)
			count++
		}
	}

	// Only ingest if there are records built
	if count > 0 {
		record := b.NewRecord()
		defer record.Release()

		// Use INSERT INTO for simple appends. Adjust if MERGE is needed.
		// Ensure the target table exists in ScopeDB.
		ingestStatement := fmt.Sprintf("INSERT INTO %s", s.table)

		_, err := s.conn.IngestArrowBatch(ctx, []arrow.Record{record}, ingestStatement)
		if err != nil {
			return 0, fmt.Errorf("ScopeDB IngestArrowBatch failed: %w", err)
		}
	}

	return count, nil
}
