package scopedb

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/prometheus/prometheus/prompb"
	scopedb "github.com/scopedb/scopedb-sdk/go"
)

func (s *ScopeDBAdapter) ReadRequest(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	resp := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	for i, q := range req.Queries {
		qresults := &prompb.QueryResult{}
		resp.Results[i] = qresults

		// Build ScopeQL query
		var whereClauses []string

		// Time range filtering (assuming 'timestamp' column of type TIMESTAMPTZ or similar)
		// ScopeDB might require specific time formatting or functions. Adjust as needed.
		// Using milliseconds directly might work if the column type supports it.
		whereClauses = append(whereClauses, fmt.Sprintf("timestamp >= %d", q.StartTimestampMs))
		if q.EndTimestampMs > 0 {
			whereClauses = append(whereClauses, fmt.Sprintf("timestamp <= %d", q.EndTimestampMs))
		}

		// Label Matcher filtering
		// This translation is basic. Regex and negative matchers might need specific ScopeDB functions
		// or might not be directly translatable efficiently.
		ignoreLabelKey, ignoreLabelValue, _ := strings.Cut(s.readIgnoreLabel, "=")

		for _, m := range q.Matchers {
			// Handle __name__ separately
			if m.Name == "__name__" {
				switch m.Type {
				case prompb.LabelMatcher_EQ:
					whereClauses = append(whereClauses, fmt.Sprintf("metric_name = '%s'", escapeScopeQLString(m.Value)))
				case prompb.LabelMatcher_NEQ:
					whereClauses = append(whereClauses, fmt.Sprintf("metric_name != '%s'", escapeScopeQLString(m.Value)))
				case prompb.LabelMatcher_RE:
					// Basic regex simulation using LIKE (adjust if ScopeDB has proper regex)
					whereClauses = append(whereClauses, fmt.Sprintf("regexp_like(metric_name, '%s')", m.Value))
				case prompb.LabelMatcher_NRE:
					// Basic regex simulation using NOT LIKE
					whereClauses = append(whereClauses, fmt.Sprintf("not regexp_like(metric_name, '%s')", m.Value))
				default:
					return nil, fmt.Errorf("unsupported __name__ LabelMatcher_Type %v", m.Type)
				}
			} else {
				// Handle other labels (assuming 'labels' is a MAP(STRING, STRING))
				labelKey := m.Name
				labelValue := m.Value

				// Skip ignored label
				if labelKey == ignoreLabelKey && labelValue == ignoreLabelValue {
					continue
				}

				switch m.Type {
				case prompb.LabelMatcher_EQ:
					// Check if key exists and value matches
					whereClauses = append(whereClauses, fmt.Sprintf("labels['%s'] = '%s'", escapeScopeQLString(labelKey), escapeScopeQLString(labelValue)))
				case prompb.LabelMatcher_NEQ:
					// Check if key doesn't exist OR value doesn't match
					whereClauses = append(whereClauses, fmt.Sprintf("(NOT contains_key(labels, '%s') OR labels['%s'] != '%s')", escapeScopeQLString(labelKey), escapeScopeQLString(labelKey), escapeScopeQLString(labelValue)))
				case prompb.LabelMatcher_RE:
					// Requires ScopeDB regex support on map values.
					whereClauses = append(whereClauses, fmt.Sprintf("regexp_like(labels['%s'], '%s')", escapeScopeQLString(labelKey), labelValue))
				case prompb.LabelMatcher_NRE:
					// Requires ScopeDB regex support.
					whereClauses = append(whereClauses, fmt.Sprintf("(NOT contains_key(labels, '%s') OR NOT regexp_like(labels['%s'], '%s'))", escapeScopeQLString(labelKey), escapeScopeQLString(labelKey), labelValue))
				default:
					return nil, fmt.Errorf("unsupported label '%s' LabelMatcher_Type %v", m.Name, m.Type)
				}
			}
		}

		// Construct the full query
		// Note: Hints (StepMs, RangeMs) are ignored for simplicity. Implementing them
		// would require ScopeDB aggregation/time-bucketing functions (e.g., time_bucket, avg/max).
		selectCols := "timestamp, metric_name, labels, value"
		query := fmt.Sprintf("SELECT %s FROM %s", selectCols, s.table)
		if len(whereClauses) > 0 {
			query += " WHERE " + strings.Join(whereClauses, " AND ")
		}
		// Order by time to ensure samples within a series are chronological
		query += " ORDER BY timestamp"

		if s.debug {
			fmt.Printf("ScopeDB Query: %s\n", query)
		}

		// Execute query
		resultSet, err := s.conn.QueryAsArrowBatch(ctx, &scopedb.StatementRequest{
			Statement: query,
			Format:    scopedb.ArrowJSONFormat,
		})
		if err != nil {
			// Check for specific ScopeDB errors if needed
			return nil, fmt.Errorf("ScopeDB QueryAsArrowBatch failed: %w", err)
		}

		// Process results
		if resultSet == nil || resultSet.Records == nil || len(*resultSet.Records) == 0 {
			continue // No data found for this query
		}

		// Group results by timeseries (metric_name + labels)
		// Map key: composite key "metric_name|sorted_label_key1=value1|key2=value2"
		seriesMap := make(map[string]*prompb.TimeSeries)

		for _, record := range *resultSet.Records {
			tsCol := record.Column(0).(*array.Timestamp)
			nameCol := record.Column(1).(*array.String)
			labelsCol := record.Column(2).(*array.Map)
			valueCol := record.Column(3).(*array.Float64)

			keyArray := labelsCol.Keys().(*array.String)
			valueArray := labelsCol.Items().(*array.String)

			for r := 0; r < int(record.NumRows()); r++ {
				if tsCol.IsNull(r) || nameCol.IsNull(r) || valueCol.IsNull(r) {
					continue // Skip rows with null essential fields
				}

				timestamp := int64(tsCol.Value(r))
				metricName := nameCol.Value(r)
				value := valueCol.Value(r)

				// Extract labels from map
				promLabels := []prompb.Label{{Name: "__name__", Value: metricName}}
				var labelParts []string // For creating the map key

				if !labelsCol.IsNull(r) {
					start := labelsCol.Offsets()[r]
					end := labelsCol.Offsets()[r+1]
					for k := start; k < end; k++ {
						key := keyArray.Value(int(k))
						val := valueArray.Value(int(k))
						promLabels = append(promLabels, prompb.Label{Name: key, Value: val})
						labelParts = append(labelParts, fmt.Sprintf("%s=%s", key, val))
					}
				}

				// Sort label parts for consistent map key
				sort.Strings(labelParts)
				mapKey := metricName + "|" + strings.Join(labelParts, "|")

				// Get or create timeseries in map
				ts, exists := seriesMap[mapKey]
				if !exists {
					// Sort labels before assigning to the timeseries
					sort.Slice(promLabels, func(i, j int) bool {
						return promLabels[i].Name < promLabels[j].Name
					})
					ts = &prompb.TimeSeries{Labels: promLabels}
					seriesMap[mapKey] = ts
				}

				// Append sample
				ts.Samples = append(ts.Samples, prompb.Sample{
					Value:     value,
					Timestamp: timestamp,
				})
			}
		}

		// Add grouped timeseries to the query result
		for _, ts := range seriesMap {
			// Samples should already be sorted by time due to ORDER BY timestamp
			qresults.Timeseries = append(qresults.Timeseries, ts)
		}
	}

	return resp, nil
}

// escapeScopeQLString basic escaping for single quotes in ScopeQL strings
func escapeScopeQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
