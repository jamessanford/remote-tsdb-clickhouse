package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/prompb"
)

func (ch *ClickHouseAdapter) ReadRequest(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	res := &prompb.ReadResponse{}

	for _, q := range req.Queries {
		qresults := &prompb.QueryResult{}
		res.Results = append(res.Results, qresults)

		sb := &sqlBuilder{}

		sb.Clause("updated_at >= fromUnixTimestamp64Milli(?)", q.StartTimestampMs)

		if q.EndTimestampMs > 0 {
			sb.Clause("updated_at <= fromUnixTimestamp64Milli(?)", q.EndTimestampMs)
		}

		if err := addMatcherClauses(q.Matchers, sb, ch.readRequestIgnoreLabel); err != nil {
			return nil, err
		}

		rows, err := ch.db.QueryContext(ctx, "SELECT metric_name, arraySort(labels) as slb, updated_at, value FROM "+ch.table+" WHERE "+sb.Where()+" ORDER BY metric_name, slb, updated_at", sb.Args()...)
		if err != nil {
			return nil, err
		}

		// Fill out a single TimeSeries as long as metric_name and labels are the same.
		var lastName string
		var lastLabels []string
		var thisTimeseries *prompb.TimeSeries

		for rows.Next() {
			var name string
			var labels []string
			var updatedAt time.Time
			var value float64
			err := rows.Scan(&name, &labels, &updatedAt, &value)
			if err != nil {
				return nil, err
			}

			if thisTimeseries == nil || lastName != name || !slices.Equal(lastLabels, labels) {
				lastName = name
				lastLabels = labels

				thisTimeseries = &prompb.TimeSeries{}
				qresults.Timeseries = append(qresults.Timeseries, thisTimeseries)

				promlabs := []prompb.Label{{Name: "__name__", Value: name}}
				for _, label := range labels {
					ln, lv, _ := strings.Cut(label, "=")
					promlabs = append(promlabs, prompb.Label{Name: ln, Value: lv})
				}
				thisTimeseries.Labels = promlabs
			}

			thisTimeseries.Samples = append(thisTimeseries.Samples, prompb.Sample{Value: value, Timestamp: updatedAt.UnixMilli()})
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	return res, nil
}

func addMatcherClauses(matchers []*prompb.LabelMatcher, sb *sqlBuilder, readRequestIgnoreLabel string) error {
	// NOTE: The match() calls use concat() to anchor the regexes to match prometheus behavior.
	for _, m := range matchers {
		if m.Name == "__name__" {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				sb.Clause("metric_name=?", m.Value)
			case prompb.LabelMatcher_NEQ:
				sb.Clause("metric_name!=?", m.Value) // Don't do this.
			case prompb.LabelMatcher_RE:
				sb.Clause("match(metric_name, concat(?, ?, ?))", "^", m.Value, "$")
			case prompb.LabelMatcher_NRE:
				sb.Clause("NOT match(metric_name, concat(?, ?, ?))", "^", m.Value, "$") // Don't do this.
			default:
				return fmt.Errorf("unsupported LabelMatcher_Type %v", m.Type)
			}
		} else {
			label := m.Name + "=" + m.Value
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if label == readRequestIgnoreLabel {
					continue
				}
				sb.Clause("has(labels, ?)", label)
			case prompb.LabelMatcher_NEQ:
				sb.Clause("NOT has(labels, ?)", label)
			case prompb.LabelMatcher_RE:
				sb.Clause("arrayExists(x -> match(x, concat(?, ?, ?)), labels)", "^", label, "$")
			case prompb.LabelMatcher_NRE:
				sb.Clause("NOT arrayExists(x -> match(x, concat(?, ?, ?)), labels)", "^", label, "$")
			default:
				return fmt.Errorf("unsupported LabelMatcher_Type %v", m.Type)
			}
		}
	}
	return nil
}
