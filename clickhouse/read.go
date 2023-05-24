package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/prompb"
)

func (w *ClickHouseAdapter) ReadRequest(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	res := &prompb.ReadResponse{}

	for _, q := range req.Queries {
		qresults := &prompb.QueryResult{}
		res.Results = append(res.Results, qresults)

		sb := &sqlBuilder{}

		sb.Clause("updated_at >= fromUnixTimestamp64Milli(?)", q.StartTimestampMs)

		if q.EndTimestampMs > 0 {
			sb.Clause("updated_at <= fromUnixTimestamp64Milli(?)", q.EndTimestampMs)
		}

		for _, m := range q.Matchers {
			if m.Type == prompb.LabelMatcher_EQ {
				if m.Name == "__name__" {
					sb.Clause("metric_name=?", m.Value)
				} else {
					// TODO: CONSIDER: A way to remove selectors like "remote=clickhouse"
					// --read.ignore-label=remote=clickhouse
					if m.Name == "job" && m.Value == "clickhouse" {
						continue
					}
					if m.Name == "foo" && m.Value == "bar" {
						continue
					}
					sb.Clause("has(labels, ?)", fmt.Sprintf("%s=%s", m.Name, m.Value))
				}
			}
			// for NEQ, use NOT has(labels, 'xxx')
			// for RE, use arrayExists(match(xxx)
			// for NRE, use arrayAll(not match(...))
		}

		rows, err := w.db.QueryContext(ctx, "SELECT metric_name, arraySort(labels) as slb, updated_at, value FROM "+w.table+" WHERE "+sb.Where()+" ORDER BY metric_name, slb, updated_at", sb.Args()...)
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
			rows.Scan(&name, &labels, &updatedAt, &value)

			if thisTimeseries == nil || lastName != name || !slices.Equal(lastLabels, labels) {
				lastName = name
				lastLabels = labels

				thisTimeseries = &prompb.TimeSeries{}
				qresults.Timeseries = append(qresults.Timeseries, thisTimeseries)

				promlabs := []prompb.Label{prompb.Label{Name: "__name__", Value: name}}
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
