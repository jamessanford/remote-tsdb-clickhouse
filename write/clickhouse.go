package write

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/prometheus/prompb"
)

var (
	// ClickHouse syntax reference
	// "Non-quoted identifiers must match the regex"
	clickHouseIdentifier = regexp.MustCompile(`^[a-zA-Z_][0-9a-zA-Z_.]*$`)
)

type ClickHouseWriter struct {
	// NOTE: Even though clickhouse.Conn is a 'driver.Conn', when using clickhouse Open() directly, PrepareBatch and Query handle concurrency.
	// So we probably don't need sql.DB.
	db    *sql.DB
	table string
}

func NewClickHouseWriter(address, table string) (*ClickHouseWriter, error) {
	if !clickHouseIdentifier.MatchString(table) {
		return nil, fmt.Errorf("invalid table name: use non-quoted identifier")
	}

	// TODO: Move this to a separate function so that people can have control -- just pass us a clickhouse.Conn
	// NewDefaultConnection(), NewClickhouseWriter(write.NewDefaultConnection(), table)
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug:       true,
		DialTimeout: 5 * time.Second,
		//		MaxOpenConns:    16,
		//		MaxIdleConns:    1,
		//		ConnMaxLifetime: time.Hour,
	})
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &ClickHouseWriter{db: db, table: table}, nil
}

func (w *ClickHouseWriter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	tx, err := w.db.Begin()
	if err != nil {
		return 0, err
	}

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s", w.table)) // FIXME
	if err != nil {
		tx.Rollback() // TODO: Use a defer with !commitDone{} check
		return 0, err
	}
	defer stmt.Close()

	count := 0

	for _, t := range req.Timeseries {
		var name string
		labels := make([]string, 0, len(t.Labels))

		for _, l := range t.Labels {
			if l.Name == "__name__" {
				name = l.Value
				continue
			}
			labels = append(labels, l.Name+"="+l.Value)
		}

		count += len(t.Samples)
		for _, s := range t.Samples {
			stmt.Exec(
				time.UnixMilli(s.Timestamp).UTC(),
				name,
				labels,
				s.Value,
			)
		}
	}
	return count, tx.Commit()
}

func (w *ClickHouseWriter) ReadRequest(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
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
					sb.Clause("has(labels, ?", fmt.Sprintf("%s=%s", m.Name, m.Value))
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
