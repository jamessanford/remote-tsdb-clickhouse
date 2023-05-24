package write

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

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
		//		Debug:           false,
		Debug: true,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
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
		tx.Rollback()  // TODO: Use a defer with !commitDone{} check
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

	// TODO: Move to sql.DB...
	for _, q := range req.Queries {
		qresults := &prompb.QueryResult{}
		res.Results = append(res.Results, qresults)

		var cQuery []string
		var cData []any

		cQuery = append(cQuery, "updated_at >= fromUnixTimestamp64Milli(?)")
		cData = append(cData, q.StartTimestampMs)
		if q.EndTimestampMs > 0 {
			cQuery = append(cQuery, "updated_at <= fromUnixTimestamp64Milli(?)")
			cData = append(cData, q.EndTimestampMs)
		}

		for _, m := range q.Matchers {
			if m.Type == prompb.LabelMatcher_EQ {
				if m.Name == "__name__" {
					cQuery = append(cQuery, "metric_name=?")
					cData = append(cData, m.Value)
				} else {
					if m.Name == "job" && m.Value == "clickhouse" {
						continue
					}
					cQuery = append(cQuery, "has(labels, ?)")
					cData = append(cData, fmt.Sprintf("%s=%s", m.Name, m.Value))
				}
			}
			// for NEQ, use NOT has(labels, 'xxx')
			// for RE, use arrayExists(match(xxx)
			// for NRE, use arrayAll(not match(...))
		}

		rows, err := w.db.QueryContext(ctx, "SELECT metric_name, arraySort(labels) as slb, updated_at, value FROM " + w.table + " WHERE "+strings.Join(cQuery, " AND ")+" ORDER BY metric_name, slb, updated_at", cData...)
		if err != nil {
			return nil, err
		}

		// Use hasAll() for labels?

		// CONSIDER: A way to remove selectors like "remote=clickhouse"
		// --read.ignore-label=remote=clickhouse

		// As long as metric_name and labels are the same, we can fill out a single TimeSeries
		// and just add Samples
		// slices.Equal
		// (should we slices.Sort?  might be better to have clickhouse do it.  but also it might just be the same)
		for rows.Next() {
			var name string
			var labels []string
			var updatedAt time.Time
			var value float64
			rows.Scan(&name, &labels, &updatedAt, &value)
			fmt.Printf("ROW %q %q %q %f\n", name, labels, updatedAt, value)
		}

		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	return res, nil
}
