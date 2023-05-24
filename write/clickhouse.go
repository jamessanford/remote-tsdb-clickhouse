package write

import (
	"context"
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
	// FIXME BUG: Conn is a connection to a database. It is not used concurrently by multiple goroutines.
	// (So, we should really open on each time, driver handles pooling)
	// FIXME: Use clickhouse.OpenDB and save the DB here, then use PrepareContext and Exec(allrows)
	conn  clickhouse.Conn
	table string
}

func NewClickHouseWriter(address, table string) (*ClickHouseWriter, error) {
	if !clickHouseIdentifier.MatchString(table) {
		return nil, fmt.Errorf("invalid table name: use non-quoted identifier")
	}

	// TODO: Move this to a separate function so that people can have control -- just pass us a clickhouse.Conn
	// NewDefaultConnection(), NewClickhouseWriter(write.NewDefaultConnection(), table)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
//		Debug:           false,
                Debug: true,
                Debugf: func(format string, v ...any) {
                        fmt.Printf(format+"\n", v)
                },
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    16,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}

	return &ClickHouseWriter{conn: conn, table: table}, nil
}

func (w *ClickHouseWriter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	// NOTE: We sanitize w.table, but there must be a way to quote it.
	batch, err := w.conn.PrepareBatch(ctx,
		fmt.Sprintf("INSERT INTO %s", w.table),
	)
	if err != nil {
		return 0, err
	}

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
			err := batch.Append(
				time.UnixMilli(s.Timestamp).UTC(),
				name,
				labels,
				s.Value,
			)
			if err != nil {
				return 0, err
			}

		}
	}
	return count, batch.Send()
}

func (w *ClickHouseWriter) ReadRequest(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	res := &prompb.ReadResponse{}

	// TODO: Move to sql.DB...
	for _, q := range req.Queries {
		qresults := &prompb.QueryResult{}
		res.Results = append(res.Results, qresults)

		var clause []string

		// FIXME: Use clauseData for the %d/%s args, remove Sprintf
		clause = append(clause, fmt.Sprintf("updated_at >= %d", q.StartTimestampMs))
		if q.EndTimestampMs > 0 {
			clause = append(clause, fmt.Sprintf("updated_at <= %d", q.EndTimestampMs))
		}

		// FIXME: Use clauseData for the %d/%s args, remove Sprintf
		for _, m := range q.Matchers {
			if m.Type == prompb.LabelMatcher_EQ {
				if m.Name == "__name__" {
					clause = append(clause, fmt.Sprintf("metric_name='%s'", m.Value))
				} else {
					clause = append(clause, fmt.Sprintf(`has(labels, "%s=%s")`, m.Name, m.Value))
				}
			}
			// for NEQ, use NOT has(labels, 'xxx')
			// for RE, use arrayExists(match(xxx)
			// for NRE, use arrayAll(not match(...))
		}

		rows, err := w.conn.Query(ctx, "SELECT metric_name, arraySort(labels) as slb, updated_at, value FROM ? WHERE ? ORDER BY metric_name, slb, updated_at", w.table, strings.Join(clause, " AND "))
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
			rows.Scan() // ...
			//			qresults.append(&prompb.TimeSeries{Label: [], Sample: [timestamp, value]}
		}

		if err := rows.Err(); err != nil {
			panic(err)
		}
	}

	return res, nil
}
