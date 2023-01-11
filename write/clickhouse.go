package write

import (
	"context"
	"fmt"
	"regexp"
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
	conn  clickhouse.Conn
	table string
}

func NewClickHouseWriter(address, table string) (*ClickHouseWriter, error) {
	if !clickHouseIdentifier.MatchString(table) {
		return nil, fmt.Errorf("invalid table name: use non-quoted identifier")
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug:           false,
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
