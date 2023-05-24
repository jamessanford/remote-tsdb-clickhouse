package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func (ch *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	commitDone := false

	tx, err := ch.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() {
		if !commitDone {
			tx.Rollback()
		}
	}()

	// NOTE: Value of ch.table is sanitized in NewClickHouseAdapter.
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s", ch.table))
	if err != nil {
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

	err = tx.Commit()
	commitDone = true
	return count, err
}
