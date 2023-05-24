package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func (w *ClickHouseAdapter) WriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
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
