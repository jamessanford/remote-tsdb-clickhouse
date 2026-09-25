remote-tsdb-clickhouse stores timeseries data in ClickHouse.

Implements both Prometheus remote writer (to store metrics) and
Prometheus remote reader (use metrics from ClickHouse directly in Prometheus)

### Install

```
go install github.com/jamessanford/remote-tsdb-clickhouse@latest
```

### Create destination table

Use `clickhouse client` to create this table:

```
CREATE TABLE metrics.samples
(
    `updated_at` DateTime CODEC(DoubleDelta, LZ4),
    `metric_name` LowCardinality(String),
    `labels` Array(LowCardinality(String)),
    `value` Float64 CODEC(Gorilla, LZ4),
    INDEX labelset_bf labels TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY (metric_name, labels, updated_at)
SETTINGS index_granularity = 8192
```

This works well with over 100 billion metrics, even when searching by label,
although cardinality of my dataset is very low at 16032 unique metrics+labels.
Including label values, it takes approximately 1 byte per value for my dataset (1 gigabyte per billion metrics)

Storing and indexing the labels array directly is a naive implementation,
setups with millions of unique metrics will need more advanced setups with
a label hash or fingerprint and a separate lookup table.

### Configure Prometheus remote writer

In your `prometheus.yaml`:

```
remote_write:
 - url: "http://localhost:9131/write"
   queue_config:
     capacity: 400000
     max_samples_per_send: 40000
     batch_send_deadline: 1m
```

ClickHouse prefers fewer writes with more samples per write.  You may need to adjust `capacity`, `max_samples_per_send`, and `batch_send_deadline` as per [Prometheus Remote Write Tuning](https://prometheus.io/docs/practices/remote_write/) if you see "Too many parts" errors or `prometheus_remote_storage_samples_pending` keeps growing.

### Configure Prometheus remote reader

In your `prometheus.yaml`:

```
remote_read:
 - url: "http://localhost:9131/read"
```

### Query data with Prometheus

The above configuration will use `remote-tsdb-clickhouse` to backfill
data not present in Prometheus.

If you'd like to query `remote-tsdb-clickhouse` immediately, consider
this configuration:

```
remote_read:
 - url: "http://localhost:9131/read"
   read_recent: true
   name: clickhouse
   required_matchers:
     remote: clickhouse
```

Then issue queries with the added `{remote="clickhouse"}` label.

`remote-tsdb-clickhouse` will remove the `{remote="clickhouse"}` label
from incoming requests by default, see `--help`.

### Query directly with Grafana

I recommend querying through Prometheus `remote_read`, but it is possible to read the ClickHouse
data directly from Grafana with the [ClickHouse Data Plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/)

Examples of using ClickHouse data plugin instead of `remote_read`:

```
$__columns(updated_at,
           arrayStringConcat(arrayConcat([metric_name], labels), ' '),
           argMax(value, updated_at) AS value
)
FROM metrics.samples
WHERE
    metric_name='go_goroutines'
```

```
$__perSecondColumns(updated_at,
                    arrayStringConcat(arrayConcat([metric_name], labels), ' '),
                    value
)
FROM metrics.samples
WHERE
    metric_name='go_memstats_alloc_bytes_total'
    AND has(labels, 'job=omada')
```


### Importing existing data

You may export TSDB data from Prometheus and reinsert it into ClickHouse.

Use a [modified `promtool` command](https://github.com/prometheus/prometheus/compare/main...jamessanford:prometheus:jamessanford/promtool-clickhouse) to dump one day at a time.

Note that `promtool tsdb` writes to your TSDB directory, so run it
against a read-only snapshot.


```
promtool tsdb dump \
  --min-time=$(date -u -d '2021-12-16' +%s)001 \
  --max-time=$(date -u -d '2021-12-17' +%s)000 \
  /zfs/tsdbsnap1/jsanford/prom2/bin/data \
|  clickhouse client \
  --query 'INSERT INTO metrics.samples FORMAT TabSeparated'
```

You may significantly speed up the bulk import by running many in parallel.

Importing one day a time makes it easy to delete and reimport data, eg

```
ALTER TABLE metrics.samples DELETE WHERE updated_at > 1656806400 AND updated_at <= 1656892800
```

Let ClickHouse settle for 30 minutes or so after bulk importing data
before determining what CPU usage will look like long term.
