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
    INDEX labelset (labels, metric_name) TYPE set(0) GRANULARITY 8192
)
ENGINE = MergeTree
ORDER BY (metric_name, labels, updated_at)
SETTINGS index_granularity = 8192
```

This works well with over 30 billion metrics, even when searching by label,
although cardinality of my dataset is low at 16032 unique metrics+labels.
Including label values, it takes approximately 1 byte per value for my dataset (1 gigabyte per billion metrics)

The `labelset` index granularity is set to 8192 (8192*8192 rows) on purpose for
queries like `has(labels, 'job=omada')` while still providing performance with
many rows that match.

### Configure Prometheus remote writer

In your `prometheus.yaml`:

```
remote_write:
 - url: "http://localhost:9131/write"
```

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

### Query ClickHouse directly with Grafana

Use the [ClickHouse Data Plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) for Grafana pointed at ClickHouse (eg `localhost:8123`)

Queries to get you started:

```
$perSecondColumns(arrayConcat([metric_name], labels), value)
FROM metrics.samples
WHERE
    metric_name='go_memstats_alloc_bytes_total'
    AND has(labels, 'job=omada')
```

```
$perSecondColumns(arrayConcat([metric_name], arrayFilter(x -> x LIKE 'name=%', labels)), value * 8)
FROM metrics.samples
WHERE
    metric_name='omada_station_transmit_bytes_total'
```

```
SELECT
    $timeSeries as t,
    metric_name,
    labels,
    max(value)
FROM $table
WHERE
    metric_name='go_goroutines'
    AND has(labels, 'job=omada')
    AND $timeFilter
GROUP BY
    metric_name,
    labels,
    t
ORDER BY t
```

```
SELECT
    t,
    if(runningDifference(max_0) < 0, nan, runningDifference(max_0) / runningDifference(t / 1000)) AS max_0_Rate
FROM
(
    SELECT
        $timeSeries AS t,
        max(value) as max_0
    FROM $table
    WHERE metric_name='go_memstats_alloc_bytes_total'
    AND has(labels, 'job=omada')
    AND $timeFilter
    GROUP BY t
    ORDER BY t
)
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
