package main

import (
	"flag"
	"io"
	"net/http"
	"strings"

	"github.com/jamessanford/remote-tsdb-clickhouse/write"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	samplesWrittenTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "samples_written_total",
			Help: "number of samples written into clickhouse",
		})
	writeRequestsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "write_requests_total",
			Help: "number of hits to write endpoint",
		})
)

func init() {
	prometheus.MustRegister(samplesWrittenTotal)
	prometheus.MustRegister(writeRequestsTotal)
}

func main() {
	var httpAddr string
	var clickAddr string
	var table string
	flag.StringVar(&httpAddr, "http", "9131", "listen on this [address:]port")
	flag.StringVar(&clickAddr, "db", "127.0.0.1:9000", "ClickHouse DB at this address:port")
	flag.StringVar(&table, "table", "metrics.samples", "write to this database.tablename")
	flag.Parse()

	if !strings.Contains(httpAddr, ":") {
		httpAddr = ":" + httpAddr
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	ch, err := write.NewClickHouseWriter(clickAddr, table)
	if err != nil {
		logger.Fatal("NewClickHouseWriter", zap.Error(err))
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "remote-clickhouse")
		r.Body.Close()
	})

	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		writeRequestsTotal.Inc()
		defer r.Body.Close()
		req, err := DecodeWriteRequest(r.Body)
		if err != nil {
			logger.Error("DecodeWriteRequest", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if count, err := ch.WriteRequest(r.Context(), req); err != nil {
			logger.Error("WriteRequest", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else if count > 0 {
			samplesWrittenTotal.Add(float64(count))
		}
	})

	logger.Info(
		"listening",
		zap.String("listen", httpAddr),
		zap.String("db", clickAddr),
		zap.String("table", table),
	)

	if err := http.ListenAndServe(httpAddr, nil); err != nil {
		logger.Fatal("ListenAndServe", zap.Error(err))
	}
}
