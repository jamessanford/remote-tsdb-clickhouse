FROM alpine:3.18.0
ARG TARGETOS
ARG TARGETARCH
COPY build/remote-tsdb-clickhouse_${TARGETOS}_${TARGETARCH} /usr/local/bin/remote-tsdb-clickhouse
RUN chmod +x /usr/local/bin/remote-tsdb-clickhouse
