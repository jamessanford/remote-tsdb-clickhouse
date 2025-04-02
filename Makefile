all: build-linux-amd64 build-linux-arm64

build-linux-amd64:
	mkdir -p build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o build/remote-tsdb-clickhouse_linux_amd64 main.go

build-linux-arm64:
	mkdir -p build
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o build/remote-tsdb-clickhouse_linux_arm64 main.go

gox-linux:
	gox -osarch="linux/amd64 linux/arm64" -output="build/remote-tsdb-clickhouse_{{.OS}}_{{.Arch}}"

gox-all:
	gox -osarch="linux/amd64 linux/arm64" -output="build/remote-tsdb-clickhouse_{{.OS}}_{{.Arch}}"

clean:
	rm -f build/remote-tsdb-clickhouse_*
