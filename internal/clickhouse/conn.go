package clickhouse

import (
	"database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouse syntax reference
// "Non-quoted identifiers must match the regex"
var clickHouseIdentifier = regexp.MustCompile(`^[a-zA-Z_][0-9a-zA-Z_.]*$`)

type ClickHouseAdapter struct {
	// NOTE: We switched to sql.DB, but clickhouse.Conn appears to handle
	// PrepareBatch and Query correctly with multiple goroutines, despite
	// technically being a "driver.Conn"
	db    *sql.DB
	table string
}

func NewClickHouseAdapter(address, table string) (*ClickHouseAdapter, error) {
	if !clickHouseIdentifier.MatchString(table) {
		return nil, fmt.Errorf("invalid table name: use non-quoted identifier")
	}

	// TODO: Move this to a separate function for flag control.
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug:       true,
		DialTimeout: 5 * time.Second,
		//MaxOpenConns:    16,
		//MaxIdleConns:    1,
		//ConnMaxLifetime: time.Hour,
	})
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(16)
	db.SetConnMaxLifetime(time.Hour)

	// Immediately try to connect with the provided credentials, fail fast.
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &ClickHouseAdapter{db: db, table: table}, nil
}
