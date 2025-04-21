package scopedb

import (
	"fmt"
	"regexp"
	"strings"

	scopedb "github.com/scopedb/scopedb-sdk/go"
)

// Simple identifier validation (adjust if ScopeDB has different rules)
var scopeDBIdentifier = regexp.MustCompile(`^[a-zA-Z_][0-9a-zA-Z_.]*$`)

type ScopeDBAdapter struct {
	conn            *scopedb.Connection
	table           string // Assumes a single table like "database.schema.table"
	readIgnoreLabel string
	readIgnoreHints bool
	debug           bool
}

type Config struct {
	Endpoint string // e.g., "http://localhost:6543"
	Table    string // e.g., "metrics.prometheus.samples"

	ReadIgnoreLabel string
	ReadIgnoreHints bool
	Debug           bool
}

func NewScopeDBAdapter(config *Config) (*ScopeDBAdapter, error) {
	// Basic validation for table name format
	parts := strings.Split(config.Table, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid table name format: expected 'database.schema.table', got %q", config.Table)
	}
	for _, part := range parts {
		if !scopeDBIdentifier.MatchString(part) {
			return nil, fmt.Errorf("invalid part %q in table name %q: use non-quoted identifiers", part, config.Table)
		}
	}

	conn := scopedb.Open(&scopedb.Config{
		Endpoint: config.Endpoint,
	})

	return &ScopeDBAdapter{
		conn:            conn,
		table:           config.Table,
		readIgnoreLabel: config.ReadIgnoreLabel,
		readIgnoreHints: config.ReadIgnoreHints,
		debug:           config.Debug,
	}, nil
}
