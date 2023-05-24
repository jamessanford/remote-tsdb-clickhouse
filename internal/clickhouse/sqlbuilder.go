package clickhouse

import (
	"strings"
)

// sqlBuilder helps generate WHERE clauses from external data.

type sqlBuilder struct {
	clause []string
	args   []any
}

func (sb *sqlBuilder) Clause(query string, args ...any) {
	sb.clause = append(sb.clause, query)
	sb.args = append(sb.args, args...)
}

func (sb *sqlBuilder) Where() string {
	return strings.Join(sb.clause, " AND ")
}

func (sb *sqlBuilder) Args() []any {
	return sb.args
}
