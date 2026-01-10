package ssproc

import "context"

// QueryExecutor abstracts database query execution.
// Implement this to run ssproc queries within your own transactions.
type QueryExecutor interface {
	// Exec executes a query that doesn't return rows.
	Exec(ctx context.Context, sql string, args ...any) (rowsAffected int64, err error)

	// QueryRow executes a query that returns at most one row.
	QueryRow(ctx context.Context, sql string, args ...any) Row

	// Query executes a query that returns rows.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Row represents a single row result from QueryRow.
type Row interface {
	Scan(dest ...any) error
}

// Rows represents multiple row results from Query.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}
