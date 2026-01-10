// Package pgxadapter provides pgx-based implementations of ssproc interfaces.
package pgxadapter

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/rickchristie/ssproc"
)

// PgxTxExecutor wraps pgx.Tx to implement ssproc.QueryExecutor.
type PgxTxExecutor struct {
	tx pgx.Tx
}

var _ ssproc.QueryExecutor = (*PgxTxExecutor)(nil)

// NewFromTx creates a QueryExecutor from a pgx.Tx.
func NewFromTx(tx pgx.Tx) *PgxTxExecutor {
	return &PgxTxExecutor{tx: tx}
}

// Exec executes a query that doesn't return rows.
func (p *PgxTxExecutor) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	res, err := p.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

// QueryRow executes a query that returns at most one row.
func (p *PgxTxExecutor) QueryRow(ctx context.Context, sql string, args ...any) ssproc.Row {
	return p.tx.QueryRow(ctx, sql, args...)
}

// Query executes a query that returns rows.
func (p *PgxTxExecutor) Query(ctx context.Context, sql string, args ...any) (ssproc.Rows, error) {
	rows, err := p.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, nil
}

// PgxConnExecutor wraps pgx.Conn to implement ssproc.QueryExecutor.
type PgxConnExecutor struct {
	conn *pgx.Conn
}

var _ ssproc.QueryExecutor = (*PgxConnExecutor)(nil)

// NewFromConn creates a QueryExecutor from a pgx.Conn.
func NewFromConn(conn *pgx.Conn) *PgxConnExecutor {
	return &PgxConnExecutor{conn: conn}
}

// Exec executes a query that doesn't return rows.
func (p *PgxConnExecutor) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	res, err := p.conn.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

// QueryRow executes a query that returns at most one row.
func (p *PgxConnExecutor) QueryRow(ctx context.Context, sql string, args ...any) ssproc.Row {
	return p.conn.QueryRow(ctx, sql, args...)
}

// Query executes a query that returns rows.
func (p *PgxConnExecutor) Query(ctx context.Context, sql string, args ...any) (ssproc.Rows, error) {
	rows, err := p.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, nil
}

// pgxRows wraps pgx.Rows to implement ssproc.Rows.
type pgxRows struct {
	rows pgx.Rows
}

func (r *pgxRows) Next() bool           { return r.rows.Next() }
func (r *pgxRows) Scan(dest ...any) error { return r.rows.Scan(dest...) }
func (r *pgxRows) Err() error           { return r.rows.Err() }
func (r *pgxRows) Close()               { r.rows.Close() }
