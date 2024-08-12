package pg

import (
	"github.com/jackc/pgx/v5"
	"time"
)

type dbContextKey string

// timeoutKey is used to override default transaction context timeout time at NewTxHelper and NewConnTxHelper.
var timeoutKey dbContextKey = "_to"

const DefaultTxTimeout = 10 * time.Second

func StandardTxOptions(readOnly bool) pgx.TxOptions {
	opts := pgx.TxOptions{
		// See: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-REPEATABLE-READ
		IsoLevel:       pgx.ReadCommitted,
		DeferrableMode: pgx.NotDeferrable,
	}

	if readOnly {
		opts.AccessMode = pgx.ReadOnly
	} else {
		// This will fail in the beginning of the transaction if the request is sent to a read-only database instance.
		// Which is what we want.
		opts.AccessMode = pgx.ReadWrite
	}

	return opts
}
