package pg

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rickchristie/ssproc/plugs"
	"github.com/rickchristie/ssproc/util"
	"time"
)

// ConnTxHelper represents a transaction and contains helper utility methods for AccessorReader implementations
// utilizing postgresql.
type ConnTxHelper struct {
	Ctx     context.Context
	Tx      pgx.Tx
	Logger  plugs.Logger
	TraceId string
	cancel  context.CancelFunc
	conn    *pgx.Conn
}

func NewConnTxHelper(
	ctx context.Context,
	traceId string,
	connString string,
	logger plugs.Logger,
	readOnly bool,
) (
	*ConnTxHelper,
	error,
) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, util.WrapErr(err, true)
	}

	// Don't need to ping because we're going to start sending commands to set connection & transaction config.
	// By default, all our transactions should have their context cancelled after 10 seconds. This prevents hanging
	// connection in postgres when there's a bug in our server code. However, for testing, we allow context to be
	// alive for longer than this, because we need to keep connection alive when debugging.
	// TODO: Add check to panic if override is done for non-testing.
	timeout := DefaultTxTimeout
	val := ctx.Value(timeoutKey)
	if override, ok := val.(time.Duration); ok {
		timeout = override
	}
	txCtx, cancel := context.WithTimeout(ctx, timeout)

	// DEFAULT CONNECTION SETTINGS
	//
	// Ideally transaction timeout is also set in the Postgres database configuration, however we can't change
	// the setting in our RDS until we migrated all our Python codebase.
	// See: https://postgresqlco.nf/doc/en/param/idle_in_transaction_session_timeout/
	// See: https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-IDLE-IN-TRANSACTION-SESSION-TIMEOUT
	// TODO PYTHON: Update RDS idle_in_transaction_session_timeout for us after migration is done.
	_, err = conn.Exec(txCtx, fmt.Sprintf(
		`SET idle_in_transaction_session_timeout = %v`,
		timeout.Milliseconds(),
	))
	if err != nil {
		defer cancel()
		defer closeConn(traceId, logger, conn)

		err = util.WrapErr(err, true)
		logger.Fatal(
			traceId, "failed to start transaction (non-pool) (set idle_in_transaction_session_timeout)",
			map[string]any{"err": err},
		)
		return nil, err
	}

	// Both Context and Postgres is informed of the timeout for the current session.
	opts := StandardTxOptions(readOnly)

	tx, err := conn.BeginTx(txCtx, opts)
	if err != nil {
		defer cancel()
		defer closeConn(traceId, logger, conn)

		err = util.WrapErr(err, true)
		logger.Fatal(traceId, "Failed to start transaction (non-pool) (begin tx)", map[string]any{"err": err})
		return nil, err
	}

	helper := &ConnTxHelper{
		TraceId: traceId,
		Ctx:     txCtx,
		Tx:      tx,
		Logger:  logger,
		conn:    conn,
		cancel:  cancel,
	}

	// Defensive coding, to ensure consistent returns so our parsing doesn't need to change.
	_, err = tx.Exec(txCtx, `SET DateStyle = 'ISO, YMD';`)
	if err != nil {
		defer helper.Rollback()
		return nil, util.WrapErr(err, true)
	}

	return helper, nil
}

func (t *ConnTxHelper) close() {
	if t.Tx != nil {
		closeConn(t.TraceId, t.Logger, t.Tx.Conn())
	}
	if t.conn != nil {
		closeConn(t.TraceId, t.Logger, t.conn)
	}
}

func closeConn(traceId string, logger plugs.Logger, conn *pgx.Conn) {
	// The connection is likely already closed due to the context cancelling, however, we want to ensure connections
	// are closed, so there are no hanging connections.
	err := conn.Close(context.Background())
	if err != nil {
		if err.Error() != "conn closed" {
			logger.Fatal(
				traceId, "Fatal! Failed to close connection!",
				map[string]any{"err": util.WrapErr(err, true)},
			)
		}
	}
}

func (t *ConnTxHelper) Commit() error {
	defer t.cancel()
	defer t.close()

	err := t.Tx.Commit(t.Ctx)
	if err != nil {
		// This means that Tx is already closed. From the documentation, it is safe to call multiple times & means
		// we don't need to return error.
		if errors.Is(pgx.ErrTxClosed, err) {
			return nil
		}

		// Means commit is failed & return error with log.
		t.Logger.Fatal(t.TraceId, "Fatal! Failed to commit transaction!", map[string]any{"err": err})
		return util.WrapErr(err, true)
	}

	return nil
}

func (t *ConnTxHelper) Rollback() {
	defer t.cancel()
	defer t.close()

	// We don't want to rollback with this TxHelper's context, because there's a possibility the helper context is
	// already expired. We don't know the behavior of pgx (no time to research), it could be the rollback will
	// fail simply because the context has failed, so the transaction would be left hanging. Just to be safe, we
	// create our own timeout context for rolling back.

	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := t.Tx.Rollback(rollbackCtx)
	if err != nil {
		// From the documentation, it is safe to call multiple times & means
		// we don't need to return error.
		if errors.Is(err, pgx.ErrTxClosed) {
			return
		}

		// Sometimes pgx will return pgconn.connLockError with status: "conn closed".
		//		return &connLockError{status: "conn closed"}
		// Because this means the connection is already closed, then we don't need to alert with a fatal error.
		// See: pgconn/pgconn.go
		if err.Error() == "conn closed" {
			return
		}

		// This is the same case, we don't need to trigger fatal error in this case.
		if err.Error() == "failed to deallocate cached statement(s): conn closed" {
			return
		}

		// Means failed to rollback.
		t.Logger.Fatal(
			t.TraceId, "Fatal! Failed to rollback transaction!",
			map[string]any{"err": util.WrapErr(err, true)},
		)
	}
}

// AffectedOneRow will return an error if unable to fetch affected rows or the affected rows is != 1.
func (t *ConnTxHelper) AffectedOneRow(res pgconn.CommandTag) error {
	return t.AffectedRows(res, 1)
}

// AffectedRows will return an error if unable to fetch affected rows or the affected rows is not as expected.
func (t *ConnTxHelper) AffectedRows(res pgconn.CommandTag, expected int64) error {
	n := res.RowsAffected()
	if n != expected {
		msg := fmt.Sprintf("unexpected %v rows affected, found %v", expected, n)
		return util.Err(msg, true)
	}

	return nil
}
