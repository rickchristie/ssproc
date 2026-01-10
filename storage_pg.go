package ssproc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	interr "github.com/rickchristie/ssproc/internal/errors"
	"github.com/rickchristie/ssproc/internal/timeutil"
)

var _ Storage = (*PgStorage)(nil)

// PgStorageConfig contains configuration for PgStorage.
type PgStorageConfig struct {
	// ConnStrSelector selects a connection string from the pool.
	ConnStrSelector Selector[string]
	// Schema is the database schema name.
	Schema string
	// Table is the table name.
	Table string
	// Location is the timezone for timestamps. Defaults to UTC.
	Location *time.Location
	// Logger is the logger. Defaults to DefaultLogger.
	Logger Logger
	// TxTimeout is the transaction timeout. Defaults to 5s.
	TxTimeout time.Duration
}

// PgStorage is a PostgreSQL implementation of Storage.
type PgStorage struct {
	connStr   Selector[string]
	table     string
	schema    string
	logger    Logger
	txTimeout time.Duration
	utilTime  timeutil.Time

	// Test logging support
	_mustSendTestLog bool
	_startTime       time.Time
	_cTestLog        chan *testLog
}

// NewPgStorage creates a new PgStorage.
func NewPgStorage(cfg PgStorageConfig) (*PgStorage, error) {
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	if cfg.Logger == nil {
		cfg.Logger = DefaultLogger("PgStorage")
	}
	if cfg.TxTimeout == 0 {
		cfg.TxTimeout = 5 * time.Second
	}

	ret := &PgStorage{
		connStr:   cfg.ConnStrSelector,
		schema:    cfg.Schema,
		table:     cfg.Table,
		logger:    cfg.Logger,
		txTimeout: cfg.TxTimeout,
		utilTime:  timeutil.NewGlobalTime(cfg.Location),
	}

	err := ret.verifySchema()
	if err != nil {
		return nil, err
	}

	err = ret.verifyIndex()
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// NewPgStorageSimple creates a new PgStorage with simple parameters.
func NewPgStorageSimple(connStr Selector[string], schemaName, tableName string) (*PgStorage, error) {
	return NewPgStorage(PgStorageConfig{
		ConnStrSelector: connStr,
		Schema:          schemaName,
		Table:           tableName,
	})
}

func (s *PgStorage) beginTx(ctx context.Context) (
	tx pgx.Tx,
	timeoutCtx context.Context,
	cancelCtxAndConn func(),
	err error,
) {
	timeoutCtx, cancelCtx := context.WithTimeout(ctx, s.txTimeout)

	conn, err := pgx.Connect(timeoutCtx, s.connStr.Get())
	if err != nil {
		defer cancelCtx()
		return nil, nil, nil, interr.Wrap(err, true)
	}

	cancelCtxAndConn = func() {
		defer cancelCtx()
		err = conn.Close(timeoutCtx)
		if err != nil {
			if errors.Is(err, pgx.ErrTxClosed) == false {
				s.logger.Error("failed to close connection for ssproc", "error", err.Error())
			}
		}
	}

	_, err = conn.Exec(timeoutCtx, fmt.Sprintf(
		`SET idle_in_transaction_session_timeout = %v`,
		s.txTimeout.Milliseconds(),
	))
	if err != nil {
		defer cancelCtxAndConn()
		return nil, nil, nil, interr.Wrap(err, true)
	}

	opts := pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		DeferrableMode: pgx.NotDeferrable,
		AccessMode:     pgx.ReadWrite,
	}
	tx, err = conn.BeginTx(timeoutCtx, opts)
	if err != nil {
		defer s.rollback(timeoutCtx, cancelCtxAndConn, tx)
		return nil, nil, nil, interr.Wrap(err, true)
	}

	return tx, timeoutCtx, cancelCtxAndConn, nil
}

func (s *PgStorage) rollback(ctx context.Context, cancelCtxAndConn func(), tx pgx.Tx) {
	defer cancelCtxAndConn()

	if tx == nil {
		return
	}

	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := tx.Rollback(rollbackCtx)
	if err != nil {
		if errors.Is(err, pgx.ErrTxClosed) {
			return
		}
		if err.Error() == "conn closed" {
			return
		}
		msg := fmt.Sprintf("failed to rollback for table: %v.%v", s.schema, s.table)
		s.logger.Error(msg, "error", err.Error())
	}
}

type testLogs map[string][]*testLog

func (t testLogs) printAll() {
	for jobId, sl := range t {
		msg := "\n" + jobId
		if len(sl) > 0 {
			msg += " (" + sl[0].procId + ")"
		}
		fmt.Println(msg)
		for _, log := range sl {
			fmt.Println(log.msg)
		}
		fmt.Println("Done")
	}
}

func (t testLogs) printJobs(jobId string) {
	msg := "\n" + jobId
	if len(t[jobId]) > 0 {
		msg += " (" + t[jobId][0].procId + ")"
	}
	fmt.Println(msg)
	for _, log := range t[jobId] {
		fmt.Println(log.msg)
	}
	fmt.Println("Done")
}

type testLog struct {
	jobId  string
	msg    string
	procId string
}

func (s *PgStorage) _sendTestLog(buffer int) {
	s._mustSendTestLog = true
	s._startTime = s.utilTime.Now()
	s._cTestLog = make(chan *testLog, buffer)
}

func (s *PgStorage) _getTestLogs() testLogs {
	logs := make(map[string][]*testLog)

Loop:
	for {
		select {
		case log := <-s._cTestLog:
			logs[log.jobId] = append(logs[log.jobId], log)
		default:
			break Loop
		}
	}

	return logs
}

func (s *PgStorage) _printTs(t time.Time) string {
	if t.IsZero() {
		return "<nil>"
	}
	return fmt.Sprintf("%v", t.Sub(s._startTime))
}

type pgColumn struct {
	columnName    string
	isNullable    bool
	dataType      string
	columnDefault string
	charMaxLength int64
}

func (p *pgColumn) isEqual(schema, table string, a *pgColumn) (errMsg string) {
	if a == nil {
		return fmt.Sprintf("%v.%v: column %v does not exist in table", schema, table, p.columnName)
	}
	if p.columnName != a.columnName {
		return fmt.Sprintf("%v.%v: expected column name to be %v", schema, table, p.columnName)
	}
	if p.isNullable != a.isNullable {
		return fmt.Sprintf("%v.%v: expected column %v nullable to be %v, found %v", schema, table, p.columnName, p.isNullable, a.isNullable)
	}
	if p.dataType != a.dataType {
		return fmt.Sprintf("%v.%v: expected column %v dataType to be %v, found %v", schema, table, p.columnName, p.dataType, a.dataType)
	}
	if p.columnDefault != a.columnDefault {
		return fmt.Sprintf("%v:%v expected column %v default value to be %v, found %v", schema, table, p.columnName, p.columnDefault, a.columnDefault)
	}
	if p.charMaxLength != a.charMaxLength {
		return fmt.Sprintf("%v:%v expected column %v char max length to be %v, found %v", schema, table, p.columnName, p.charMaxLength, a.charMaxLength)
	}
	return ""
}

var requiredCols = []*pgColumn{
	{columnName: "job_id", isNullable: false, dataType: "text", columnDefault: "", charMaxLength: 0},
	{columnName: "job_data", isNullable: false, dataType: "text", columnDefault: "''::text", charMaxLength: 0},
	{columnName: "process_id", isNullable: false, dataType: "text", columnDefault: "", charMaxLength: 0},
	{columnName: "goroutine_id", isNullable: false, dataType: "character varying", columnDefault: "''::character varying", charMaxLength: 512},
	{columnName: "goroutine_heart_beat_ts", isNullable: true, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
	{columnName: "goroutine_lease_expire_ts", isNullable: true, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
	{columnName: "status", isNullable: false, dataType: "character varying", columnDefault: "'ready'::character varying", charMaxLength: 64},
	{columnName: "next_subprocess", isNullable: false, dataType: "integer", columnDefault: "0", charMaxLength: 0},
	{columnName: "run_type", isNullable: false, dataType: "character varying", columnDefault: "'normal'::character varying", charMaxLength: 64},
	{columnName: "goroutine_ids", isNullable: false, dataType: "ARRAY", columnDefault: "'{}'::text[]", charMaxLength: 0},
	{columnName: "exec_count", isNullable: false, dataType: "integer", columnDefault: "0", charMaxLength: 0},
	{columnName: "comp_count", isNullable: false, dataType: "integer", columnDefault: "0", charMaxLength: 0},
	{columnName: "created_ts", isNullable: false, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
	{columnName: "start_after_ts", isNullable: false, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
	{columnName: "started_ts", isNullable: true, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
	{columnName: "end_ts", isNullable: true, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
	{columnName: "last_update_ts", isNullable: false, dataType: "timestamp with time zone", columnDefault: "", charMaxLength: 0},
}

func (s *PgStorage) verifySchema() error {
	tx, ctx, cancel, err := s.beginTx(context.Background())
	if err != nil {
		return err
	}
	defer s.rollback(ctx, cancel, tx)

	query := `SELECT
			column_name, is_nullable::bool, data_type, column_default,
			character_maximum_length
		FROM
			information_schema.columns
		WHERE
			table_schema = $1
			AND table_name = $2;`
	rows, err := tx.Query(ctx, query, s.schema, s.table)
	if err != nil {
		return interr.Wrap(err, true)
	}
	defer rows.Close()

	cols := make(map[string]*pgColumn)
	for rows.Next() {
		col := pgColumn{}
		colDefault := sql.NullString{}
		maxLength := sql.NullInt64{}
		err = rows.Scan(&col.columnName, &col.isNullable, &col.dataType, &colDefault, &maxLength)
		if err != nil {
			return interr.Wrap(err, true)
		}
		col.columnDefault = colDefault.String
		col.charMaxLength = maxLength.Int64
		cols[col.columnName] = &col
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	for _, req := range requiredCols {
		found := cols[req.columnName]
		errMsg := req.isEqual(s.schema, s.table, found)
		if errMsg != "" {
			return interr.Err(errMsg, true)
		}
	}

	return nil
}

var requiredIndexes = map[string]string{
	"%v_pkey":        "CREATE UNIQUE INDEX %v_pkey ON %v.%v USING btree (job_id)",
	"%v_expired":     "CREATE INDEX %v_expired ON %v.%v USING btree (process_id, status, start_after_ts, goroutine_lease_expire_ts)",
	"%v_cleanup_idx": "CREATE INDEX %v_cleanup_idx ON %v.%v USING btree (process_id, status, end_ts)",
}

func (s *PgStorage) verifyIndex() error {
	tx, ctx, cancel, err := s.beginTx(context.Background())
	if err != nil {
		return err
	}
	defer s.rollback(ctx, cancel, tx)

	query := `SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2`
	rows, err := tx.Query(ctx, query, s.schema, s.table)
	if err != nil {
		return interr.Wrap(err, true)
	}
	defer rows.Close()

	indexes := make(map[string]string)
	for rows.Next() {
		var name, def string
		err = rows.Scan(&name, &def)
		if err != nil {
			return interr.Wrap(err, true)
		}
		indexes[name] = def
	}
	err = rows.Err()
	if err != nil {
		return interr.Wrap(err, true)
	}

	if len(indexes) == 0 {
		return interr.Err(fmt.Sprintf("%v.%v: no index found! expected 1 primary key index", s.schema, s.table), true)
	}

	if len(indexes) != 3 {
		return interr.Err(fmt.Sprintf("%v.%v: multiple indexes found, expected three indexes", s.schema, s.table), true)
	}

	for rawName, pattern := range requiredIndexes {
		indexName := fmt.Sprintf(rawName, s.table)
		foundDef := indexes[indexName]
		if foundDef == "" {
			return interr.Err(fmt.Sprintf("%v.%v: index_name %v not found", s.schema, s.table, indexName), true)
		}
		expectedDef := fmt.Sprintf(pattern, s.table, s.schema, s.table)
		if foundDef != expectedDef {
			return interr.Err(fmt.Sprintf("%v.%v: expected index_name %v to be: %v, found: %v", s.schema, s.table, indexName, expectedDef, foundDef), true)
		}
	}

	return nil
}

func (s *PgStorage) RegisterJob(ctx context.Context, job *Job) error {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer s.rollback(ctx, cancel, tx)

	err = s.registerJobImpl(ctx, &pgxQueryExecutor{tx: tx}, job)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return interr.Wrap(err, true)
	}

	return nil
}

func (s *PgStorage) RegisterJobTx(ctx context.Context, qe QueryExecutor, job *Job) error {
	return s.registerJobImpl(ctx, qe, job)
}

func (s *PgStorage) registerJobImpl(ctx context.Context, qe QueryExecutor, job *Job) error {
	if job.Status != JSReady {
		return interr.Err("job must be registered with 'ready' status", true)
	}

	if job.ProcessId == "" {
		return interr.Err("process ID must not be empty!", true)
	}

	if job.GoroutineLeaseExpireTs.IsZero() == false &&
		job.GoroutineHeartBeatTs.Before(job.GoroutineLeaseExpireTs) == false {
		return interr.Err("heartbeat ts >= leaseExpireTs", true)
	}

	query := fmt.Sprintf(`INSERT INTO
			%v.%v (
			    job_id, job_data, process_id, goroutine_id, goroutine_heart_beat_ts,
			    goroutine_lease_expire_ts, status, next_subprocess,
			    created_ts, start_after_ts,
			    started_ts, last_update_ts
			)
			VALUES (
			    $1, $2, $3, $4, $5,
			    $6, $7, $8,
			    $9, $10,
			    $11, $12
			)
		`,
		s.schema, s.table,
	)
	now := s.utilTime.Now()
	rowsAffected, err := qe.Exec(
		ctx, query,
		job.JobId, job.JobData, job.ProcessId, job.GoroutineId, s.tsInput(job.GoroutineHeartBeatTs),
		s.tsInput(job.GoroutineLeaseExpireTs), job.Status, job.NextSubprocess,
		s.tsInput(job.CreatedTs), s.tsInput(job.StartAfterTs),
		s.tsInput(job.StartedTs), now,
	)
	if err != nil {
		pgErr := &pgconn.PgError{}
		ok := errors.As(err, &pgErr)
		if ok && pgErr.Code == "23505" {
			return interr.Wrap(JobIdAlreadyExist, true)
		}
		return interr.Wrap(err, true)
	}

	if rowsAffected != int64(1) {
		return interr.Err(fmt.Sprintf("rows affected is not 1: %v", rowsAffected), true)
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf("%v - Register: - HB(%v)", s._printTs(now), s._printTs(job.GoroutineHeartBeatTs))
		s._cTestLog <- &testLog{jobId: job.JobId, msg: msg, procId: job.ProcessId}
	}

	return nil
}

func (s *PgStorage) GetOpenJobCandidates(ctx context.Context, processId string, maxJobsToReturn int) ([]string, error) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer s.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`SELECT
    		job_id
		FROM
			%v.%v
		WHERE
		    (process_id = $1 AND status = $2 AND start_after_ts <= $3
		    		AND goroutine_lease_expire_ts IS NULL AND goroutine_id = '')
			OR
			(process_id = $1 AND status = $2 AND start_after_ts <= $3
					AND goroutine_lease_expire_ts < $3)
		ORDER BY
		    start_after_ts
		LIMIT $4`,
		s.schema, s.table,
	)
	now := s.tsInput(s.utilTime.Now())
	rows, err := tx.Query(ctx, query, processId, JSReady, now, maxJobsToReturn)
	if err != nil {
		return nil, interr.Wrap(err, true)
	}
	defer rows.Close()

	jobIds := make([]string, 0)
	for rows.Next() {
		var jobId string
		err = rows.Scan(&jobId)
		if err != nil {
			return nil, interr.Wrap(err, true)
		}
		jobIds = append(jobIds, jobId)
	}
	err = rows.Err()
	if err != nil {
		return nil, interr.Wrap(err, true)
	}

	return jobIds, nil
}

func (s *PgStorage) TryTakeOverJob(ctx context.Context, jobId string, goroutineId string, leaseExpireDuration time.Duration) (*Job, error) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer s.rollback(ctx, cancel, tx)

	job, err := s.lockJob(ctx, tx, jobId)
	if err != nil {
		return nil, err
	}

	err = s.validateStatusForUpdate(job)
	if err != nil {
		return nil, err
	}

	now := s.utilTime.Now()
	isLeased := job.GoroutineId != "" ||
		job.GoroutineHeartBeatTs.IsZero() == false ||
		job.GoroutineLeaseExpireTs.IsZero() == false
	isReentrant := job.GoroutineId == goroutineId
	if isLeased && isReentrant == false {
		if job.GoroutineLeaseExpireTs.After(now) || job.GoroutineLeaseExpireTs.Equal(now) {
			return nil, interr.Wrap(AlreadyLeased, true)
		}
	}
	prevGoroutineId := job.GoroutineId
	prevHeartBeatTs := job.GoroutineHeartBeatTs

	query := fmt.Sprintf(
		`UPDATE %v.%v SET
			goroutine_id = $1,
			goroutine_heart_beat_ts = $2,
			goroutine_lease_expire_ts = $3,
			last_update_ts = $4
		WHERE job_id = $5`,
		s.schema, s.table,
	)
	leaseExpireTs := now.Add(leaseExpireDuration)
	res, err := tx.Exec(ctx, query, goroutineId, now, leaseExpireTs, now, jobId)
	if err != nil {
		return nil, interr.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		return nil, interr.Err(fmt.Sprintf("expected 1 row to be updated, got %v", res.RowsAffected()), true)
	}

	job, err = s.lockJob(ctx, tx, jobId)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf("%v - TakenOver: By Gr(%v) - Prev[Gr(%v) HB(%v)]", s._printTs(now), goroutineId, prevGoroutineId, s._printTs(prevHeartBeatTs))
		s._cTestLog <- &testLog{jobId: jobId, msg: msg, procId: job.ProcessId}
	}

	return job, nil
}

func (s *PgStorage) SendHeartbeat(ctx context.Context, goroutineId string, jobId string, leaseExpireDuration time.Duration) error {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer s.rollback(ctx, cancel, tx)

	job, err := s.lockJob(ctx, tx, jobId)
	if err != nil {
		return err
	}

	if job.GoroutineId != goroutineId {
		return interr.Wrap(AlreadyLeased, true)
	}

	err = s.validateStatusForUpdate(job)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`UPDATE %v.%v SET
		    goroutine_heart_beat_ts = $1,
		    goroutine_lease_expire_ts = $2,
		    last_update_ts = $3
		WHERE job_id = $4`,
		s.schema, s.table,
	)
	now := s.utilTime.Now()
	leaseExpireTs := now.Add(leaseExpireDuration)
	res, err := tx.Exec(ctx, query, now, leaseExpireTs, now, jobId)
	if err != nil {
		return interr.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		return interr.Err(fmt.Sprintf("expected 1 row to be updated, got %v", res.RowsAffected()), true)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return interr.Wrap(err, true)
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf("%v - HeartBeat: By Gr(%v) - Prev[HB(%v)]", s._printTs(now), goroutineId, s._printTs(job.GoroutineHeartBeatTs))
		s._cTestLog <- &testLog{jobId: jobId, msg: msg, procId: job.ProcessId}
	}

	return nil
}

func (s *PgStorage) GetJob(ctx context.Context, jobId string) (*Job, error) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer s.rollback(ctx, cancel, tx)

	return s.getJobImpl(ctx, tx, jobId, false)
}

func (s *PgStorage) UpdateJob(ctx context.Context, newMeta *Job, leaseExpireDuration time.Duration) error {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer s.rollback(ctx, cancel, tx)

	jobId := newMeta.JobId
	current, err := s.lockJob(ctx, tx, jobId)
	if err != nil {
		return err
	}

	if current.GoroutineId != newMeta.GoroutineId {
		return interr.Wrap(AlreadyLeased, true)
	}

	err = s.validateStatusForUpdate(current)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`UPDATE %v.%v SET
			job_data = $1, status = $2, end_ts = $3,
			goroutine_ids = $4, exec_count = $5, started_ts = $6,
		    next_subprocess = $7, goroutine_heart_beat_ts = $8,
		    goroutine_lease_expire_ts = $9, last_update_ts = $10
		WHERE job_id = $11`,
		s.schema, s.table,
	)
	now := s.utilTime.Now()
	leaseExpireTs := now.Add(leaseExpireDuration)
	res, err := tx.Exec(
		ctx, query,
		newMeta.JobData, newMeta.Status, s.tsInput(newMeta.EndTs),
		newMeta.GoroutineIds, newMeta.ExecCount, s.tsInput(newMeta.StartedTs),
		newMeta.NextSubprocess, now,
		leaseExpireTs, now,
		jobId,
	)
	if err != nil {
		return interr.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		return interr.Err(fmt.Sprintf("expected 1 row to be updated, got %v", res.RowsAffected()), true)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return interr.Wrap(err, true)
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf("%v - Update: By %v ->", s._printTs(now), newMeta.GoroutineId)
		if current.Status != newMeta.Status {
			msg += fmt.Sprintf(" status(%v)", newMeta.Status)
		}
		if current.ExecCount != newMeta.ExecCount {
			msg += fmt.Sprintf(" execCount(%v)", newMeta.ExecCount)
		}
		if current.NextSubprocess != newMeta.NextSubprocess {
			msg += fmt.Sprintf(" next(%v)", newMeta.NextSubprocess)
		}
		if current.StartedTs.Equal(newMeta.StartedTs) == false {
			msg += fmt.Sprintf(" started(%v)", s._printTs(newMeta.StartedTs))
		}
		if len(current.GoroutineIds) != len(newMeta.GoroutineIds) {
			msg += fmt.Sprintf(" grs(%v)", newMeta.GoroutineIds)
		}
		if current.JobData != newMeta.JobData {
			msg += fmt.Sprintf(" data(%v)", newMeta.JobData)
		}
		if current.EndTs.Equal(newMeta.EndTs) == false {
			msg += fmt.Sprintf(" end(%v)", s._printTs(newMeta.EndTs))
		}
		msg += fmt.Sprintf(" heart(%v)", s._printTs(now))
		msg += fmt.Sprintf(" expire(%v)", s._printTs(leaseExpireTs))
		s._cTestLog <- &testLog{jobId: jobId, procId: current.ProcessId, msg: msg}
	}

	return nil
}

func (s *PgStorage) validateStatusForUpdate(job *Job) error {
	if job.Status == JSDone {
		return interr.Wrap(AlreadyDone, true)
	}
	if job.Status == JSError {
		return interr.Wrap(AlreadyError, true)
	}
	if job.Status != JSReady {
		msg := fmt.Sprintf("expected job status 'ready', got %v instead, job id: %v", job.Status, job.JobId)
		return interr.Err(msg, true)
	}
	return nil
}

func (s *PgStorage) ClearDoneJobs(ctx context.Context, processId string, activeThreshold time.Time, maxRowsToDelete int) (int64, error) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer s.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(
		`DELETE FROM %v.%v
			WHERE job_id in(
				SELECT job_id FROM %v.%v WHERE end_ts <= $1 AND process_id = $2 AND status=$3 ORDER BY end_ts ASC
				LIMIT $4 FOR UPDATE SKIP LOCKED
			)`, s.schema, s.table, s.schema, s.table,
	)
	res, err := tx.Exec(ctx, query, activeThreshold, processId, JSDone, maxRowsToDelete)
	if err != nil {
		return 0, interr.Wrap(err, true)
	}

	result := res.RowsAffected()
	if result > int64(maxRowsToDelete) {
		return 0, interr.Err(fmt.Sprintf("expected up to %v, got %v", maxRowsToDelete, res.RowsAffected()), true)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, interr.Wrap(err, true)
	}

	return result, nil
}

func (s *PgStorage) lockJob(ctx context.Context, tx pgx.Tx, jobId string) (*Job, error) {
	return s.getJobImpl(ctx, tx, jobId, true)
}

func (s *PgStorage) getJobImpl(ctx context.Context, tx pgx.Tx, jobId string, lock bool) (*Job, error) {
	query := fmt.Sprintf(`SELECT
			job_id, job_data, process_id, goroutine_id, goroutine_heart_beat_ts, goroutine_lease_expire_ts, status,
			next_subprocess, run_type, goroutine_ids, exec_count, comp_count, created_ts, start_after_ts,
			started_ts, end_ts, last_update_ts
		FROM %v.%v
		WHERE job_id = $1`,
		s.schema, s.table,
	)
	if lock {
		query += " FOR UPDATE"
	}

	row := tx.QueryRow(ctx, query, jobId)
	m := Job{GoroutineIds: make([]string, 0)}

	err := s.convertJob(row, &m)
	if err != nil {
		return nil, interr.Wrap(err, true)
	}
	return &m, nil
}

func (s *PgStorage) tsInput(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// Scannable is an interface for pgx rows.
type Scannable interface {
	Scan(dest ...any) error
}

func (s *PgStorage) convertJob(row Scannable, m *Job) error {
	heartbeatTs := sql.NullTime{}
	leaseExpireTs := sql.NullTime{}
	startedTs := sql.NullTime{}
	endTs := sql.NullTime{}

	err := row.Scan(
		&m.JobId, &m.JobData, &m.ProcessId, &m.GoroutineId, &heartbeatTs, &leaseExpireTs, &m.Status,
		&m.NextSubprocess, &m.RunType, &m.GoroutineIds, &m.ExecCount, &m.CompCount, &m.CreatedTs, &m.StartAfterTs,
		&startedTs, &endTs, &m.LastUpdateTs,
	)
	if err != nil {
		return interr.Wrap(err, true)
	}
	m.GoroutineHeartBeatTs = heartbeatTs.Time
	m.GoroutineLeaseExpireTs = leaseExpireTs.Time
	m.StartedTs = startedTs.Time
	m.EndTs = endTs.Time

	return nil
}

func (s *PgStorage) FilterJobs(ctx context.Context, input FilterJob, page int, itemsPerPage int) ([]*Job, int, error) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer s.rollback(ctx, cancel, tx)

	tableName := fmt.Sprintf("%s.%s", s.schema, s.table)
	builder := sq.StatementBuilder.PlaceholderFormat(sq.Dollar).
		Select("*").
		From(tableName)

	if input.JobId != "" {
		builder = builder.Where(sq.Eq{"job_id": input.JobId})
	}
	if input.ProcessId != "" {
		builder = builder.Where(sq.Eq{"process_id": input.ProcessId})
	}
	if input.GoroutineId != "" {
		builder = builder.Where(sq.Eq{"goroutine_id": input.GoroutineId})
	}
	if input.JobStatus != "" {
		builder = builder.Where(sq.Eq{"status": input.JobStatus})
	}
	if input.ExecCountGt != 0 {
		builder = builder.Where(sq.Gt{"exec_count": input.ExecCountGt})
	}
	if input.ExecCountLt != 0 {
		builder = builder.Where(sq.Lt{"exec_count": input.ExecCountLt})
	}
	if input.RunType != "" {
		builder = builder.Where(sq.Eq{"run_type": input.RunType})
	}
	if !input.CreatedTsGte.IsZero() {
		builder = builder.Where(sq.GtOrEq{"created_ts": input.CreatedTsGte})
	}
	if !input.CreatedTsLte.IsZero() {
		builder = builder.Where(sq.LtOrEq{"created_ts": input.CreatedTsLte})
	}
	if !input.LastUpdateTsGte.IsZero() {
		builder = builder.Where(sq.GtOrEq{"last_update_ts": input.LastUpdateTsGte})
	}
	if !input.LastUpdateTsLte.IsZero() {
		builder = builder.Where(sq.LtOrEq{"last_update_ts": input.LastUpdateTsLte})
	}
	if !input.StartedTsGte.IsZero() {
		builder = builder.Where(sq.GtOrEq{"started_ts": input.StartedTsGte})
	}
	if !input.StartedTsLte.IsZero() {
		builder = builder.Where(sq.LtOrEq{"started_ts": input.StartedTsLte})
	}
	if !input.EndTsGte.IsZero() {
		builder = builder.Where(sq.GtOrEq{"end_ts": input.EndTsGte})
	}
	if !input.EndTsLte.IsZero() {
		builder = builder.Where(sq.LtOrEq{"end_ts": input.EndTsLte})
	}

	countBuilder := builder.RemoveColumns()
	countBuilder = countBuilder.Column("COUNT(*)")

	countQuery, countArgs, err := countBuilder.ToSql()
	if err != nil {
		return nil, 0, interr.Wrap(err, true)
	}

	var count int
	err = tx.QueryRow(ctx, countQuery, countArgs...).Scan(&count)
	if err != nil {
		return nil, 0, interr.Wrap(err, true)
	}

	if count == 0 {
		return []*Job{}, 0, nil
	}

	offset := (page - 1) * itemsPerPage

	selectBuilder := builder.
		RemoveColumns().
		Columns(
			"job_id", "job_data", "process_id", "goroutine_id",
			"goroutine_heart_beat_ts", "goroutine_lease_expire_ts",
			"status", "next_subprocess", "run_type", "goroutine_ids",
			"exec_count", "comp_count", "created_ts", "start_after_ts",
			"started_ts", "end_ts", "last_update_ts",
		).
		OrderBy("created_ts DESC").
		Limit(uint64(itemsPerPage)).
		Offset(uint64(offset))

	query, args, err := selectBuilder.ToSql()
	if err != nil {
		return nil, 0, interr.Wrap(err, true)
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, interr.Wrap(err, true)
	}
	defer rows.Close()

	jobs := make([]*Job, 0)
	for rows.Next() {
		job := &Job{GoroutineIds: make([]string, 0)}
		err = s.convertJob(rows, job)
		if err != nil {
			return nil, 0, interr.Wrap(err, true)
		}
		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return nil, 0, interr.Wrap(err, true)
	}

	return jobs, count, nil
}

// pgxQueryExecutor wraps pgx.Tx to implement QueryExecutor.
type pgxQueryExecutor struct {
	tx pgx.Tx
}

func (p *pgxQueryExecutor) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	res, err := p.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func (p *pgxQueryExecutor) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return p.tx.QueryRow(ctx, sql, args...)
}

func (p *pgxQueryExecutor) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := p.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{rows: rows}, nil
}

type pgxRows struct {
	rows pgx.Rows
}

func (r *pgxRows) Next() bool      { return r.rows.Next() }
func (r *pgxRows) Scan(dest ...any) error { return r.rows.Scan(dest...) }
func (r *pgxRows) Err() error      { return r.rows.Err() }
func (r *pgxRows) Close()          { r.rows.Close() }
