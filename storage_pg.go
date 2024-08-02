package ssproc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"rukita.co/main/be/data"
	"rukita.co/main/be/lib/mend"
	"rukita.co/main/be/lib/util"
	"time"
)

var _ Storage = (*PgStorage)(nil)

type PgStorage struct {
	connStr   string
	table     string
	schema    string
	logger    *mend.ZerologLogger
	txTimeout time.Duration
	utilTime  util.Time

	// _mustSendTestLog is used to send log of events happening on each job (e.g. take-overs, heartbeats, etc.). This is
	// only turned on manually with _mustSendTestLog whenever we have a test failure for troubleshooting bugs. It's not
	// turned on in any of our test cases once the bug is fixed.
	_mustSendTestLog bool
	_startTime       time.Time
	_cTestLog        chan *testLog
}

func NewPgStorage(connStr string, schemaName string, tableName string) (*PgStorage, error) {
	ret := &PgStorage{
		connStr:   connStr,
		schema:    schemaName,
		table:     tableName,
		logger:    mend.NewZerologLogger("PgStorage"),
		txTimeout: 5 * time.Second,
		utilTime:  util.NewGlobalTime(data.DefaultTimeZone()),
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

func (s *PgStorage) beginTx(ctx context.Context) (
	tx pgx.Tx,
	timeoutCtx context.Context,
	cancelCtxAndConn func(),
	err error,
) {
	// We don't want operations to take longer than 5 seconds.
	timeoutCtx, cancelCtx := context.WithTimeout(ctx, s.txTimeout)

	conn, err := pgx.Connect(timeoutCtx, s.connStr)
	if err != nil {
		defer cancelCtx()
		return nil, nil, nil, mend.Wrap(err, true)
	}

	// This cancel function will cancel both the connection and the context.
	// Because we're using direct connection, not just connection pooling, it means
	cancelCtxAndConn = func() {
		defer cancelCtx()
		err = conn.Close(timeoutCtx)
		if err != nil {
			if errors.Is(err, pgx.ErrTxClosed) == false {
				s.logger.ErrorNoTrace().Error(mend.Wrap(err, true)).
					Msg("failed to close connection for ssproc")
			}
		}
	}

	// We don't want heartbeat to keep connection up more than this time.
	_, err = conn.Exec(timeoutCtx, fmt.Sprintf(
		`SET idle_in_transaction_session_timeout = %v`,
		s.txTimeout.Milliseconds(),
	))
	if err != nil {
		defer cancelCtxAndConn()
		return nil, nil, nil, mend.Wrap(err, true)
	}

	opts := pgx.TxOptions{
		// When this is set to "repeatable read" there can be lots of "cannot serialize" (SQLSTATE 40001) because
		// Postgres repeatable read guarantees that the transaction never sees any changes (committed or not) after
		// transaction began. This causes conflict between Executor and Heartbeat goroutines:
		//		1. Executor: Start transaction.
		//		2. Heartbeat: Start transaction.
		//		3. Heartbeat: Lock Job.
		//		4. Heartbeat: Update heartbeat.
		//		5. Heartbeat: Commit.
		//		6. Executor: Lock Job.
		//		7. Executor: "cannot serialize" error.
		//			- Job row is already updated by Heartbeat after transaction started.
		//
		// ReadCommitted isolation level is enough, as we always lock before updating.
		IsoLevel: pgx.ReadCommitted,

		DeferrableMode: pgx.NotDeferrable,
		AccessMode:     pgx.ReadWrite,
	}
	tx, err = conn.BeginTx(timeoutCtx, opts)
	if err != nil {
		defer s.rollback(timeoutCtx, cancelCtxAndConn, tx)
		return nil, nil, nil, mend.Wrap(err, true)
	}

	return tx, timeoutCtx, cancelCtxAndConn, nil
}

func (s *PgStorage) rollback(ctx context.Context, cancelCtxAndConn func(), tx pgx.Tx) {
	defer cancelCtxAndConn()

	// This call is from failure to create transaction, there's nothing more to be done.
	if tx == nil {
		return
	}

	// We don't want the rollback operation to fail because context is canceled. Create new context with 5 seconds
	// of timeout to rollback the transaction.
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := tx.Rollback(rollbackCtx)
	if err != nil {
		if errors.Is(err, pgx.ErrTxClosed) {
			// No need to log in this case.
			return
		}

		if err.Error() == "conn closed" {
			// No need to log, as connection is already closed.
			return
		}

		err = mend.Wrap(err, true)
		msg := fmt.Sprintf("failed to rollback for table: %v.%v", s.schema, s.table)
		s.logger.ErrorNoTrace().Error(err).Msg(msg)
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

	// Print time as monotonic duration, easier to keep track of, and makes sure that timings are correct.
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
		return fmt.Sprintf(
			"%v.%v: column %v does not exist in table",
			schema, table,
			p.columnName,
		)
	}

	if p.columnName != a.columnName {
		return fmt.Sprintf(
			"%v.%v: expected column name to be %v",
			schema, table,
			p.columnName,
		)
	}
	if p.isNullable != a.isNullable {
		return fmt.Sprintf(
			"%v.%v: expected column %v nullable to be %v, found %v",
			schema, table,
			p.columnName, p.isNullable, a.isNullable,
		)
	}
	if p.dataType != a.dataType {
		return fmt.Sprintf(
			"%v.%v: expected column %v dataType to be %v, found %v",
			schema, table,
			p.columnName, p.dataType, a.dataType,
		)
	}
	if p.columnDefault != a.columnDefault {
		return fmt.Sprintf(
			"%v:%v expected column %v default value to be %v, found %v",
			schema, table,
			p.columnName, p.columnDefault, a.columnDefault,
		)
	}
	if p.charMaxLength != a.charMaxLength {
		return fmt.Sprintf(
			"%v:%v expected column %v char max length to be %v, found %v",
			schema, table,
			p.columnName, p.charMaxLength, a.charMaxLength,
		)
	}

	return ""
}

var requiredCols = []*pgColumn{
	{
		columnName:    "job_id",
		isNullable:    false,
		dataType:      "text",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "job_data",
		isNullable:    false,
		dataType:      "text",
		columnDefault: "''::text",
		charMaxLength: 0,
	},
	{
		columnName:    "process_id",
		isNullable:    false,
		dataType:      "text",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "goroutine_id",
		isNullable:    false,
		dataType:      "character varying",
		columnDefault: "''::character varying",
		charMaxLength: 128,
	},
	{
		columnName:    "goroutine_heart_beat_ts",
		isNullable:    true,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "goroutine_lease_expire_ts",
		isNullable:    true,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "status",
		isNullable:    false,
		dataType:      "character varying",
		columnDefault: "'ready'::character varying",
		charMaxLength: 64,
	},
	{
		columnName:    "next_subprocess",
		isNullable:    false,
		dataType:      "integer",
		columnDefault: "0",
		charMaxLength: 0,
	},
	{
		columnName:    "run_type",
		isNullable:    false,
		dataType:      "character varying",
		columnDefault: "'normal'::character varying",
		charMaxLength: 64,
	},
	{
		columnName:    "goroutine_ids",
		isNullable:    false,
		dataType:      "ARRAY",
		columnDefault: "'{}'::text[]",
		charMaxLength: 0,
	},
	{
		columnName:    "exec_count",
		isNullable:    false,
		dataType:      "integer",
		columnDefault: "0",
		charMaxLength: 0,
	},
	{
		columnName:    "comp_count",
		isNullable:    false,
		dataType:      "integer",
		columnDefault: "0",
		charMaxLength: 0,
	},
	{
		columnName:    "created_ts",
		isNullable:    false,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "start_after_ts",
		isNullable:    false,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "started_ts",
		isNullable:    true,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "end_ts",
		isNullable:    true,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
	{
		columnName:    "last_update_ts",
		isNullable:    false,
		dataType:      "timestamp with time zone",
		columnDefault: "",
		charMaxLength: 0,
	},
}

func (s *PgStorage) verifySchema() error {
	tx, ctx, cancel, err := s.beginTx(context.Background())
	if err != nil {
		return err
	}

	defer s.rollback(ctx, cancel, tx)

	// language=sql
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
		return mend.Wrap(err, true)
	}

	defer rows.Close()

	cols := make(map[string]*pgColumn)
	for rows.Next() {
		col := pgColumn{}
		colDefault := sql.NullString{}
		maxLength := sql.NullInt64{}
		err = rows.Scan(
			&col.columnName, &col.isNullable, &col.dataType, &colDefault,
			&maxLength,
		)
		if err != nil {
			return mend.Wrap(err, true)
		}

		col.columnDefault = colDefault.String
		col.charMaxLength = maxLength.Int64
		cols[col.columnName] = &col
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	// Verify columns.
	for _, req := range requiredCols {
		found := cols[req.columnName]
		errMsg := req.isEqual(s.schema, s.table, found)
		if errMsg != "" {
			return mend.Err(errMsg, true)
		}
	}

	return nil
}

var requiredIndexes = map[string]string{
	"%v_pkey":    "CREATE UNIQUE INDEX %v_pkey ON %v.%v USING btree (job_id)",
	"%v_expired": "CREATE INDEX %v_expired ON %v.%v USING btree (process_id, status, start_after_ts, goroutine_lease_expire_ts)",
}

func (s *PgStorage) verifyIndex() error {
	tx, ctx, cancel, err := s.beginTx(context.Background())
	if err != nil {
		return err
	}

	defer s.rollback(ctx, cancel, tx)

	// language=sql
	query := `SELECT
    		indexname, indexdef
		FROM
			pg_indexes 
		WHERE 
		    schemaname = $1 AND tablename = $2`
	rows, err := tx.Query(ctx, query, s.schema, s.table)
	if err != nil {
		return mend.Wrap(err, true)
	}

	defer rows.Close()

	indexes := make(map[string]string)
	for rows.Next() {
		var name, def string
		err = rows.Scan(&name, &def)
		if err != nil {
			return mend.Wrap(err, true)
		}

		indexes[name] = def
	}
	err = rows.Err()
	if err != nil {
		return mend.Wrap(err, true)
	}

	if len(indexes) == 0 {
		msg := fmt.Sprintf(
			"%v.%v: no index found! expected 1 primary key index",
			s.schema, s.table,
		)
		return mend.Err(msg, true)
	}

	if len(indexes) != 2 {
		msg := fmt.Sprintf(
			"%v.%v: multiple indexes found, expected two indexes",
			s.schema, s.table,
		)
		return mend.Err(msg, true)
	}

	for rawName, pattern := range requiredIndexes {
		indexName := fmt.Sprintf(rawName, s.table)
		foundDef := indexes[indexName]
		if foundDef == "" {
			msg := fmt.Sprintf(
				"%v.%v: index_name %v not found",
				s.schema, s.table, indexName,
			)
			return mend.Err(msg, true)
		}

		expectedDef := fmt.Sprintf(pattern, s.table, s.schema, s.table)
		if foundDef != expectedDef {
			msg := fmt.Sprintf(
				"%v.%v: expected index_name %v to be: %v, found: %v",
				s.schema, s.table, indexName,
				expectedDef, foundDef,
			)
			return mend.Err(msg, true)
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

	if job.Status != JSReady {
		return mend.Err("job must be registered with 'ready' status", true)
	}

	if job.ProcessId == "" {
		return mend.Err("process ID must not be empty!", true)
	}

	if job.GoroutineLeaseExpireTs.IsZero() == false &&
		job.GoroutineHeartBeatTs.Before(job.GoroutineLeaseExpireTs) == false {
		return mend.Err("heartbeat ts >= leaseExpireTs", true)
	}

	// language=sql
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
	res, err := tx.Exec(
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
			// The error code is duplicate key error. We've already verified that there's only 1 key in the table.
			return mend.Wrap(JobIdAlreadyExist, true)
		}

		return mend.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		return mend.Err(fmt.Sprintf("rows affected is not 1: %v", res.RowsAffected()), true)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return mend.Wrap(err, true)
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf(
			"%v - Register: - HB(%v)",
			s._printTs(now), s._printTs(job.GoroutineHeartBeatTs),
		)
		s._cTestLog <- &testLog{
			jobId:  job.JobId,
			msg:    msg,
			procId: job.ProcessId,
		}
	}

	// Otherwise job was successfully registered with the given metadata.
	return nil
}

func (s *PgStorage) GetOpenJobCandidates(
	ctx context.Context,
	processId string,
	maxJobsToReturn int,
) (
	[]string,
	error,
) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}

	defer s.rollback(ctx, cancel, tx)

	// language=sql
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
	rows, err := tx.Query(
		ctx, query,

		// Where...
		processId, JSReady, now,
		maxJobsToReturn,
	)
	if err != nil {
		return nil, mend.Wrap(err, true)
	}

	defer rows.Close()

	jobIds := make([]string, 0)
	for rows.Next() {
		var jobId string
		err = rows.Scan(&jobId)
		if err != nil {
			return nil, mend.Wrap(err, true)
		}

		jobIds = append(jobIds, jobId)
	}
	err = rows.Err()
	if err != nil {
		return nil, mend.Wrap(err, true)
	}

	return jobIds, nil
}

func (s *PgStorage) TryTakeOverJob(
	ctx context.Context,
	jobId string,
	goroutineId string,
	leaseExpireDuration time.Duration,
) (
	job *Job,
	err error,
) {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return nil, err
	}

	defer s.rollback(ctx, cancel, tx)

	// First we lock the job, to make sure nobody else is using it.
	job, err = s.lockJob(ctx, tx, jobId)
	if err != nil {
		return nil, err
	}

	// Verify statuses.
	err = s.validateStatusForUpdate(job)
	if err != nil {
		return nil, err
	}

	// Skip checking executor heartbeat if existing goroutine ID is the same. There are two possibilities:
	//		1. GoroutineLeaseExpireTs >= now (active). It's okay since it's the same Goroutine.
	//		2. GoroutineLeaseExpireTs < now (expired). It's okay because it's already expired.
	// This allows reentrant lock.
	//
	// Otherwise, when goroutineId is not empty, verify that it's okay to take over this job.
	now := s.utilTime.Now()
	isLeased := job.GoroutineId != "" ||
		job.GoroutineHeartBeatTs.IsZero() == false ||
		job.GoroutineLeaseExpireTs.IsZero() == false
	isReentrant := job.GoroutineId == goroutineId
	if isLeased && isReentrant == false {
		// Reject if still leased.
		if job.GoroutineLeaseExpireTs.After(now) ||
			job.GoroutineLeaseExpireTs.Equal(now) {
			return nil, mend.Wrap(AlreadyLeased, true)
		}
	}
	prevGoroutineId := job.GoroutineId
	prevHeartBeatTs := job.GoroutineHeartBeatTs

	// We've verified status is ready and one of these are true:
	// 	1. Goroutine ID, HeartBeatTs and LeaseExpireTs is empty.
	//	2. Goroutine ID is not empty, but LeaseExpireTs is already in the past.
	//
	// Take over this JobData by setting the goroutineId and first heartbeat.

	// language=sql
	query := fmt.Sprintf(
		`UPDATE 
				%v.%v 
			SET 
				goroutine_id = $1, 
				goroutine_heart_beat_ts = $2,
				goroutine_lease_expire_ts = $3, 
				last_update_ts = $4
			WHERE 
				job_id = $5`,
		s.schema, s.table,
	)
	leaseExpireTs := now.Add(leaseExpireDuration)
	res, err := tx.Exec(
		ctx, query,

		// Set...
		goroutineId, now, leaseExpireTs, now,

		// Where...
		jobId,
	)
	if err != nil {
		return nil, mend.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		msg := fmt.Sprintf("exected 1 row to be updated, got %v", res.RowsAffected())
		return nil, mend.Err(msg, true)
	}

	// Get the latest data (the one we already updated).
	job, err = s.lockJob(ctx, tx, jobId)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf(
			"%v - TakenOver: By Gr(%v) - Prev[Gr(%v) HB(%v)]",
			s._printTs(now), goroutineId, prevGoroutineId, s._printTs(prevHeartBeatTs),
		)
		s._cTestLog <- &testLog{
			jobId:  jobId,
			msg:    msg,
			procId: job.ProcessId,
		}
	}

	return job, nil
}

func (s *PgStorage) SendHeartbeat(
	ctx context.Context,
	goroutineId string,
	jobId string,
	leaseExpireDuration time.Duration,
) error {
	tx, ctx, cancel, err := s.beginTx(ctx)
	if err != nil {
		return err
	}

	defer s.rollback(ctx, cancel, tx)

	// First lock the job.
	job, err := s.lockJob(ctx, tx, jobId)
	if err != nil {
		return err
	}

	// Allow to update heartbeat ONLY if goroutineId is still the same.
	// This can happen when time in some servers are out-of-sync, or an Executor runtime got stop-the-world for longer
	// than ExecutionTimeout config.
	if job.GoroutineId != goroutineId {
		return mend.Wrap(AlreadyLeased, true)
	}

	// Only allow to send heartbeat for ready status.
	err = s.validateStatusForUpdate(job)
	if err != nil {
		return err
	}

	// Why we only check goroutineId and statuses here?
	//
	// There's a chance that even though the goroutineId is still the same, lease is already expired.
	// However, if the goroutine ID is still the same, and we've ensured this by locking the row, it also means that
	// no other Executor instances have taken over this JobData. In this case, it's better to let the original executor
	// continue working on it.
	//
	// Next, update the heartbeat.

	// language=sql
	query := fmt.Sprintf(`UPDATE 
    			%v.%v 
			SET 
			    goroutine_heart_beat_ts = $1,
			    goroutine_lease_expire_ts = $2,
			    last_update_ts = $3
			WHERE 
			    job_id = $4`,
		s.schema, s.table,
	)
	now := s.utilTime.Now()
	leaseExpireTs := now.Add(leaseExpireDuration)
	res, err := tx.Exec(
		ctx, query,

		// Set...
		now, leaseExpireTs, now,

		// Where...
		jobId,
	)
	if err != nil {
		return mend.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		msg := fmt.Sprintf("exected 1 row to be updated, got %v", res.RowsAffected())
		return mend.Err(msg, true)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return mend.Wrap(err, true)
	}

	if s._mustSendTestLog {
		msg := fmt.Sprintf(
			"%v - HeartBeat: By Gr(%v) - Prev[HB(%v)]",
			s._printTs(now), goroutineId, s._printTs(job.GoroutineHeartBeatTs),
		)
		s._cTestLog <- &testLog{
			jobId:  jobId,
			msg:    msg,
			procId: job.ProcessId,
		}
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

	// First lock the job.
	jobId := newMeta.JobId
	current, err := s.lockJob(ctx, tx, jobId)
	if err != nil {
		return err
	}

	// Only allow updating if executorId is still the same.
	if current.GoroutineId != newMeta.GoroutineId {
		return mend.Wrap(AlreadyLeased, true)
	}

	err = s.validateStatusForUpdate(current)
	if err != nil {
		return err
	}

	// Why we only check goroutineId and statuses here?
	//
	// See explanation at: SendHeartbeat.
	//
	// Next, update only fields that are allowed to be updated.

	// language=sql
	query := fmt.Sprintf(`UPDATE 
    		%v.%v 
		SET
			job_data = $1, status = $2, end_ts = $3,
			goroutine_ids = $4, exec_count = $5, started_ts = $6, 
		    next_subprocess = $7, goroutine_heart_beat_ts = $8,
		    goroutine_lease_expire_ts = $9, last_update_ts = $10
		WHERE
			job_id = $11`,
		s.schema, s.table,
	)
	now := s.utilTime.Now()
	leaseExpireTs := now.Add(leaseExpireDuration)
	res, err := tx.Exec(
		ctx, query,

		// Set...
		newMeta.JobData, newMeta.Status, s.tsInput(newMeta.EndTs),
		newMeta.GoroutineIds, newMeta.ExecCount, s.tsInput(newMeta.StartedTs),
		newMeta.NextSubprocess, now,
		leaseExpireTs, now,

		// Where...
		jobId,
	)
	if err != nil {
		return mend.Wrap(err, true)
	}

	if res.RowsAffected() != int64(1) {
		msg := fmt.Sprintf("exected 1 row to be updated, got %v", res.RowsAffected())
		return mend.Err(msg, true)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return mend.Wrap(err, true)
	}

	if s._mustSendTestLog {
		// Only log changes.
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
		s._cTestLog <- &testLog{
			jobId:  jobId,
			procId: current.ProcessId,
			msg:    msg,
		}
	}

	return nil
}

func (s *PgStorage) validateStatusForUpdate(job *Job) error {
	// Only allow to send heartbeat for ready status.
	if job.Status == JSDone {
		return mend.Wrap(AlreadyDone, true)
	}
	if job.Status == JSError {
		return mend.Wrap(AlreadyError, true)
	}
	if job.Status != JSReady {
		// Defensive coding. Should not happen.
		msg := fmt.Sprintf("expected job status 'ready', got %v instead, job id: %v", job.Status, job.JobId)
		return mend.Err(msg, true)
	}

	return nil
}

func (s *PgStorage) ClearDoneJobs(ctx context.Context, activeThreshold time.Time) error {
	// todo pr: implement automatic clearing of jobs longer than specific time.
	panic("implement me")
}

func (s *PgStorage) lockJob(
	ctx context.Context,
	tx pgx.Tx,
	jobId string,
) (
	job *Job,
	err error,
) {
	return s.getJobImpl(ctx, tx, jobId, true)
}

//goland:noinspection SqlResolve
func (s *PgStorage) getJobImpl(ctx context.Context, tx pgx.Tx, jobId string, lock bool) (job *Job, err error) {
	// language=sql
	query := fmt.Sprintf(`SELECT
				job_id, job_data, process_id, goroutine_id, goroutine_heart_beat_ts, goroutine_lease_expire_ts, status, 
				next_subprocess, run_type, goroutine_ids, exec_count, comp_count, created_ts, start_after_ts, 
				started_ts, end_ts, last_update_ts
    		FROM
    		    %v.%v
			WHERE
				job_id = $1`,
		s.schema, s.table,
	)
	if lock {
		query += " FOR UPDATE"
	}

	row := tx.QueryRow(ctx, query, jobId)

	m := Job{
		GoroutineIds: make([]string, 0),
	}
	heartbeatTs := sql.NullTime{}
	leaseExpireTs := sql.NullTime{}
	startedTs := sql.NullTime{}
	endTs := sql.NullTime{}

	// Will return error if row does not exist.
	err = row.Scan(
		&m.JobId, &m.JobData, &m.ProcessId, &m.GoroutineId, &heartbeatTs, &leaseExpireTs, &m.Status,
		&m.NextSubprocess, &m.RunType, &m.GoroutineIds, &m.ExecCount, &m.CompCount, &m.CreatedTs, &m.StartAfterTs,
		&startedTs, &endTs, &m.LastUpdateTs,
	)
	if err != nil {
		return nil, mend.Wrap(err, true)
	}

	m.GoroutineHeartBeatTs = heartbeatTs.Time
	m.GoroutineLeaseExpireTs = leaseExpireTs.Time
	m.StartedTs = startedTs.Time
	m.EndTs = endTs.Time
	return &m, nil
}

func (s *PgStorage) tsInput(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
