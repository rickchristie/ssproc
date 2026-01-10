package ssproc

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
	"github.com/rickchristie/ssproc/internal/timeutil"
)

// PgTestHelper provides testing utilities for ssproc.
type PgTestHelper struct {
	T         *testing.T
	Ctx       context.Context
	PgStorage *PgStorage
}

// SetPgMockTimeForTest sets a mock time for the storage.
func (h *PgTestHelper) SetPgMockTimeForTest(mockTime timeutil.Time) {
	h.PgStorage.utilTime = mockTime
}

// GetJob retrieves a job from the database.
func (h *PgTestHelper) GetJob(t *testing.T, jobId string) (foundJob *Job, isValid map[string]bool) {
	tx, ctx, cancel, err := h.PgStorage.beginTx(h.Ctx)
	assert.Nil(t, err)
	defer h.PgStorage.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`SELECT
			job_id, job_data, process_id, goroutine_id, goroutine_heart_beat_ts, goroutine_lease_expire_ts,
			status, next_subprocess, run_type, goroutine_ids, exec_count, comp_count,
			created_ts, start_after_ts, started_ts, end_ts, last_update_ts
		FROM
			%v.%v
		WHERE
			job_id = $1`,
		h.PgStorage.schema, h.PgStorage.table,
	)

	row := tx.QueryRow(h.Ctx, query, jobId)
	job := Job{GoroutineIds: make([]string, 0)}
	heartbeat := sql.NullTime{}
	leaseExpire := sql.NullTime{}
	started := sql.NullTime{}
	end := sql.NullTime{}
	err = row.Scan(
		&job.JobId, &job.JobData, &job.ProcessId, &job.GoroutineId, &heartbeat, &leaseExpire,
		&job.Status, &job.NextSubprocess, &job.RunType, &job.GoroutineIds, &job.ExecCount, &job.CompCount,
		&job.CreatedTs, &job.StartAfterTs, &started, &end, &job.LastUpdateTs,
	)
	assert.Nil(t, err)

	job.GoroutineHeartBeatTs = heartbeat.Time
	job.GoroutineLeaseExpireTs = leaseExpire.Time
	job.StartedTs = started.Time
	job.EndTs = end.Time

	isValid = make(map[string]bool)
	isValid["goroutine_heart_beat_ts"] = heartbeat.Valid
	isValid["goroutine_lease_expire_ts"] = leaseExpire.Valid
	isValid["started_ts"] = started.Valid
	isValid["end_ts"] = end.Valid

	return &job, isValid
}

// CountNotDoneJobs counts jobs that are not done.
func (h *PgTestHelper) CountNotDoneJobs(t *testing.T, processId string) int {
	tx, ctx, cancel, err := h.PgStorage.beginTx(h.Ctx)
	assert.Nil(t, err)
	defer h.PgStorage.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`SELECT
			COUNT(*)
		FROM
			%v.%v
		WHERE
			process_id = $1 AND status != $2`,
		h.PgStorage.schema, h.PgStorage.table,
	)
	row := tx.QueryRow(h.Ctx, query, processId, JSDone)

	var count int
	err = row.Scan(&count)
	assert.Nil(t, err)

	return count
}

// SetLeaseExpireTime sets the lease expire time for a job.
func (h *PgTestHelper) SetLeaseExpireTime(t *testing.T, jobId string, leaseExpireTs time.Time) {
	tx, ctx, cancel, err := h.PgStorage.beginTx(h.Ctx)
	assert.Nil(t, err)
	defer h.PgStorage.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`UPDATE
			%v.%v
		SET
			goroutine_lease_expire_ts = $1
		WHERE
			job_id = $2`,
		h.PgStorage.schema, h.PgStorage.table,
	)
	_, err = tx.Exec(h.Ctx, query, leaseExpireTs, jobId)
	assert.Nil(t, err)

	err = tx.Commit(h.Ctx)
	assert.Nil(t, err)
}

// WaitJobStatus waits for a job to reach a specific status.
func (h *PgTestHelper) WaitJobStatus(t *testing.T, timeout time.Duration, jobId string, status JobStatus) {
	err := testutil.Await(timeout, func() bool {
		job, _ := h.GetJob(t, jobId)
		return job.Status == status
	})
	assert.Nil(t, err)
}

// WaitJobGoroutineIdChanged waits for the goroutine ID to change.
func (h *PgTestHelper) WaitJobGoroutineIdChanged(t *testing.T, timeout time.Duration, goroutineIds []string, jobId string) {
	err := testutil.Await(timeout, func() bool {
		found, _ := h.GetJob(t, jobId)
		return len(goroutineIds) < len(found.GoroutineIds)
	})
	assert.Nil(t, err)

	found, _ := h.GetJob(t, jobId)
	assert.Equal(t, len(goroutineIds)+1, len(found.GoroutineIds))
	expected := append(goroutineIds, found.GoroutineIds[len(found.GoroutineIds)-1])
	assert.Equal(t, expected, found.GoroutineIds)
}

// WaitAllJobsDone waits for all jobs to be done.
func (h *PgTestHelper) WaitAllJobsDone(t *testing.T, timeout time.Duration, processId string) {
	err := testutil.Await(timeout, func() bool {
		count := h.CountNotDoneJobs(t, processId)
		return count == 0
	})
	assert.Nil(t, err)
}

// GetSuccessfulJobs returns jobs with status JSDone.
func (h *PgTestHelper) GetSuccessfulJobs(endTsLte time.Time) []*Job {
	tx, ctx, cancel, err := h.PgStorage.beginTx(h.Ctx)
	assert.Nil(h.T, err)
	defer h.PgStorage.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`SELECT
			job_id, job_data, process_id, goroutine_id, goroutine_heart_beat_ts, goroutine_lease_expire_ts, status,
			next_subprocess, run_type, goroutine_ids, exec_count, comp_count, created_ts, start_after_ts,
			started_ts, end_ts, last_update_ts
		FROM %v.%v
		WHERE status=$1 and end_ts <= $2
	`, h.PgStorage.schema, h.PgStorage.table)
	rows, err := tx.Query(ctx, query, JSDone, endTsLte)
	assert.Nil(h.T, err)
	defer rows.Close()

	res := []*Job{}
	for rows.Next() {
		job := Job{}
		err = h.PgStorage.convertJob(rows, &job)
		assert.Nil(h.T, err)
		res = append(res, &job)
	}

	err = rows.Err()
	assert.Nil(h.T, err)

	return res
}

// GetAllJobs returns all jobs.
func (h *PgTestHelper) GetAllJobs() map[string]*Job {
	tx, ctx, cancel, err := h.PgStorage.beginTx(h.Ctx)
	assert.Nil(h.T, err)
	defer h.PgStorage.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`SELECT
			job_id, job_data, process_id, goroutine_id, goroutine_heart_beat_ts, goroutine_lease_expire_ts, status,
			next_subprocess, run_type, goroutine_ids, exec_count, comp_count, created_ts, start_after_ts,
			started_ts, end_ts, last_update_ts
		FROM %v.%v
	`, h.PgStorage.schema, h.PgStorage.table)
	rows, err := tx.Query(ctx, query)
	assert.Nil(h.T, err)
	defer rows.Close()

	res := map[string]*Job{}
	for rows.Next() {
		job := Job{}
		err = h.PgStorage.convertJob(rows, &job)
		assert.Nil(h.T, err)
		res[job.JobId] = &job
	}

	err = rows.Err()
	assert.Nil(h.T, err)

	return res
}

// SetJobRunType updates the run_type field for a job.
func (h *PgTestHelper) SetJobRunType(t *testing.T, jobId string, runType RunType) {
	tx, ctx, cancel, err := h.PgStorage.beginTx(h.Ctx)
	assert.Nil(t, err)
	defer h.PgStorage.rollback(ctx, cancel, tx)

	query := fmt.Sprintf(`UPDATE
			%v.%v
		SET
			run_type = $1
		WHERE
			job_id = $2`,
		h.PgStorage.schema, h.PgStorage.table,
	)
	_, err = tx.Exec(h.Ctx, query, runType, jobId)
	assert.Nil(t, err)

	err = tx.Commit(h.Ctx)
	assert.Nil(t, err)
}

// CreateJobUpdaterForTest creates a JobDataUpdater for testing.
func CreateJobUpdaterForTest[D JobData](
	t *testing.T,
	ctx context.Context,
	process Process[D],
	storage Storage,
	job *Job,
	executor *Executor[D],
) JobDataUpdater[D] {
	return func(jobData D) error {
		serialized, err := process.Serialize(jobData)
		if err != nil {
			return err
		}
		job.JobData = serialized
		return storage.UpdateJob(ctx, job, executor.config.LeaseExpireDuration)
	}
}
