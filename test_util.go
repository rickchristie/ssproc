package ssproc

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/rickchristie/ssproc/util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// PgTestHelper is a testing utilities that can be used to set up states for testing.
//
// IMPORTANT: Do not use this outside of tests. Suggestions on how to best organize test utilities are welcome.
type PgTestHelper struct {
	T         *testing.T
	Ctx       context.Context
	PgStorage *PgStorage
}

func SetProcClientMockTimeForTest[Data JobData](client *Client[Data], mockTime *util.MockTime) {
	client.utilTime = mockTime
}

func SetExecutorMockTimeForTest[Data JobData](executor *Executor[Data], mockTime *util.MockTime) {
	executor.timeUtil = mockTime
}

func (h *PgTestHelper) SetProcMockTimeForTest(mockTime *util.MockTime) {
	h.PgStorage.utilTime = mockTime
}

//goland:noinspection SqlResolve
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

func (h *PgTestHelper) WaitJobStatus(t *testing.T, timeout time.Duration, jobId string, status JobStatus) {
	err := util.Await(timeout, func() bool {
		job, _ := h.GetJob(t, jobId)
		return job.Status == status
	})
	assert.Nil(t, err)
}

func (h *PgTestHelper) WaitJobGoroutineIdChanged(t *testing.T, timeout time.Duration, goroutineIds []string, jobId string) {
	err := util.Await(timeout, func() bool {
		found, _ := h.GetJob(t, jobId)
		return len(goroutineIds) < len(found.GoroutineIds)
	})
	assert.Nil(t, err)

	// Assert that trace is inserted.
	found, _ := h.GetJob(t, jobId)
	assert.Equal(t, len(goroutineIds)+1, len(found.GoroutineIds))
	expected := append(goroutineIds, found.GoroutineIds[len(found.GoroutineIds)-1])
	assert.Equal(t, expected, found.GoroutineIds)
}

func (h *PgTestHelper) WaitAllJobsDone(t *testing.T, timeout time.Duration, processId string) {
	err := util.Await(timeout, func() bool {
		count := h.CountNotDoneJobs(t, processId)
		return count == 0
	})
	assert.Nil(t, err)
}

func assertJobDone[Data JobData](
	t *testing.T,
	h *PgTestHelper,
	process Process[Data],
	jobData Data,
	execCount int,
	nextSubprocess int,
	regTime time.Time,
) {
	found, _ := h.GetJob(t, jobData.GetJobId())
	assert.Equal(t, jobData.GetJobId(), found.JobId)
	assert.Equal(t, JSDone, found.Status)
	assert.NotEmpty(t, found.GoroutineId)
	assert.Equal(t, nextSubprocess, found.NextSubprocess)
	assert.Equal(t, execCount, found.ExecCount)

	assert.Equal(t, RTNormal, found.RunType)
	assert.NotEmpty(t, found.GoroutineIds)

	assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)
}

func assertJobNotTakenOver[Data JobData](
	t *testing.T,
	h *PgTestHelper,
	process Process[Data],
	jobData Data,
	regTime time.Time,
) {
	found, _ := h.GetJob(t, jobData.GetJobId())
	assert.Equal(t, jobData.GetJobId(), found.JobId)
	assert.Equal(t, JSReady, found.Status)
	assert.Empty(t, found.GoroutineId)
	assert.Equal(t, 0, found.NextSubprocess)
	assert.Equal(t, 0, found.ExecCount)

	assert.Equal(t, RTNormal, found.RunType)
	assert.Empty(t, found.GoroutineIds)

	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

	assert.Equal(t, true, found.GoroutineHeartBeatTs.IsZero())
	assert.Equal(t, true, found.StartedTs.IsZero())
	assert.Equal(t, true, found.EndTs.IsZero())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)
}

func assertJobError[Data JobData](
	t *testing.T,
	h *PgTestHelper,
	process Process[Data],
	jobData Data,
	execCount int,
	nextSubprocess int,
	regTime time.Time,
) {
	found, _ := h.GetJob(t, jobData.GetJobId())
	assert.Equal(t, jobData.GetJobId(), found.JobId)
	assert.Equal(t, JSError, found.Status)
	assert.NotEmpty(t, found.GoroutineId)
	assert.Equal(t, nextSubprocess, found.NextSubprocess)
	assert.Equal(t, execCount, found.ExecCount)

	assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)
}

func assertJobFailExec[Data JobData](
	t *testing.T,
	h *PgTestHelper,
	process Process[Data],
	jobData Data,
	execCount int,
	nextSubprocess int,
	regTime time.Time,
) {
	found, _ := h.GetJob(t, jobData.GetJobId())
	assert.Equal(t, jobData.GetJobId(), found.JobId)
	assert.Equal(t, JSReady, found.Status)
	assert.NotEmpty(t, found.GoroutineId)
	assert.Equal(t, nextSubprocess, found.NextSubprocess)
	assert.Equal(t, execCount, found.ExecCount)

	assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.EndTs.IsZero())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)
}

func assertJobDataIsLatest[Data JobData](t *testing.T, h *PgTestHelper, process Process[Data], jobData Data) {
	found, _ := h.GetJob(t, jobData.GetJobId())
	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)
}
