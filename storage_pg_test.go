package ssproc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
)

func TestPgStorage_RegisterJob(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	job := &Job{
		JobId:        "test-job-1",
		JobData:      `{"id":"test-job-1","counter":0}`,
		ProcessId:    "test-process",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Verify job was registered
	found, isValid := state.h.GetJob(t, job.JobId)
	assert.Equal(t, job.JobId, found.JobId)
	assert.Equal(t, job.JobData, found.JobData)
	assert.Equal(t, job.ProcessId, found.ProcessId)
	assert.Equal(t, JSReady, found.Status)
	assert.Equal(t, false, isValid["goroutine_heart_beat_ts"])
	assert.Equal(t, false, isValid["goroutine_lease_expire_ts"])
}

func TestPgStorage_RegisterJob_DuplicateError(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	job := &Job{
		JobId:        "test-job-dup",
		JobData:      `{"id":"test-job-dup","counter":0}`,
		ProcessId:    "test-process",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Try to register again
	err = state.Storage.RegisterJob(ctx, job)
	assert.ErrorIs(t, err, JobIdAlreadyExist)
}

func TestPgStorage_GetOpenJobCandidates(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	// Register a job
	job := &Job{
		JobId:        "test-job-open",
		JobData:      `{"id":"test-job-open","counter":0}`,
		ProcessId:    "test-process-open",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Get open job candidates
	jobIds, err := state.Storage.GetOpenJobCandidates(ctx, "test-process-open", 10)
	assert.Nil(t, err)
	assert.Contains(t, jobIds, "test-job-open")
}

func TestPgStorage_TryTakeOverJob(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	job := &Job{
		JobId:        "test-job-takeover",
		JobData:      `{"id":"test-job-takeover","counter":0}`,
		ProcessId:    "test-process",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Take over the job
	taken, err := state.Storage.TryTakeOverJob(ctx, job.JobId, "goroutine-1", 1*time.Minute)
	assert.Nil(t, err)
	assert.Equal(t, "goroutine-1", taken.GoroutineId)
	assert.False(t, taken.GoroutineHeartBeatTs.IsZero())
	assert.False(t, taken.GoroutineLeaseExpireTs.IsZero())
}

func TestPgStorage_SendHeartbeat(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	job := &Job{
		JobId:        "test-job-heartbeat",
		JobData:      `{"id":"test-job-heartbeat","counter":0}`,
		ProcessId:    "test-process",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Take over the job
	_, err = state.Storage.TryTakeOverJob(ctx, job.JobId, "goroutine-1", 1*time.Minute)
	assert.Nil(t, err)

	// Send heartbeat
	err = state.Storage.SendHeartbeat(ctx, "goroutine-1", job.JobId, 1*time.Minute)
	assert.Nil(t, err)
}

func TestPgStorage_UpdateJob(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	job := &Job{
		JobId:        "test-job-update",
		JobData:      `{"id":"test-job-update","counter":0}`,
		ProcessId:    "test-process",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Take over the job
	taken, err := state.Storage.TryTakeOverJob(ctx, job.JobId, "goroutine-1", 1*time.Minute)
	assert.Nil(t, err)

	// Update the job
	taken.JobData = `{"id":"test-job-update","counter":1}`
	taken.ExecCount = 1
	taken.StartedTs = now
	taken.NextSubprocess = 1
	err = state.Storage.UpdateJob(ctx, taken, 1*time.Minute)
	assert.Nil(t, err)

	// Verify update
	found, _ := state.h.GetJob(t, job.JobId)
	assert.Equal(t, `{"id":"test-job-update","counter":1}`, found.JobData)
	assert.Equal(t, 1, found.ExecCount)
	assert.Equal(t, 1, found.NextSubprocess)
}

func TestPgStorage_ClearDoneJobs(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	// Create a done job
	job := &Job{
		JobId:        "test-job-cleanup",
		JobData:      `{"id":"test-job-cleanup","counter":0}`,
		ProcessId:    "test-process-cleanup",
		Status:       JSReady,
		CreatedTs:    now,
		StartAfterTs: now,
	}

	err := state.Storage.RegisterJob(ctx, job)
	assert.Nil(t, err)

	// Take over and complete the job
	taken, err := state.Storage.TryTakeOverJob(ctx, job.JobId, "goroutine-1", 1*time.Minute)
	assert.Nil(t, err)

	taken.Status = JSDone
	taken.EndTs = now.Add(-1 * time.Hour) // Set end time in the past
	taken.GoroutineIds = []string{"goroutine-1"}
	err = state.Storage.UpdateJob(ctx, taken, 1*time.Minute)
	assert.Nil(t, err)

	// Clear done jobs
	deleted, err := state.Storage.ClearDoneJobs(ctx, "test-process-cleanup", now, 100)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), deleted)
}

func TestPgStorage_FilterJobs(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	now := time.Now()

	// Register multiple jobs
	for i := 0; i < 5; i++ {
		job := &Job{
			JobId:        testutil.RandomAlphaNum(16),
			JobData:      `{"id":"filter-test","counter":0}`,
			ProcessId:    "test-process-filter",
			Status:       JSReady,
			CreatedTs:    now,
			StartAfterTs: now,
		}
		err := state.Storage.RegisterJob(ctx, job)
		assert.Nil(t, err)
	}

	// Filter jobs
	jobs, total, err := state.Storage.FilterJobs(ctx, FilterJob{
		ProcessId: "test-process-filter",
	}, 1, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, total)
	assert.Equal(t, 5, len(jobs))
}
