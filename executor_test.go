package ssproc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
)

func TestExecutor_SingleJob(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-executor-single")

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:          2,
		HeartbeatInterval:   5 * time.Second,
		LeaseExpireDuration: 10 * time.Second,
		SweepInterval:       100 * time.Millisecond,
		ExecutionTimeout:    30 * time.Second,
		MaxExecutionCount:   3,
	})
	assert.Nil(t, err)

	// Register and execute directly
	jobId := uuid.New().String()
	jobData := TestJobData{ID: jobId, Counter: 0}
	traceId := uuid.New().String()

	result, err := executor.RegisterExecuteWait(ctx, traceId, jobData)
	assert.Nil(t, err)
	assert.Equal(t, 1, result.Counter)

	// Verify job is done
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSDone, found.Status)
}

func TestExecutor_MultipleJobs(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-executor-multi")

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:          4,
		HeartbeatInterval:   5 * time.Second,
		LeaseExpireDuration: 10 * time.Second,
		SweepInterval:       100 * time.Millisecond,
		ExecutionTimeout:    30 * time.Second,
		MaxExecutionCount:   3,
	})
	assert.Nil(t, err)
	executor.Start()
	defer executor.Stop()

	// Register multiple jobs using client
	client := NewClientSimple(state.Storage, process)
	jobIds := make([]string, 5)
	for i := 0; i < 5; i++ {
		jobId := uuid.New().String()
		jobIds[i] = jobId
		err := client.Register(ctx, TestJobData{ID: jobId, Counter: 0})
		assert.Nil(t, err)
	}

	// Wait for all jobs to complete
	state.h.WaitAllJobsDone(t, 30*time.Second, process.Id())

	// Verify all jobs are done
	for _, jobId := range jobIds {
		found, _ := state.h.GetJob(t, jobId)
		assert.Equal(t, JSDone, found.Status)
	}
}

func TestExecutor_ExecuteNow(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-executor-now")

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:          2,
		HeartbeatInterval:   5 * time.Second,
		LeaseExpireDuration: 10 * time.Second,
		SweepInterval:       1 * time.Hour, // Long interval so sweep doesn't pick it up
		ExecutionTimeout:    30 * time.Second,
		MaxExecutionCount:   3,
	})
	assert.Nil(t, err)

	// Register a job using client
	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, TestJobData{ID: jobId, Counter: 0})
	assert.Nil(t, err)

	// Execute now
	executor.ExecuteNow(jobId)

	// Verify job is done
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSDone, found.Status)
}

func TestClient_Register(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-client-register")
	client := NewClientSimple(state.Storage, process)

	jobId := uuid.New().String()
	err := client.Register(ctx, TestJobData{ID: jobId, Counter: 0})
	assert.Nil(t, err)

	// Verify job was registered
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, jobId, found.JobId)
	assert.Equal(t, JSReady, found.Status)
	assert.Equal(t, process.Id(), found.ProcessId)
}

func TestClient_RegisterStartAfter(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-client-start-after")
	client := NewClientSimple(state.Storage, process)

	jobId := uuid.New().String()
	startAfter := time.Now().Add(1 * time.Hour)
	err := client.RegisterStartAfter(ctx, TestJobData{ID: jobId, Counter: 0}, startAfter)
	assert.Nil(t, err)

	// Verify job was registered with correct start time
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, jobId, found.JobId)
	assert.Equal(t, JSReady, found.Status)
	// StartAfterTs should be close to what we set
	diff := found.StartAfterTs.Sub(startAfter)
	assert.Less(t, diff.Abs(), 1*time.Second)
}

func TestExecutor_Cleanup(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-executor-cleanup")

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:          2,
		HeartbeatInterval:   5 * time.Second,
		LeaseExpireDuration: 10 * time.Second,
		SweepInterval:       100 * time.Millisecond,
		ExecutionTimeout:    30 * time.Second,
		MaxExecutionCount:   3,
		EnableCleanup:       true,
		CleanupInterval:     200 * time.Millisecond,
		CleanupThreshold:    1 * time.Millisecond,
		CleanupBatchSize:    100,
	})
	assert.Nil(t, err)
	executor.Start()
	defer executor.Stop()

	// Register and execute a job
	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, TestJobData{ID: jobId, Counter: 0})
	assert.Nil(t, err)

	// Wait for job to complete
	state.h.WaitJobStatus(t, 10*time.Second, jobId, JSDone)

	// Wait for cleanup
	err = testutil.Await(10*time.Second, func() bool {
		jobs := state.h.GetAllJobs()
		return len(jobs) == 0
	})
	assert.Nil(t, err)
}
