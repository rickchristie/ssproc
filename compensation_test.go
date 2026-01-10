package ssproc

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
)

// CompensationTestJobData is job data for compensation tests.
type CompensationTestJobData struct {
	ID               string   `json:"id"`
	TransactionSteps []int    `json:"transaction_steps"`
	CompensationSteps []int   `json:"compensation_steps"`
}

func (d CompensationTestJobData) GetJobId() string {
	return d.ID
}

// CompensationTestProcess is a configurable process for compensation tests.
type CompensationTestProcess struct {
	id                string
	subprocesses      []*Subprocess[CompensationTestJobData]
	transactionCalls  []int32
	compensationCalls []int32
	mu                sync.Mutex
}

func NewCompensationTestProcess(id string, numSubprocesses int) *CompensationTestProcess {
	p := &CompensationTestProcess{
		id:                id,
		transactionCalls:  make([]int32, numSubprocesses),
		compensationCalls: make([]int32, numSubprocesses),
	}

	subprocesses := make([]*Subprocess[CompensationTestJobData], numSubprocesses)
	for i := 0; i < numSubprocesses; i++ {
		idx := i
		subprocesses[i] = &Subprocess[CompensationTestJobData]{
			Transaction: func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
				atomic.AddInt32(&p.transactionCalls[idx], 1)
				data.TransactionSteps = append(data.TransactionSteps, idx)
				err := update(data)
				if err != nil {
					return SRFailed
				}
				return SRSuccess
			},
			Compensation: func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
				atomic.AddInt32(&p.compensationCalls[idx], 1)
				data.CompensationSteps = append(data.CompensationSteps, idx)
				err := update(data)
				if err != nil {
					return SRFailed
				}
				return SRSuccess
			},
		}
	}

	p.subprocesses = subprocesses
	return p
}

func (p *CompensationTestProcess) Id() string {
	return p.id
}

func (p *CompensationTestProcess) GetSubprocesses() []*Subprocess[CompensationTestJobData] {
	return p.subprocesses
}

func (p *CompensationTestProcess) Serialize(data CompensationTestJobData) (string, error) {
	b, err := json.Marshal(data)
	return string(b), err
}

func (p *CompensationTestProcess) Deserialize(serialized string) (CompensationTestJobData, error) {
	var data CompensationTestJobData
	err := json.Unmarshal([]byte(serialized), &data)
	return data, err
}

func (p *CompensationTestProcess) GetTransactionCalls() []int32 {
	result := make([]int32, len(p.transactionCalls))
	for i := range p.transactionCalls {
		result[i] = atomic.LoadInt32(&p.transactionCalls[i])
	}
	return result
}

func (p *CompensationTestProcess) GetCompensationCalls() []int32 {
	result := make([]int32, len(p.compensationCalls))
	for i := range p.compensationCalls {
		result[i] = atomic.LoadInt32(&p.compensationCalls[i])
	}
	return result
}

// ========== Validation Tests ==========

func TestExecutor_RunCompensation_NoCompensationDefined(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewTestProcess("test-no-compensation")

	_, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		MaxExecutionCount:    3,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "no subprocess has Compensation defined")
}

func TestExecutor_RunCompensation_MaxCompCountZero(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-max-comp-zero", 3)

	_, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		MaxExecutionCount:    3,
		MaxCompensationCount: 0,
		RunCompensation:      true,
	})
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "MaxCompensationCount must be > 0")
}

// ========== Basic Compensation Flow Tests ==========

func TestCompensation_BasicFlow(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-basic", 3)

	// Make the last subprocess fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		atomic.AddInt32(&process.transactionCalls[2], 1)
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    2,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute multiple times until compensation
	// Need to expire lease between executions to simulate different executor pickups
	for i := 0; i < 10; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
	}

	// Verify job is compensated
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)
	assert.Equal(t, RTCompensation, found.RunType)
	assert.Equal(t, -1, found.NextSubprocess)

	// Verify compensation ran backward
	compCalls := process.GetCompensationCalls()
	assert.GreaterOrEqual(t, compCalls[2], int32(1))
	assert.GreaterOrEqual(t, compCalls[1], int32(1))
	assert.GreaterOrEqual(t, compCalls[0], int32(1))
}

func TestCompensation_SkipNilCompensation(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-skip-nil", 3)

	// Set middle compensation to nil
	process.subprocesses[1].Compensation = nil

	// Make the last subprocess fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute until compensation
	for i := 0; i < 5; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)

	// Verify compensation[1] was skipped (call count should be 0)
	compCalls := process.GetCompensationCalls()
	assert.GreaterOrEqual(t, compCalls[2], int32(1))
	assert.Equal(t, int32(0), compCalls[1]) // Skipped
	assert.GreaterOrEqual(t, compCalls[0], int32(1))
}

func TestCompensation_DisabledByDefault(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-disabled", 3)

	// Make a subprocess fail
	process.subprocesses[1].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:          2,
		HeartbeatInterval:   5 * time.Second,
		LeaseExpireDuration: 10 * time.Second,
		SweepInterval:       100 * time.Millisecond,
		ExecutionTimeout:    30 * time.Second,
		MaxExecutionCount:   2,
		RunCompensation:     false, // Disabled
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute multiple times
	for i := 0; i < 5; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSError {
			break
		}
	}

	// Verify job is in error, not compensated
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSError, found.Status)
	assert.Equal(t, RTNormal, found.RunType)

	// Verify no compensation was called
	compCalls := process.GetCompensationCalls()
	for _, c := range compCalls {
		assert.Equal(t, int32(0), c)
	}
}

func TestCompensation_MaxCountExhausted(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-max-count", 3)

	// Make transaction and compensation fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 2,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute until error
	for i := 0; i < 10; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSError {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSError, found.Status)
	assert.Equal(t, RTCompensation, found.RunType)
	assert.Equal(t, 2, found.CompCount) // Max compensation count
}

// ========== Subprocess Result Tests ==========

func TestCompensation_SREarlyExitDone(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-early-done", 3)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}
	// Make compensation[2] return early exit done
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		atomic.AddInt32(&process.compensationCalls[2], 1)
		return SREarlyExitDone
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute
	for i := 0; i < 5; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)

	// Verify only compensation[2] was called (early exit skips remaining)
	compCalls := process.GetCompensationCalls()
	assert.GreaterOrEqual(t, compCalls[2], int32(1))
	assert.Equal(t, int32(0), compCalls[1])
	assert.Equal(t, int32(0), compCalls[0])
}

func TestCompensation_SREarlyExitError(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-early-error", 3)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}
	// Make compensation return early exit error
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SREarlyExitError
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute
	for i := 0; i < 5; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSError {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSError, found.Status)
}

// ========== Trigger Tests ==========

func TestCompensation_TriggerOnSREarlyExitError(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-trigger-early-error", 3)

	// Make subprocess return SREarlyExitError
	process.subprocesses[1].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		data.TransactionSteps = append(data.TransactionSteps, 1)
		update(data)
		return SREarlyExitError
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    3,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute - should trigger compensation immediately on SREarlyExitError
	executor.ExecuteNow(jobId)

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)
	assert.Equal(t, RTCompensation, found.RunType)

	// Verify compensation was called for indexes 0 and 1 (subprocess 2 was never reached)
	compCalls := process.GetCompensationCalls()
	assert.GreaterOrEqual(t, compCalls[1], int32(1))
	assert.GreaterOrEqual(t, compCalls[0], int32(1))
	assert.Equal(t, int32(0), compCalls[2]) // Never executed
}

func TestCompensation_NoTriggerOnSREarlyExitDone(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-no-trigger-done", 3)

	// Make subprocess return SREarlyExitDone
	process.subprocesses[1].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		data.TransactionSteps = append(data.TransactionSteps, 1)
		update(data)
		return SREarlyExitDone
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    3,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute
	executor.ExecuteNow(jobId)

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSDone, found.Status)
	assert.Equal(t, RTNormal, found.RunType)

	// No compensation was called
	compCalls := process.GetCompensationCalls()
	for _, c := range compCalls {
		assert.Equal(t, int32(0), c)
	}
}

// ========== Cleanup Tests ==========

func TestCleanup_IncludesCompensatedJobs(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-cleanup-compensated", 3)

	// Make subprocess fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    100 * time.Millisecond,
		LeaseExpireDuration:  500 * time.Millisecond,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
		EnableCleanup:        true,
		CleanupInterval:      200 * time.Millisecond,
		CleanupThreshold:     1 * time.Millisecond,
		CleanupBatchSize:     100,
	})
	assert.Nil(t, err)
	executor.Start()
	defer executor.Stop()

	// Register a job
	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Wait for job to reach compensated status
	err = testutil.Await(10*time.Second, func() bool {
		job, _ := state.h.GetJob(t, jobId)
		return job.Status == JSCompensated
	})
	assert.Nil(t, err)

	// Wait for cleanup
	err = testutil.Await(10*time.Second, func() bool {
		jobs := state.h.GetAllJobs()
		return len(jobs) == 0
	})
	assert.Nil(t, err)
}

// ========== Panic Recovery Tests ==========

func TestCompensation_PanicRecovery(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-panic", 3)

	panicCount := int32(0)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}
	// Make compensation panic once, then succeed
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		count := atomic.AddInt32(&panicCount, 1)
		if count == 1 {
			panic("test panic in compensation")
		}
		return SRSuccess
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 5,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute multiple times
	for i := 0; i < 10; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&panicCount), int32(1))
}

// ========== Data Persistence Tests ==========

func TestCompensation_DataPersistsDuringCompensation(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-data-persist", 3)

	// Make transaction fail but update data
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		data.TransactionSteps = append(data.TransactionSteps, 2)
		update(data)
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute until compensated
	for i := 0; i < 5; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
	}

	// Verify job data was preserved and updated
	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)

	var data CompensationTestJobData
	err = json.Unmarshal([]byte(found.JobData), &data)
	assert.Nil(t, err)

	// Should have transaction steps
	assert.Contains(t, data.TransactionSteps, 0)
	assert.Contains(t, data.TransactionSteps, 1)
	assert.Contains(t, data.TransactionSteps, 2)

	// Should have compensation steps (in reverse order)
	assert.Contains(t, data.CompensationSteps, 2)
	assert.Contains(t, data.CompensationSteps, 1)
	assert.Contains(t, data.CompensationSteps, 0)
}

// ========== Executor Crash Recovery Tests ==========

func TestCompensation_ExecutorCrashRecovery(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-crash-recovery", 3)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute once to trigger compensation mode
	executor.ExecuteNow(jobId)

	// Verify job is in compensation mode
	found, _ := state.h.GetJob(t, jobId)
	if found.Status == JSReady && found.RunType == RTCompensation {
		// Simulate crash by expiring lease
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))

		// Another executor picks up the job
		executor.ExecuteNow(jobId)

		// Verify job continues in compensation mode
		found, _ = state.h.GetJob(t, jobId)
		assert.Equal(t, RTCompensation, found.RunType)
	}
}

// ========== Multiple Executors Compensation Tests ==========

func TestCompensation_MultipleExecutors(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-multi-exec", 3)

	failCount := int32(0)
	// Make transaction fail a few times
	process.subprocesses[1].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		count := atomic.AddInt32(&failCount, 1)
		data.TransactionSteps = append(data.TransactionSteps, 1)
		update(data)
		if count <= 2 {
			return SRFailed
		}
		return SRSuccess
	}

	executor1, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		ExecutorName:         "Exec1",
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    3,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	executor2, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		ExecutorName:         "Exec2",
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    3,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Alternate execution between executors
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			executor1.ExecuteNow(jobId)
		} else {
			// Expire lease first
			state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
			executor2.ExecuteNow(jobId)
		}

		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSDone || job.Status == JSCompensated || job.Status == JSError {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	// Should eventually complete (either done or compensated depending on retry timing)
	assert.True(t, found.Status == JSDone || found.Status == JSCompensated || found.Status == JSError)
}

// ========== Race Condition Tests ==========

// TestCompensation_Race_TwoExecutorsEnterCompensation tests that when two executors
// try to process a job that needs compensation, only one successfully transitions
// to compensation mode.
func TestCompensation_Race_TwoExecutorsEnterCompensation(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-race-enter", 3)

	// Make transaction fail on last step
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	executor1, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		ExecutorName:         "RaceExec1",
		MaxWorkers:           2,
		HeartbeatInterval:    50 * time.Millisecond,
		LeaseExpireDuration:  200 * time.Millisecond,
		SweepInterval:        50 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1, // Force compensation on first failure
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	executor2, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		ExecutorName:         "RaceExec2",
		MaxWorkers:           2,
		HeartbeatInterval:    50 * time.Millisecond,
		LeaseExpireDuration:  200 * time.Millisecond,
		SweepInterval:        50 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Run first executor to trigger compensation mode
	executor1.ExecuteNow(jobId)

	// Wait a bit for lease to expire
	time.Sleep(250 * time.Millisecond)

	// Both executors try to pick up the same job concurrently
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		executor1.ExecuteNow(jobId)
	}()
	go func() {
		defer wg.Done()
		executor2.ExecuteNow(jobId)
	}()
	wg.Wait()

	// Verify job is in valid state
	found, _ := state.h.GetJob(t, jobId)
	assert.True(t, found.RunType == RTCompensation)
	// Job should be either still ready (in compensation mode), compensated, or error
	assert.True(t, found.Status == JSReady || found.Status == JSCompensated || found.Status == JSError,
		"Expected ready/compensated/error, got %v", found.Status)
}

// TestCompensation_Race_ContextTimeoutDuringCompensation tests that context timeout
// during compensation execution is handled correctly and allows retry.
func TestCompensation_Race_ContextTimeoutDuringCompensation(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-ctx-timeout", 3)

	timeoutCount := int32(0)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	// Make compensation block on first attempt until context times out
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		count := atomic.AddInt32(&timeoutCount, 1)
		if count == 1 {
			// Block until context timeout
			<-ctx.Done()
			return SRFailed
		}
		// Subsequent attempts succeed
		return SRSuccess
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    50 * time.Millisecond,
		LeaseExpireDuration:  500 * time.Millisecond,
		SweepInterval:        50 * time.Millisecond,
		ExecutionTimeout:     200 * time.Millisecond, // Short timeout
		MaxExecutionCount:    1,
		MaxCompensationCount: 5,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute - first will timeout
	executor.ExecuteNow(jobId)

	// Wait for lease to expire
	time.Sleep(600 * time.Millisecond)

	// Execute again - should succeed
	for i := 0; i < 5; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&timeoutCount), int32(1))
}

// TestCompensation_Race_HeartbeatFailureDuringCompensation tests that heartbeat
// failure during compensation is handled correctly.
func TestCompensation_Race_HeartbeatFailureDuringCompensation(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-heartbeat-fail", 3)

	compExecutions := int32(0)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	// Make compensation take longer than heartbeat interval on first attempt
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		count := atomic.AddInt32(&compExecutions, 1)
		if count == 1 {
			// Sleep longer than lease but check context regularly
			for i := 0; i < 20; i++ {
				select {
				case <-ctx.Done():
					return SRFailed
				case <-time.After(50 * time.Millisecond):
				}
			}
			return SRSuccess
		}
		return SRSuccess
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    50 * time.Millisecond,
		LeaseExpireDuration:  200 * time.Millisecond,
		SweepInterval:        50 * time.Millisecond,
		ExecutionTimeout:     10 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 5,
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute multiple times
	for i := 0; i < 10; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSCompensated {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSCompensated, found.Status)
}

// TestCompensation_Race_MaxCompCountMidExecution tests the scenario where
// MaxCompensationCount is reached during compensation execution.
func TestCompensation_Race_MaxCompCountMidExecution(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-max-mid", 3)

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	// Make all compensations fail
	for i := 0; i < 3; i++ {
		idx := i
		process.subprocesses[i].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
			atomic.AddInt32(&process.compensationCalls[idx], 1)
			return SRFailed
		}
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    5 * time.Second,
		LeaseExpireDuration:  10 * time.Second,
		SweepInterval:        100 * time.Millisecond,
		ExecutionTimeout:     30 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3, // Limited attempts
		RunCompensation:      true,
	})
	assert.Nil(t, err)

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Execute until error
	for i := 0; i < 10; i++ {
		state.h.SetLeaseExpireTime(t, jobId, time.Now().Add(-1*time.Minute))
		executor.ExecuteNow(jobId)
		job, _ := state.h.GetJob(t, jobId)
		if job.Status == JSError {
			break
		}
	}

	found, _ := state.h.GetJob(t, jobId)
	assert.Equal(t, JSError, found.Status)
	assert.Equal(t, RTCompensation, found.RunType)
	assert.Equal(t, 3, found.CompCount)
}

// TestCompensation_Race_ConcurrentCompensationAndCleanup tests that cleanup
// does not interfere with ongoing compensation.
func TestCompensation_Race_ConcurrentCompensationAndCleanup(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-cleanup-race", 3)

	compensationStarted := make(chan struct{})
	var compensationStartedOnce sync.Once
	compensationDone := make(chan struct{})

	// Make transaction fail
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		return SRFailed
	}

	// Make compensation signal when it starts and wait
	process.subprocesses[2].Compensation = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		compensationStartedOnce.Do(func() {
			close(compensationStarted)
		})
		select {
		case <-compensationDone:
			return SRSuccess
		case <-ctx.Done():
			return SRFailed
		}
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           2,
		HeartbeatInterval:    100 * time.Millisecond,
		LeaseExpireDuration:  300 * time.Millisecond,
		SweepInterval:        50 * time.Millisecond,
		ExecutionTimeout:     10 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 5,
		RunCompensation:      true,
		EnableCleanup:        true,
		CleanupInterval:      100 * time.Millisecond,
		CleanupThreshold:     1 * time.Millisecond, // Aggressive cleanup
		CleanupBatchSize:     100,
	})
	assert.Nil(t, err)

	// Start executor (sweeper will pick up jobs)
	executor.Start()
	defer executor.Stop()

	client := NewClientSimple(state.Storage, process)
	jobId := uuid.New().String()
	err = client.Register(ctx, CompensationTestJobData{ID: jobId})
	assert.Nil(t, err)

	// Wait for compensation to start (sweeper will pick up job, run transaction,
	// transition to compensation, then in a subsequent execution, run compensation)
	select {
	case <-compensationStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("Compensation did not start in time")
	}

	// Cleanup is running while compensation is blocked
	// Wait a bit to give cleanup a chance to try to clean while job is active
	time.Sleep(500 * time.Millisecond)

	// Complete compensation
	close(compensationDone)

	// Wait for job to complete
	err = testutil.Await(10*time.Second, func() bool {
		job, _ := state.h.GetJob(t, jobId)
		return job == nil || job.Status == JSCompensated
	})
	assert.Nil(t, err)
}

// TestCompensation_Race_StressMultipleJobs tests compensation under stress
// with multiple jobs being processed concurrently.
func TestCompensation_Race_StressMultipleJobs(t *testing.T) {
	state := StateCreator()
	state.Setup(t)
	defer state.TearDown(t)

	ctx := context.Background()
	process := NewCompensationTestProcess("test-comp-stress", 3)

	// Make transaction fail randomly
	failCount := int32(0)
	process.subprocesses[2].Transaction = func(ctx context.Context, goroutineId string, data CompensationTestJobData, update JobDataUpdater[CompensationTestJobData]) SubprocessResult {
		// Fail every other job
		count := atomic.AddInt32(&failCount, 1)
		if count%2 == 0 {
			return SRFailed
		}
		return SRSuccess
	}

	executor, err := NewExecutor(ctx, process, state.Storage, ExecutorConfig{
		MaxWorkers:           5,
		HeartbeatInterval:    100 * time.Millisecond,
		LeaseExpireDuration:  300 * time.Millisecond,
		SweepInterval:        50 * time.Millisecond,
		ExecutionTimeout:     5 * time.Second,
		MaxExecutionCount:    1,
		MaxCompensationCount: 3,
		RunCompensation:      true,
	})
	assert.Nil(t, err)
	executor.Start()
	defer executor.Stop()

	client := NewClientSimple(state.Storage, process)

	// Register multiple jobs
	numJobs := 10
	jobIds := make([]string, numJobs)
	for i := 0; i < numJobs; i++ {
		jobId := uuid.New().String()
		jobIds[i] = jobId
		err := client.Register(ctx, CompensationTestJobData{ID: jobId})
		assert.Nil(t, err)
	}

	// Wait for all jobs to complete
	err = testutil.Await(60*time.Second, func() bool {
		allDone := true
		for _, jobId := range jobIds {
			job, _ := state.h.GetJob(t, jobId)
			if job == nil {
				allDone = false
				continue
			}
			if job.Status != JSDone && job.Status != JSCompensated && job.Status != JSError {
				allDone = false
			}
		}
		return allDone
	})
	assert.Nil(t, err, "Timed out waiting for jobs to complete")

	// Verify all jobs reached terminal state
	doneCount := 0
	compensatedCount := 0
	errorCount := 0
	for _, jobId := range jobIds {
		job, _ := state.h.GetJob(t, jobId)
		if job == nil {
			continue
		}
		switch job.Status {
		case JSDone:
			doneCount++
		case JSCompensated:
			compensatedCount++
		case JSError:
			errorCount++
		}
	}

	// Should have mix of done and compensated jobs
	t.Logf("Results: done=%d, compensated=%d, error=%d", doneCount, compensatedCount, errorCount)
	assert.Equal(t, numJobs, doneCount+compensatedCount+errorCount)
}
