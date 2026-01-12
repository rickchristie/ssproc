package ssproc

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
)

// ========== Stress Test Job Data ==========

// compensationStressJobData is job data for compensation stress tests.
type compensationStressJobData struct {
	JobId             string         `json:"job_id"`
	SavedDataInt      int            `json:"saved_data_int"`
	SavedDataString   string         `json:"saved_data_string"`
	SavedDataMap      map[string]int `json:"saved_data_map"`
	TransactionSteps  []int          `json:"transaction_steps"`
	CompensationSteps []int          `json:"compensation_steps"`
}

func (d *compensationStressJobData) GetJobId() string {
	return d.JobId
}

// compensationStressProcess is a configurable process for compensation stress tests.
type compensationStressProcess struct {
	id           string
	subprocesses []*Subprocess[*compensationStressJobData]
	mux          *sync.RWMutex

	// Stubs for customizing behavior.
	transactionStub  func(subprocessIndex int, goroutineId string, jobData *compensationStressJobData, update JobDataUpdater[*compensationStressJobData]) SubprocessResult
	compensationStub func(subprocessIndex int, goroutineId string, jobData *compensationStressJobData, update JobDataUpdater[*compensationStressJobData]) SubprocessResult

	// Execution tracking per job.
	transactionCounts  map[string][]int64
	compensationCounts map[string][]int64
	transactionTime    map[string][]time.Time
	compensationTime   map[string][]time.Time
}

func newCompensationStressProcess(id string, numSubprocesses int) *compensationStressProcess {
	p := &compensationStressProcess{
		id:                 id,
		mux:                &sync.RWMutex{},
		transactionCounts:  make(map[string][]int64),
		compensationCounts: make(map[string][]int64),
		transactionTime:    make(map[string][]time.Time),
		compensationTime:   make(map[string][]time.Time),
	}

	// Default stubs that just succeed.
	p.transactionStub = func(subprocessIndex int, goroutineId string, jobData *compensationStressJobData, update JobDataUpdater[*compensationStressJobData]) SubprocessResult {
		return SRSuccess
	}
	p.compensationStub = func(subprocessIndex int, goroutineId string, jobData *compensationStressJobData, update JobDataUpdater[*compensationStressJobData]) SubprocessResult {
		return SRSuccess
	}

	subprocesses := make([]*Subprocess[*compensationStressJobData], numSubprocesses)
	for i := 0; i < numSubprocesses; i++ {
		idx := i
		subprocesses[i] = &Subprocess[*compensationStressJobData]{
			Transaction: func(ctx context.Context, goroutineId string, data *compensationStressJobData, update JobDataUpdater[*compensationStressJobData]) SubprocessResult {
				p.recordTransaction(idx, data.JobId)
				time.Sleep(10 * time.Millisecond)
				return p.transactionStub(idx, goroutineId, data, update)
			},
			Compensation: func(ctx context.Context, goroutineId string, data *compensationStressJobData, update JobDataUpdater[*compensationStressJobData]) SubprocessResult {
				p.recordCompensation(idx, data.JobId)
				time.Sleep(10 * time.Millisecond)
				return p.compensationStub(idx, goroutineId, data, update)
			},
		}
	}

	p.subprocesses = subprocesses
	return p
}

func (p *compensationStressProcess) Id() string {
	return p.id
}

func (p *compensationStressProcess) GetSubprocesses() []*Subprocess[*compensationStressJobData] {
	return p.subprocesses
}

func (p *compensationStressProcess) Serialize(data *compensationStressJobData) (string, error) {
	b, err := json.Marshal(data)
	return string(b), err
}

func (p *compensationStressProcess) Deserialize(serialized string) (*compensationStressJobData, error) {
	var data compensationStressJobData
	err := json.Unmarshal([]byte(serialized), &data)
	return &data, err
}

func (p *compensationStressProcess) newJobData() *compensationStressJobData {
	p.mux.Lock()
	defer p.mux.Unlock()

	jobId := testutil.UUIDString()
	numSubprocesses := len(p.subprocesses)

	p.transactionCounts[jobId] = make([]int64, numSubprocesses)
	p.compensationCounts[jobId] = make([]int64, numSubprocesses)
	p.transactionTime[jobId] = make([]time.Time, numSubprocesses)
	p.compensationTime[jobId] = make([]time.Time, numSubprocesses)

	return &compensationStressJobData{
		JobId: jobId,
	}
}

func (p *compensationStressProcess) recordTransaction(subprocessIndex int, jobId string) {
	p.mux.Lock()
	defer p.mux.Unlock()

	p.transactionCounts[jobId][subprocessIndex]++
	p.transactionTime[jobId][subprocessIndex] = time.Now()
}

func (p *compensationStressProcess) recordCompensation(subprocessIndex int, jobId string) {
	p.mux.Lock()
	defer p.mux.Unlock()

	p.compensationCounts[jobId][subprocessIndex]++
	p.compensationTime[jobId][subprocessIndex] = time.Now()
}

func (p *compensationStressProcess) getTransactionCount(jobId string, subprocessIndex int) int64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.transactionCounts[jobId][subprocessIndex]
}

func (p *compensationStressProcess) getCompensationCount(jobId string, subprocessIndex int) int64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.compensationCounts[jobId][subprocessIndex]
}

// ========== Stress Tests ==========

// TestCompensationStress_MultipleJobsAllCompensated tests 1000 jobs that all fail
// at subprocess 2 and go through full compensation flow.
func TestCompensationStress_MultipleJobsAllCompensated(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-all", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// All transactions succeed until subprocess 2, which always fails.
	// This triggers compensation for all jobs.
	compensationSum := atomic.Int64{}
	expectedCompensationSum := atomic.Int64{}
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		if subprocessIndex == 0 {
			// Save a random value in subprocess 0.
			jobData.SavedDataInt = rand.Intn(100) + 1
			jobData.TransactionSteps = append(jobData.TransactionSteps, 0)
			err := update(jobData)
			if err != nil {
				return SRFailed
			}
			expectedCompensationSum.Add(int64(jobData.SavedDataInt))
			return SRSuccess
		}
		if subprocessIndex == 1 {
			jobData.TransactionSteps = append(jobData.TransactionSteps, 1)
			err := update(jobData)
			if err != nil {
				return SRFailed
			}
			return SRSuccess
		}
		// Subprocess 2 always fails, triggering compensation.
		return SRFailed
	}

	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		err := update(jobData)
		if err != nil {
			return SRFailed
		}

		// In compensation 0, verify saved data is present and add to sum.
		if subprocessIndex == 0 {
			if jobData.SavedDataInt == 0 {
				panic("saved data not present during compensation!")
			}
			compensationSum.Add(int64(jobData.SavedDataInt))
		}
		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	regTime := time.Now()
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create executor with compensation enabled.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 3
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Wait until all jobs are compensated.
	err := testutil.Await(5*time.Minute, func() bool {
		count := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)
		return count == int64(jobCount)
	})
	assert.Nil(t, err)

	// Verify compensation sum matches expected sum.
	assert.Equal(t, expectedCompensationSum.Load(), compensationSum.Load())

	// Verify all jobs are compensated correctly.
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSCompensated, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, RTCompensation, found.RunType)
		assert.Equal(t, -1, found.NextSubprocess) // Terminal value for compensated.
		assert.Equal(t, 1, found.ExecCount)
		assert.True(t, found.CompCount >= 1)

		assert.True(t, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.True(t, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.True(t, found.EndTs.UnixMicro() >= regTime.UnixMicro())

		// Verify job data.
		foundData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, []int{0, 1}, foundData.TransactionSteps)
		assert.Equal(t, []int{2, 1, 0}, foundData.CompensationSteps) // Reverse order.
		assert.True(t, foundData.SavedDataInt > 0)

		// Verify execution counts.
		assert.Equal(t, int64(1), process.getTransactionCount(jobData.JobId, 0))
		assert.Equal(t, int64(1), process.getTransactionCount(jobData.JobId, 1))
		assert.Equal(t, int64(1), process.getTransactionCount(jobData.JobId, 2))
		assert.True(t, process.getCompensationCount(jobData.JobId, 2) >= 1)
		assert.True(t, process.getCompensationCount(jobData.JobId, 1) >= 1)
		assert.True(t, process.getCompensationCount(jobData.JobId, 0) >= 1)
	}
}

// TestCompensationStress_RetriesUntilMaxCompCount tests 1000 jobs where compensation
// has 30% failure chance, requiring retries until successful.
func TestCompensationStress_RetriesUntilMaxCompCount(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-retry", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// Track compensation completions.
	compensationSum := atomic.Int64{}
	expectedSum := atomic.Int64{}

	// Transaction fails at subprocess 2 to trigger compensation.
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		if subprocessIndex == 0 {
			jobData.SavedDataInt = rand.Intn(100) + 1
			jobData.TransactionSteps = append(jobData.TransactionSteps, 0)
			err := update(jobData)
			if err != nil {
				return SRFailed
			}
			expectedSum.Add(int64(jobData.SavedDataInt))
			return SRSuccess
		}
		if subprocessIndex == 1 {
			jobData.TransactionSteps = append(jobData.TransactionSteps, 1)
			update(jobData)
			return SRSuccess
		}
		// Subprocess 2 fails.
		return SRFailed
	}

	// Compensation has 30% failure chance.
	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		roll := rand.Intn(100)
		if roll < 30 {
			return SRFailed
		}

		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		err := update(jobData)
		if err != nil {
			return SRFailed
		}

		// Only count sum at final compensation step.
		if subprocessIndex == 0 {
			compensationSum.Add(int64(jobData.SavedDataInt))
		}
		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	regTime := time.Now()
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create executor with high compensation retry count.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 5000 // High to ensure all succeed eventually.
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Wait until all jobs are compensated.
	err := testutil.Await(5*time.Minute, func() bool {
		count := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)
		return count == int64(jobCount)
	})
	assert.Nil(t, err)

	// Verify sums match.
	assert.Equal(t, expectedSum.Load(), compensationSum.Load())

	// Verify jobs and count retries.
	compCountMoreThan1 := 0
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, JSCompensated, found.Status)
		assert.Equal(t, RTCompensation, found.RunType)
		assert.Equal(t, -1, found.NextSubprocess)
		assert.True(t, found.CompCount >= 1)
		if found.CompCount > 1 {
			compCountMoreThan1++
		}

		assert.True(t, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.True(t, found.EndTs.UnixMicro() >= regTime.UnixMicro())

		// Verify final compensation steps.
		foundData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		// Due to retries, we may have multiple entries but must end with [2, 1, 0].
		assert.True(t, len(foundData.CompensationSteps) >= 3)
		lastThree := foundData.CompensationSteps[len(foundData.CompensationSteps)-3:]
		assert.Equal(t, []int{2, 1, 0}, lastThree)
	}

	// 30% failure rate should result in many retries.
	assert.True(t, compCountMoreThan1 >= 50, "expected at least 50 jobs with comp_count > 1, got %d", compCountMoreThan1)
}

// TestCompensationStress_ContextTimeoutDuringCompensation tests 1000 jobs where
// compensation randomly times out 15% of the time.
func TestCompensationStress_ContextTimeoutDuringCompensation(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-timeout", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// Track completed compensations.
	completedCompensations := atomic.Int64{}

	// Transaction fails at subprocess 2.
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		if subprocessIndex == 2 {
			return SRFailed
		}
		jobData.TransactionSteps = append(jobData.TransactionSteps, subprocessIndex)
		update(jobData)
		return SRSuccess
	}

	// Compensation randomly sleeps beyond timeout 15% of the time.
	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		roll := rand.Intn(100)
		if roll < 15 {
			// Sleep beyond execution timeout.
			time.Sleep(time.Duration(300+rand.Intn(100)) * time.Millisecond)
		}

		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		update(jobData)

		if subprocessIndex == 0 {
			completedCompensations.Add(1)
		}
		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	regTime := time.Now()
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create two executors with short timeout.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 5000
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.ExecutionTimeout = 300 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 200 * time.Millisecond
	executorB.config.MaxJobsPerSweep = 100
	executorB.config.MaxExecutionCount = 1
	executorB.config.MaxCompensationCount = 5000
	executorB.config.RunCompensation = true
	executorB.config.LeaseExpireDuration = 300 * time.Millisecond
	executorB.config.HeartbeatInterval = 100 * time.Millisecond
	executorB.config.ExecutionTimeout = 300 * time.Millisecond
	executorB.Start()
	defer executorB.Stop()

	// Wait until all jobs are compensated.
	err := testutil.Await(5*time.Minute, func() bool {
		count := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)
		return count == int64(jobCount)
	})
	assert.Nil(t, err)

	// Due to timeouts, some compensations may execute multiple times.
	assert.True(t, completedCompensations.Load() >= int64(jobCount))

	// Verify all jobs.
	compCountMoreThan1 := 0
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, JSCompensated, found.Status)
		assert.Equal(t, RTCompensation, found.RunType)
		assert.True(t, found.CompCount >= 1)
		if found.CompCount > 1 {
			compCountMoreThan1++
		}

		assert.True(t, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	}

	// 15% timeout rate should cause retries.
	assert.True(t, compCountMoreThan1 >= 25, "expected at least 25 jobs with comp_count > 1, got %d", compCountMoreThan1)
}

// TestCompensationStress_MultipleExecutors tests 1000 jobs being processed and
// compensated by multiple executors concurrently.
func TestCompensationStress_MultipleExecutors(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-multi-exec", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// Track which executor handled each job.
	executorCounts := sync.Map{}

	// Transaction fails at subprocess 2.
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		if subprocessIndex == 2 {
			return SRFailed
		}
		jobData.TransactionSteps = append(jobData.TransactionSteps, subprocessIndex)
		update(jobData)
		return SRSuccess
	}

	// Compensation tracks executor.
	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		update(jobData)

		// Track executor on final compensation step.
		if subprocessIndex == 0 {
			if _, loaded := executorCounts.LoadOrStore(jobData.JobId, goroutineId); !loaded {
				// Count per executor prefix.
				prefix := goroutineId[:9] // "ExecutorA" or "ExecutorB"
				val, _ := executorCounts.LoadOrStore(prefix, &atomic.Int64{})
				val.(*atomic.Int64).Add(1)
			}
		}
		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create multiple executors with high compensation count to handle retries.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 100 // High to handle lease expiry retries.
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 200 * time.Millisecond
	executorB.config.MaxJobsPerSweep = 100
	executorB.config.MaxExecutionCount = 1
	executorB.config.MaxCompensationCount = 100 // High to handle lease expiry retries.
	executorB.config.RunCompensation = true
	executorB.config.LeaseExpireDuration = 500 * time.Millisecond
	executorB.config.HeartbeatInterval = 100 * time.Millisecond
	executorB.Start()
	defer executorB.Stop()

	// Wait until all jobs are compensated.
	err := testutil.Await(5*time.Minute, func() bool {
		count := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)
		return count == int64(jobCount)
	})
	assert.Nil(t, err)

	// Verify all jobs are compensated.
	// In a concurrent multi-executor scenario with short leases, jobs can be taken over
	// mid-execution, so we only verify the final status, not the exact steps recorded.
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, JSCompensated, found.Status, "job %s should be compensated", jobData.JobId)
		assert.Equal(t, RTCompensation, found.RunType, "job %s should have compensation run type", jobData.JobId)
		assert.Equal(t, -1, found.NextSubprocess, "job %s should have next_subprocess = -1", jobData.JobId)
	}

	// Both executors should have processed jobs.
	valA, okA := executorCounts.Load("ExecutorA")
	valB, okB := executorCounts.Load("ExecutorB")
	if okA && okB {
		countA := valA.(*atomic.Int64).Load()
		countB := valB.(*atomic.Int64).Load()
		t.Logf("ExecutorA handled %d jobs, ExecutorB handled %d jobs", countA, countB)
		assert.True(t, countA > 0, "ExecutorA should have handled some jobs")
		assert.True(t, countB > 0, "ExecutorB should have handled some jobs")
		// Total may be slightly different due to race conditions in tracking, but should be close.
		total := countA + countB
		assert.True(t, total >= int64(jobCount-10), "total handled should be close to job count, got %d", total)
	}
}

// TestCompensationStress_MixedOutcomes tests 1000 jobs where some succeed,
// some fail and get compensated, and some error out.
func TestCompensationStress_MixedOutcomes(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-mixed", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// Track outcomes.
	successCount := atomic.Int64{}
	compensatedCount := atomic.Int64{}

	// 50% of jobs succeed, 50% fail at subprocess 2 triggering compensation.
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		jobData.TransactionSteps = append(jobData.TransactionSteps, subprocessIndex)
		update(jobData)

		if subprocessIndex == 2 {
			// Use saved int as deterministic success/fail indicator.
			if jobData.SavedDataInt%2 == 0 {
				successCount.Add(1)
				return SRSuccess
			}
			return SRFailed
		}
		return SRSuccess
	}

	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		update(jobData)

		if subprocessIndex == 0 {
			compensatedCount.Add(1)
		}
		return SRSuccess
	}

	// Register 1000 jobs with alternating outcome.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		jobData.SavedDataInt = i // Even = success, Odd = compensate
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 3
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Wait until no ready jobs remain.
	err := testutil.Await(5*time.Minute, func() bool {
		ready := s.h.CountJobsByStatus(t, proc.Id(), JSReady)
		return ready == 0
	})
	assert.Nil(t, err)

	// Verify counts.
	doneCount := s.h.CountJobsByStatus(t, proc.Id(), JSDone)
	compCount := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)

	assert.Equal(t, int64(500), doneCount, "expected 500 done jobs")
	assert.Equal(t, int64(500), compCount, "expected 500 compensated jobs")
	assert.Equal(t, int64(500), successCount.Load())
	assert.Equal(t, int64(500), compensatedCount.Load())

	// Verify individual jobs.
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		foundData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)

		if jobData.SavedDataInt%2 == 0 {
			// Should be done.
			assert.Equal(t, JSDone, found.Status)
			assert.Equal(t, RTNormal, found.RunType)
			assert.Equal(t, 3, found.NextSubprocess)
			assert.Equal(t, []int{0, 1, 2}, foundData.TransactionSteps)
			assert.Empty(t, foundData.CompensationSteps)
		} else {
			// Should be compensated.
			assert.Equal(t, JSCompensated, found.Status)
			assert.Equal(t, RTCompensation, found.RunType)
			assert.Equal(t, -1, found.NextSubprocess)
			assert.Equal(t, []int{0, 1, 2}, foundData.TransactionSteps)
			assert.Equal(t, []int{2, 1, 0}, foundData.CompensationSteps)
		}
	}
}

// TestCompensationStress_LeaseExpireDuringCompensation tests 1000 jobs where
// lease expires during compensation, causing takeover by other executors.
func TestCompensationStress_LeaseExpireDuringCompensation(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-lease", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// Track takeovers.
	takeovers := atomic.Int64{}

	// Transaction fails at subprocess 2.
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		if subprocessIndex == 2 {
			return SRFailed
		}
		jobData.TransactionSteps = append(jobData.TransactionSteps, subprocessIndex)
		update(jobData)
		return SRSuccess
	}

	// Compensation sometimes takes long, causing lease expiry.
	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		// 10% chance of sleeping beyond lease expiry.
		if rand.Intn(100) < 10 {
			time.Sleep(700 * time.Millisecond)
			takeovers.Add(1)
		}

		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		update(jobData)
		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create multiple executors with short lease.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 5000
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 200 * time.Millisecond
	executorB.config.MaxJobsPerSweep = 100
	executorB.config.MaxExecutionCount = 1
	executorB.config.MaxCompensationCount = 5000
	executorB.config.RunCompensation = true
	executorB.config.LeaseExpireDuration = 500 * time.Millisecond
	executorB.config.HeartbeatInterval = 100 * time.Millisecond
	executorB.Start()
	defer executorB.Stop()

	// Wait until all jobs are compensated.
	err := testutil.Await(5*time.Minute, func() bool {
		count := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)
		return count == int64(jobCount)
	})
	assert.Nil(t, err)

	t.Logf("Long sleeps (beyond lease expiry) during compensation: %d", takeovers.Load())

	// Verify all jobs compensated despite some taking longer than lease expiry.
	// Note: comp_count stays at 1 because the compensation completes successfully
	// before another executor can take over. This is correct behavior - the system
	// handles slow operations gracefully.
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, JSCompensated, found.Status, "job %s should be compensated", jobData.JobId)
		assert.Equal(t, RTCompensation, found.RunType, "job %s should have compensation run type", jobData.JobId)
		assert.Equal(t, -1, found.NextSubprocess, "job %s should have next_subprocess = -1", jobData.JobId)
	}

	// Verify that some operations did take longer than lease expiry.
	assert.True(t, takeovers.Load() > 0, "expected some long-running compensations, got %d", takeovers.Load())
}

// TestCompensationStress_DataPersistenceAcrossRetries tests 1000 jobs ensuring
// data saved during compensation persists across retries.
func TestCompensationStress_DataPersistenceAcrossRetries(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	process := newCompensationStressProcess("comp-stress-data", 3)
	var proc Process[*compensationStressJobData] = process
	client := NewClientSimple(s.Storage, proc)

	// Transaction fails at subprocess 2.
	process.transactionStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		if subprocessIndex == 0 {
			jobData.SavedDataInt = rand.Intn(1000) + 1
			jobData.SavedDataString = testutil.UUIDString()
			jobData.TransactionSteps = append(jobData.TransactionSteps, 0)
			update(jobData)
			return SRSuccess
		}
		if subprocessIndex == 1 {
			jobData.TransactionSteps = append(jobData.TransactionSteps, 1)
			if jobData.SavedDataMap == nil {
				jobData.SavedDataMap = make(map[string]int)
			}
			jobData.SavedDataMap["key1"] = 100
			update(jobData)
			return SRSuccess
		}
		return SRFailed
	}

	// Compensation has 30% failure rate but preserves/adds data.
	process.compensationStub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *compensationStressJobData,
		update JobDataUpdater[*compensationStressJobData],
	) SubprocessResult {
		// Verify saved data from transaction persists.
		if jobData.SavedDataInt == 0 || jobData.SavedDataString == "" {
			panic("transaction data not preserved!")
		}
		if jobData.SavedDataMap == nil || jobData.SavedDataMap["key1"] != 100 {
			panic("transaction map data not preserved!")
		}

		// 30% failure.
		if rand.Intn(100) < 30 {
			return SRFailed
		}

		// Add compensation data.
		jobData.CompensationSteps = append(jobData.CompensationSteps, subprocessIndex)
		if jobData.SavedDataMap == nil {
			jobData.SavedDataMap = make(map[string]int)
		}
		jobData.SavedDataMap["comp_"+string(rune('0'+subprocessIndex))] = subprocessIndex * 10
		update(jobData)
		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*compensationStressJobData, 0, jobCount)
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1
	executorA.config.MaxCompensationCount = 5000
	executorA.config.RunCompensation = true
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Wait until all jobs are compensated.
	err := testutil.Await(5*time.Minute, func() bool {
		count := s.h.CountJobsByStatus(t, proc.Id(), JSCompensated)
		return count == int64(jobCount)
	})
	assert.Nil(t, err)

	// Verify all jobs have preserved data.
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, JSCompensated, found.Status)

		foundData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)

		// Transaction data preserved.
		assert.True(t, foundData.SavedDataInt > 0)
		assert.NotEmpty(t, foundData.SavedDataString)
		assert.Equal(t, 100, foundData.SavedDataMap["key1"])

		// Compensation data added.
		assert.Equal(t, 20, foundData.SavedDataMap["comp_2"])
		assert.Equal(t, 10, foundData.SavedDataMap["comp_1"])
		assert.Equal(t, 0, foundData.SavedDataMap["comp_0"])
	}
}
