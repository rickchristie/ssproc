package ssproc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"math/rand"
	"rukita.co/main/be/accessor/db/pg/pgtest"
	"rukita.co/main/be/lib/mend"
	"rukita.co/main/be/lib/test"
	"rukita.co/main/be/lib/tr"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var _ JobData = (*singleIncJob)(nil)

type singleIncJob struct {
	JobId     string
	Increment int64
}

func (s *singleIncJob) GetJobId() string {
	return s.JobId
}

var _ Process[*singleIncJob] = (*singleIncProcess)(nil)

type singleIncProcess struct {
	ExecuteStub Execute[*singleIncJob]

	incrementCount   atomic.Int64
	incrementSum     atomic.Int64
	shouldWait       bool
	randomFailures   bool
	failureChancePct int

	processId string
}

func (s *singleIncProcess) Id() string {
	return s.processId
}

func (s *singleIncProcess) Execute(
	ctx context.Context,
	goroutineId string,
	job *singleIncJob,
	update JobDataUpdater[*singleIncJob],
) SubprocessResult {
	if s.shouldWait {
		// Sleep between 0-2000 milliseconds.
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
	}

	if s.ExecuteStub != nil {
		return s.ExecuteStub(ctx, goroutineId, job, update)
	}

	// Randomly fail execution to test retry and recapture.
	if s.randomFailures {
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		roll := random.Intn(100)
		// failureChancePct = 30 means 0-29, 30% chance of failures.
		if roll < s.failureChancePct {
			return SRFailed
		}
	}

	s.incrementCount.Add(1)
	s.incrementSum.Add(job.Increment)
	return SRSuccess
}

func (s *singleIncProcess) GetSubprocesses() []*Subprocess[*singleIncJob] {
	return []*Subprocess[*singleIncJob]{
		{Transaction: s.Execute},
	}
}

func (s *singleIncProcess) Serialize(jobData *singleIncJob) (serialized string, err error) {
	bytes, err := json.Marshal(jobData)
	if err != nil {
		return "", mend.Wrap(err, true)
	}
	return string(bytes), nil
}

func (s *singleIncProcess) Deserialize(serialized string) (jobData *singleIncJob, err error) {
	jobData = &singleIncJob{}
	err = json.Unmarshal([]byte(serialized), jobData)
	if err != nil {
		return nil, mend.Wrap(err, true)
	}
	return jobData, nil
}

func newExecutor[Data JobData](t *testing.T, s *State, proc Process[Data], name string) *Executor[Data] {
	config := ExecutorConfig{
		ExecutorName:        name,
		MaxWorkers:          0,
		MinWorkers:          8,
		HeartbeatInterval:   100 * time.Millisecond,
		LeaseExpireDuration: 3 * time.Second,
		SweepInterval:       1 * time.Second,
		SweepIntervalJitter: 10 * time.Millisecond,
		MaxJobsPerSweep:     100,
		ExecutionTimeout:    10 * time.Second,
		MaxExecutionCount:   3,
	}
	executor, err := NewExecutor(s.Db.DebugCtx, proc, s.Storage, config)
	assert.Nil(t, err)
	return executor
}

func TestSingleSubprocess_SingleJobSuccessFailure(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	process := &singleIncProcess{processId: "increment"}
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.Start()
	defer executorA.Stop()

	t.Run("first job", func(t *testing.T) {
		jobData := &singleIncJob{
			JobId:     "1",
			Increment: 20,
		}
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		// Wait until the job is complete.
		err = test.Await(5*time.Hour, func() bool {
			found, _ := s.h.GetJob(t, jobData.JobId)
			return found.Status == JSDone
		})
		assert.Nil(t, err)

		// Assert result is as expected.
		assert.Equal(t, int64(1), process.incrementCount.Load())
		assert.Equal(t, int64(20), process.incrementSum.Load())

		// Assert job is as expected.
		assertJobDone[*singleIncJob](t, s.h, process, jobData, 1, 1, regTime)
	})

	t.Run("second job, failure rate 100%, will be marked as error", func(t *testing.T) {
		process.failureChancePct = 100
		process.randomFailures = true

		jobData := &singleIncJob{
			JobId:     "2",
			Increment: 20,
		}
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		// Wait until the job is complete.
		// LeaseExpiry is 3 seconds, which means we need to wait ~9 seconds.
		err = test.Await(12*time.Second, func() bool {
			found, _ := s.h.GetJob(t, jobData.JobId)
			return found.Status == JSError
		})
		assert.Nil(t, err)

		// Assert result is as expected.
		assert.Equal(t, int64(1), process.incrementCount.Load())
		assert.Equal(t, int64(20), process.incrementSum.Load())

		// Assert job is as expected.
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSError, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 0, found.NextSubprocess)
		assert.Equal(t, 3, found.ExecCount)

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

		foundJobData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	})
}

func TestSingleSubprocess_StartAfter(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// This process is the correct process.
	process := &singleIncProcess{processId: "increment"}
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 2 * time.Second
	executorA.config.SweepIntervalJitter = 1 * time.Second
	executorA.Start()
	defer executorA.Stop()

	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 2 * time.Second
	executorB.config.SweepIntervalJitter = 1 * time.Second
	executorB.Start()
	defer executorB.Stop()

	// Schedule job 1 to be executed after 7 seconds.
	jobData1 := &singleIncJob{
		JobId:     "1",
		Increment: 10,
	}
	regTime := time.Now()
	startAfter1 := regTime.Add(7 * time.Second)
	err := client.RegisterStartAfter(s.h.Ctx, jobData1, startAfter1)
	assert.Nil(t, err)

	// Schedule job 2 to be executed after 3 seconds.
	jobData2 := &singleIncJob{
		JobId:     "2",
		Increment: 5,
	}
	startAfter2 := regTime.Add(3 * time.Second)
	err = client.RegisterStartAfter(s.h.Ctx, jobData2, startAfter2)
	assert.Nil(t, err)

	// Sweep happens every 2+1 seconds, wait until job 2 is done.
	// Max wait time is always LeaseExpireDuration + SweepInterval + SweepIntervalJitter
	s.h.WaitJobStatus(t, 10*time.Second, jobData2.JobId, JSDone)

	// Assert second job is done.
	assertJobDone[*singleIncJob](t, s.h, process, jobData2, 1, 1, regTime)

	// Assert only Job2 has incremented.
	assert.Equal(t, int64(5), process.incrementSum.Load())
	assert.Equal(t, int64(1), process.incrementCount.Load())

	// Assert first job is still not done.
	found, _ := s.h.GetJob(t, jobData1.JobId)
	assert.Equal(t, jobData1.JobId, found.JobId)
	assert.Equal(t, JSReady, found.Status)
	assert.Empty(t, found.GoroutineId)
	assert.Equal(t, 0, found.NextSubprocess)
	assert.Equal(t, 0, found.ExecCount)

	assert.Equal(t, true, found.GoroutineHeartBeatTs.IsZero())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.IsZero())
	assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= startAfter1.UnixMicro())
	assert.Equal(t, true, found.EndTs.IsZero())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData1, foundJobData)

	// Wait until the first job is executed.
	// Max wait time is always LeaseExpireDuration + SweepInterval + SweepIntervalJitter.
	s.h.WaitJobStatus(t, 10*time.Second, jobData1.JobId, JSDone)

	// Assert job is executed.
	assert.Equal(t, int64(15), process.incrementSum.Load())
	assert.Equal(t, int64(2), process.incrementCount.Load())

	// Assert job 1 is done.
	assertJobDone[*singleIncJob](t, s.h, process, jobData1, 1, 1, regTime)

	// Assert job 1 is started and done after the start time.
	found, _ = s.h.GetJob(t, jobData1.JobId)
	assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= startAfter1.UnixMicro())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.UnixMicro() >= startAfter1.UnixMicro())
	assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= startAfter1.UnixMicro())
	assert.Equal(t, true, found.EndTs.UnixMicro() >= startAfter1.UnixMicro())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= startAfter1.UnixMicro())
}

func TestSingleSubprocess_MultipleJobs_SingleExecutor(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	process := &singleIncProcess{processId: "increment"}
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	// Increase executorA's submitted jobs and frequency of sweeping.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100

	executorA.Start()
	defer executorA.Stop()

	// Register 1000 jobs to increment.
	jobsCount := 1000
	var expectedSum int64 = 0
	var expectedCount = int64(jobsCount)
	insertedJobs := make([]*singleIncJob, 0, jobsCount)
	regTime := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		expectedSum += inc
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobs = append(insertedJobs, jobData)
	}

	err := test.Await(2*time.Minute, func() bool {
		return process.incrementCount.Load() >= expectedCount
	})
	assert.Nil(t, err)

	assert.Equal(t, expectedCount, process.incrementCount.Load())
	assert.Equal(t, expectedSum, process.incrementSum.Load())

	// All jobs are updated correctly.
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 1, found.NextSubprocess)
		assert.Equal(t, 1, found.ExecCount)

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

		foundJobData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}
}

func TestSingleSubprocess_ExecutorIgnoreOtherProcessJobs(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Create main process.
	processA := &singleIncProcess{processId: "processA"}
	var procA Process[*singleIncJob] = processA
	clientA := NewClient(s.Storage, procA)

	// Create another process.
	processB := &singleIncProcess{processId: "processB"}
	var procB Process[*singleIncJob] = processB
	clientB := NewClient(s.Storage, procB)

	// Increase executorA's submitted jobs and frequency of sweeping.
	executorA := newExecutor(t, s, procA, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.Start()
	defer executorA.Stop()

	// Insert 1000 jobs on proc B first.
	jobsCount := 1000
	var insertedJobsB []*singleIncJob
	regTimeB := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := clientB.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobsB = append(insertedJobsB, jobData)
	}

	// Insert 1000 jobs on proc A next.
	var expectedSum int64 = 0
	var expectedCount = int64(jobsCount)
	insertedJobsA := make([]*singleIncJob, 0, jobsCount)
	regTimeA := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		expectedSum += inc
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := clientA.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobsA = append(insertedJobsA, jobData)
	}

	err := test.Await(2*time.Minute, func() bool {
		return processA.incrementCount.Load() >= expectedCount
	})
	assert.Nil(t, err)

	// Assert process A has been incremented.
	assert.Equal(t, expectedCount, processA.incrementCount.Load())
	assert.Equal(t, expectedSum, processA.incrementSum.Load())

	// All processA jobs are updated correctly.
	for _, jobData := range insertedJobsA {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 1, found.NextSubprocess)
		assert.Equal(t, 1, found.ExecCount)

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTimeA.UnixMicro())

		foundJobData, err := processA.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}

	// Assert that process B is not updated at tall.
	for _, jobData := range insertedJobsB {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSReady, found.Status)
		assert.Empty(t, found.GoroutineId)
		assert.Equal(t, 0, found.NextSubprocess)
		assert.Equal(t, 0, found.ExecCount)

		assert.Equal(t, true, found.GoroutineHeartBeatTs.IsZero())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.StartedTs.IsZero())
		assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.EndTs.IsZero())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTimeB.UnixMicro())

		foundJobData, err := processA.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}
}

func TestSingleSubprocess_ExecutorOnlyWorkOnSameProcessId(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Create main process.
	processA := &singleIncProcess{processId: "processA"}
	var procA Process[*singleIncJob] = processA
	clientA := NewClient(s.Storage, procA)

	// Create another process.
	processB := &singleIncProcess{processId: "processB"}
	var procB Process[*singleIncJob] = processB
	clientB := NewClient(s.Storage, procB)

	// Increase executorA's submitted jobs and frequency of sweeping.
	executorA := newExecutor(t, s, procA, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.Start()
	defer executorA.Stop()

	// Increase executorB's submitted jobs and frequency of sweeping.
	executorB := newExecutor(t, s, procB, "ExecutorB")
	executorB.config.SweepInterval = 200 * time.Millisecond
	executorB.config.MaxJobsPerSweep = 100
	executorB.Start()
	defer executorB.Stop()

	// Insert 1000 jobs on proc B first.
	jobsCount := 1000
	var expectedSumB int64 = 0
	var expectedCountB = int64(jobsCount)
	var insertedJobsB []*singleIncJob
	regTimeB := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		expectedSumB += inc
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := clientB.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobsB = append(insertedJobsB, jobData)
	}

	// Insert 1000 jobs on proc A next.
	var expectedSumA int64 = 0
	var expectedCountA = int64(jobsCount)
	insertedJobsA := make([]*singleIncJob, 0, jobsCount)
	regTimeA := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		expectedSumA += inc
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := clientA.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobsA = append(insertedJobsA, jobData)
	}

	err := test.Await(2*time.Minute, func() bool {
		return processA.incrementCount.Load() >= expectedCountA &&
			processB.incrementCount.Load() >= expectedCountB
	})
	assert.Nil(t, err)

	// Assert process A has been incremented.
	assert.Equal(t, expectedCountA, processA.incrementCount.Load())
	assert.Equal(t, expectedSumA, processA.incrementSum.Load())

	// Assert process B has been incremented.
	assert.Equal(t, expectedCountB, processB.incrementCount.Load())
	assert.Equal(t, expectedSumB, processB.incrementSum.Load())

	// All processA jobs are updated correctly.
	for _, jobData := range insertedJobsA {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 1, found.NextSubprocess)
		assert.Equal(t, 1, found.ExecCount)

		// Executed by ExecutorA.
		assert.Equal(t, true, strings.HasPrefix(found.GoroutineId, "ExecutorA"))

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTimeA.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTimeA.UnixMicro())

		foundJobData, err := processA.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}

	// Assert that process B are updated correctly.
	for _, jobData := range insertedJobsB {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 1, found.NextSubprocess)
		assert.Equal(t, 1, found.ExecCount)

		// Executed by ExecutorB.
		assert.Equal(t, true, strings.HasPrefix(found.GoroutineId, "ExecutorB"))

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.StartAfterTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTimeB.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTimeB.UnixMicro())

		foundJobData, err := processA.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}
}

func TestSingleSubprocess_MultipleJobs_MultipleExecutors(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	process := &singleIncProcess{processId: "increment"}
	process.shouldWait = true
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	//s.Storage._sendTestLog(10000000)

	// Use multiple executors. When one job is locked and heartbeat is still there, it's not taken over by the other
	// executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 1 * time.Second
	executorA.config.SweepIntervalJitter = 1 * time.Second
	executorA.config.HeartbeatInterval = 1 * time.Second
	executorA.config.LeaseExpireDuration = 6 * time.Second
	executorA.config.ExecutionTimeout = 10 * time.Second
	executorA.config.MaxJobsPerSweep = 100
	executorA.Start()
	defer executorA.Stop()

	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 1 * time.Second
	executorB.config.SweepIntervalJitter = 1 * time.Second
	executorB.config.HeartbeatInterval = 1 * time.Second
	executorB.config.LeaseExpireDuration = 6 * time.Second
	executorB.config.ExecutionTimeout = 10 * time.Second
	executorB.config.MaxJobsPerSweep = 100
	executorB.Start()
	defer executorB.Stop()

	// Register 1000 jobs to increment.
	jobsCount := 1000
	var expectedSum int64 = 0
	var expectedCount = int64(jobsCount)
	insertedJobs := make([]*singleIncJob, 0, jobsCount)
	regTime := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		expectedSum += inc
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobs = append(insertedJobs, jobData)
	}

	err := test.Await(5*time.Minute, func() bool {
		count := s.h.CountNotDoneJobs(t, proc.Id())
		return count == 0
	})
	assert.Nil(t, err)

	// Jobs might be executed twice, given the wait time, some goroutines might fail to update their heartbeat or have
	// their context cancelled even though the operation successfully completed.
	//
	// What is guaranteed, is that each Job is executed at least once. Locking and idempotence is the responsibility of
	// the Subprocess code.
	assert.Equal(t, expectedCount, process.incrementCount.Load())
	assert.Equal(t, expectedSum, process.incrementSum.Load())

	// All jobs are updated correctly.
	problematicJobIds := make([]string, 0)
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 1, found.NextSubprocess)
		assert.Equal(t, 1, found.ExecCount)

		if found.ExecCount > 1 {
			problematicJobIds = append(problematicJobIds, jobData.JobId)
		}

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

		foundJobData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}

	fmt.Println(">>> ", problematicJobIds)
	//logs := s.Storage._getTestLogs()
	//for _, jobId := range problematicJobIds {
	//	logs.printJobs(jobId)
	//}
}

func TestSingleSubprocess_RetriesUntilMaxCount(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	process := &singleIncProcess{processId: "increment"}
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	// 30% chance of failure.
	// When MaxExecCount is 1000, there should be at least 1 time it's successful.
	process.failureChancePct = 30
	process.randomFailures = true

	// Increase executorA's submitted jobs and frequency of sweeping.
	// Also decrease LeaseExpiryTime so error-ed jobs gets retried sooner.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 1000
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond

	executorA.Start()
	defer executorA.Stop()

	// Register 1000 jobs to increment.
	jobsCount := 1000
	var expectedSum int64 = 0
	var expectedCount = int64(jobsCount)
	insertedJobs := make([]*singleIncJob, 0, jobsCount)
	regTime := time.Now()
	for i := 0; i < jobsCount; i++ {
		inc := int64(rand.Intn(1000))
		expectedSum += inc
		jobData := &singleIncJob{
			JobId:     test.UUIDString(),
			Increment: inc,
		}

		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobs = append(insertedJobs, jobData)
	}

	err := test.Await(2*time.Minute, func() bool {
		return process.incrementCount.Load() >= expectedCount
	})
	assert.Nil(t, err)

	assert.Equal(t, expectedCount, process.incrementCount.Load())
	assert.Equal(t, expectedSum, process.incrementSum.Load())

	// All jobs are updated correctly.
	execCountMoreThan1 := 0
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 1, found.NextSubprocess)
		assert.True(t, found.ExecCount >= 1)
		if found.ExecCount > 1 {
			execCountMoreThan1++
		}

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

		foundJobData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.Equal(t, jobData, foundJobData)
	}
	// 30% failure, there should be at least 50 failed executions.
	assert.Equal(t, true, execCountMoreThan1 >= 50)
}

func TestSingleSubprocess_UpdatesHeartbeatAndTimesOut(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	process := &singleIncProcess{processId: "increment"}
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	done := make(chan struct{})
	process.ExecuteStub = func(
		ctx context.Context,
		goroutineId string,
		job *singleIncJob,
		update JobDataUpdater[*singleIncJob],
	) SubprocessResult {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()

		// Wait until test is done or timer fires.
		select {
		case <-done:
			// No-op.
		case <-timer.C:
		}

		// Do nothing, then return error.
		return SRFailed
	}

	// Set execution timeout to 5 seconds.
	// We'll execute this with two executors, so it also makes sure that the heartbeat process works.
	leaseExpireDuration := 3 * time.Second
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 2 * time.Second
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = leaseExpireDuration
	executorA.config.ExecutionTimeout = 5 * time.Second
	executorA.config.MaxExecutionCount = 2
	executorA.Start()
	defer executorA.Stop()

	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 2 * time.Second
	executorB.config.HeartbeatInterval = 100 * time.Millisecond
	executorB.config.LeaseExpireDuration = leaseExpireDuration
	executorB.config.ExecutionTimeout = 5 * time.Second
	executorB.config.MaxExecutionCount = 2
	executorB.Start()
	defer executorB.Stop()

	jobData := &singleIncJob{
		JobId:     "1",
		Increment: 20,
	}
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	// Wait until the job is taken over.
	err = test.Await(4*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return found.GoroutineId != ""
	})
	assert.Nil(t, err)

	found, _ := s.h.GetJob(t, jobData.JobId)
	goroutineId := found.GoroutineId

	// What we want to see is that the heartbeat actually gets updated per 100 milliseconds.
	// For 5 seconds of expiration time, with heartbeat updated every 100 milliseconds, there should be around
	// ~50 increments, give or take (depends on the test machine's specification).
	//
	// To reduce intermittent test errors, let's just test for 25 increments.
	expectedTime := time.Now()
	for i := 0; i < 25; i++ {
		expectedTime = expectedTime.Add(100 * time.Millisecond)
		err = test.Await(1*time.Second, func() bool {
			found, _ := s.h.GetJob(t, jobData.JobId)
			assert.Equal(t, goroutineId, found.GoroutineId)

			isIncremented := found.GoroutineHeartBeatTs.UnixMicro() >= expectedTime.UnixMicro()
			if isIncremented {
				expectedTime = found.GoroutineHeartBeatTs
			}

			// No matter if it's incremented or not, LeaseExpireTs must be the same as HeartBeat + ExpireDuration.
			assert.Equal(t, found.GoroutineLeaseExpireTs, found.GoroutineHeartBeatTs.Add(leaseExpireDuration))

			return isIncremented
		})
		assert.Nil(t, err)
	}

	// The execution will time out. We expect it to be taken by a new goroutine.
	err = test.Await(7*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return found.GoroutineId != goroutineId
	})
	assert.Nil(t, err)

	// Fetch new goroutine ID and continue asserting heartbeat is updated.
	found, _ = s.h.GetJob(t, jobData.JobId)
	goroutineId = found.GoroutineId

	expectedTime = time.Now()
	for i := 0; i < 25; i++ {
		expectedTime = expectedTime.Add(100 * time.Millisecond)
		err = test.Await(5*time.Second, func() bool {
			found, _ := s.h.GetJob(t, jobData.JobId)
			assert.Equal(t, goroutineId, found.GoroutineId)

			isIncremented := found.GoroutineHeartBeatTs.UnixMicro() >= expectedTime.UnixMicro()
			if isIncremented {
				expectedTime = found.GoroutineHeartBeatTs
			}

			// No matter if it's incremented or not, LeaseExpireTs must be the same as HeartBeat + ExpireDuration.
			assert.Equal(t, found.GoroutineLeaseExpireTs, found.GoroutineHeartBeatTs.Add(leaseExpireDuration))

			return isIncremented
		})
		assert.Nil(t, err)
	}

	// The execution will time out. It will be marked as error.
	err = test.Await(10*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return found.Status == JSError
	})
	assert.Nil(t, err)

	// Assert job is as expected.
	found, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, jobData.JobId, found.JobId)
	assert.Equal(t, JSError, found.Status)
	assert.Equal(t, 0, found.NextSubprocess)
	assert.Equal(t, 2, found.ExecCount)

	// It will be another job that marks it as error.
	assert.NotEqual(t, goroutineId, found.GoroutineId)

	assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= expectedTime.UnixMicro())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.EndTs.UnixMicro() >= expectedTime.UnixMicro())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= expectedTime.UnixMicro())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)

	// Close all waiting goroutines.
	close(done)
}

func TestSingleSubprocess_MultipleExecutor_TakeOver(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// This process is the correct process.
	process := &singleIncProcess{processId: "increment"}
	var proc Process[*singleIncJob] = process
	client := NewClient(s.Storage, proc)

	// This second process will stall, and then will not return at all, forcing execution timeout error.
	processStall := &singleIncProcess{processId: "increment"}
	var procStall Process[*singleIncJob] = processStall
	stallDone := make(chan struct{}, 1)
	processStall.ExecuteStub = func(
		ctx context.Context,
		goroutineId string,
		job *singleIncJob,
		update JobDataUpdater[*singleIncJob],
	) SubprocessResult {
		// Wait until context is cancelled.
		select {
		case <-ctx.Done():
			// Send message that context is cancelled.
			stallDone <- struct{}{}
		}

		// Do nothing, then return error.
		return SRFailed
	}

	// ExecutorA is using the stalled process.
	executorA := newExecutor(t, s, procStall, "ExecutorA")
	executorA.config.SweepInterval = 100 * time.Minute
	executorA.config.LeaseExpireDuration = 5 * time.Second
	executorA.config.ExecutionTimeout = 4 * time.Second
	executorA.Start()
	defer executorA.Stop()

	// Insert the job.
	jobData := &singleIncJob{
		JobId:     "1",
		Increment: 20,
	}
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	// Manually trigger ExecutorA to sweep.
	// Then wait until job is worked on by ExecutorA.
	executorA.sweepJobs()
	startTime := time.Now()
	err = test.Await(4*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return found.GoroutineId != ""
	})
	assert.Nil(t, err)

	found, _ := s.h.GetJob(t, jobData.JobId)
	goroutineId := found.GoroutineId
	assert.Equal(t, true, strings.HasPrefix(goroutineId, "ExecutorA"))

	// Run ExecutorB using the correct process. Sweeping every 1s.
	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 500 * time.Millisecond
	executorB.config.LeaseExpireDuration = 5 * time.Second
	executorB.config.ExecutionTimeout = 4 * time.Second
	executorB.Start()
	defer executorB.Stop()

	// Wait until the job is picked up by executorB.
	err = test.Await(12*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return found.GoroutineId != goroutineId
	})
	assert.Nil(t, err)

	// The execution times out at 4 seconds.
	// Because the heartbeat of ExecutorA is sent every 100ms, at the time of the timeout it is still fresh.
	// The lease expiry duration is 5 seconds, time elapsed since ExecutorA swept the jobs should be 9 seconds.
	// This assertion proves that ExecutorB respects the heartbeat ping. We assert 8.5sec to avoid intermittent issues
	// due to goroutine execution jitter in package test runs.
	timeUntilExecutorBTakesOver := time.Since(startTime)
	assert.Equal(t, true, timeUntilExecutorBTakesOver >= 8*time.Second+500*time.Millisecond)

	// Assert that the context is cancelled.
	<-stallDone

	// Assert that it's taken over by ExecutorA, because ExecutorA's execution has timed out.
	found, _ = s.h.GetJob(t, jobData.JobId)
	assert.True(t, strings.HasPrefix(found.GoroutineId, "ExecutorB"))
	goroutineId = found.GoroutineId

	// Wait until the job is done.
	err = test.Await(2*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return found.Status == JSDone
	})
	assert.Nil(t, err)

	// Assert job is done.
	assert.Equal(t, int64(1), process.incrementCount.Load())
	assert.Equal(t, int64(20), process.incrementSum.Load())

	// Assert job is as expected.
	found, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, jobData.JobId, found.JobId)
	assert.Equal(t, JSDone, found.Status)
	assert.Equal(t, goroutineId, found.GoroutineId)
	assert.Equal(t, 1, found.NextSubprocess)
	assert.Equal(t, 2, found.ExecCount)

	assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
	assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

	foundJobData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, jobData, foundJobData)
}

func TestSingleSubprocess_RegisterExecute(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Delay process simulates long-running process.
	// We'll register and execute immediately, make sure it's blocking until done.
	type delayJob struct {
		*TestJobData
		DelayDuration time.Duration
	}
	process := newTestProcess("delay", func() *delayJob {
		return &delayJob{}
	})
	counter := &test.Counter{}
	process.subprocesses = []*Subprocess[*delayJob]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *delayJob,
				update JobDataUpdater[*delayJob],
			) SubprocessResult {
				counter.Add("start", job.JobId, goroutineId)
				time.Sleep(job.DelayDuration)
				counter.Add("executed", job.JobId)
				return SRSuccess
			},
		},
	}
	var proc Process[*delayJob] = process
	client := NewClient(s.Storage, proc)

	// Create and run executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.Start()
	defer executorA.Stop()

	// Register job and wait until job is executed, assert that the executor is actually running tasks.
	bgJob := &delayJob{
		TestJobData:   &TestJobData{JobId: "background-job"},
		DelayDuration: 0,
	}
	regTime := time.Now()
	err := client.Register(s.h.Ctx, bgJob)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 3*time.Second, bgJob.JobId, JSDone)

	// Assert job is done.
	assertJobDone[*delayJob](t, s.h, process, bgJob, 1, 1, regTime)
	foundJob, _ := s.h.GetJob(t, bgJob.JobId)
	assert.Equal(t, 2, counter.TotalEvents())
	assert.Equal(t, 1, counter.Get("start", bgJob.JobId, foundJob.GoroutineId))
	assert.Equal(t, 1, counter.Get("executed", bgJob.JobId))
	counter.Reset()

	// Register another job, now with 5+ seconds delay.
	// We'll assert that it blocks for 5+ seconds by asserting time elapsed.
	// We'll assert that it gets executed immediately by checking that "start" for this job ID is executed immediately.
	directJob := &delayJob{
		TestJobData:   &TestJobData{JobId: "direct-job"},
		DelayDuration: 5*time.Second + time.Duration(rand.Intn(int(2*time.Second))),
	}

	startTime := time.Now()
	traceId := test.UUIDString()
	latest, err := executorA.RegisterExecuteWait(s.h.Ctx, &tr.Trace{TraceId: traceId}, directJob)
	assert.Nil(t, err)
	assertJobDataIsLatest(t, s.h, proc, latest)

	// Blocks for at least the delay duration.
	assert.GreaterOrEqual(t, time.Since(startTime), directJob.DelayDuration)

	// Assert job is done.
	assertJobDone[*delayJob](t, s.h, process, directJob, 1, 1, startTime)
	foundJob, _ = s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, 2, counter.TotalEvents())
	assert.Equal(t, 1, counter.Get("start", directJob.JobId, foundJob.GoroutineId))
	assert.Equal(t, 1, counter.Get("executed", directJob.JobId))

	// Assert that RegisterExecute uses the given Trace ID.
	found, _ := s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, []string{"ExecutorA-" + traceId}, found.GoroutineIds)
}

func TestSingleSubprocess_RegisterExecutePanicPickedUp(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Delay process simulates long-running process.
	// We'll register and execute immediately, make sure it's blocking until done.
	runCount := atomic.Int64{}
	type failurePickUpJob struct {
		*TestJobData
	}
	process := newTestProcess("delay", func() *failurePickUpJob {
		return &failurePickUpJob{}
	})
	counter := &test.Counter{}
	process.subprocesses = []*Subprocess[*failurePickUpJob]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *failurePickUpJob,
				update JobDataUpdater[*failurePickUpJob],
			) SubprocessResult {
				counter.Add("start", job.JobId, goroutineId)
				newRunCount := runCount.Add(1)
				if newRunCount == 1 {
					// Panic for first run.
					panic("this is an example panic failure")
				}
				if newRunCount == 2 {
					// Return error for second run.
					return SRFailed
				}

				// Otherwise return successful
				return SRSuccess
			},
		},
	}
	var proc Process[*failurePickUpJob] = process

	// Create and run executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 3 * time.Second // Give time for assertions.
	executorA.Start()
	defer executorA.Stop()

	// Register the job. First, failure is due to panic.
	directJob := &failurePickUpJob{
		TestJobData: &TestJobData{JobId: "direct-job"},
	}
	startTime := time.Now()
	traceId := test.UUIDString()
	latest, err := executorA.RegisterExecuteWait(s.h.Ctx, &tr.Trace{TraceId: traceId}, directJob)
	assert.NotNil(t, err)
	assertJobDataIsLatest(t, s.h, proc, latest)
	assert.Equal(
		t, "subprocess 0 failed: ExecutorA-delay: panic on execution: this is an example panic failure",
		err.Error(),
	)

	// Assert job ends up in failure.
	assert.Equal(t, int64(1), runCount.Load())
	assertJobFailExec(t, s.h, proc, directJob, 1, 0, startTime)

	// Assert the trace ID is inserted into goroutine ID.
	found, _ := s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, "ExecutorA-"+traceId, found.GoroutineId)
	assert.Equal(t, "ExecutorA-"+traceId, found.GoroutineIds[0])
	assert.Equal(t, 1, counter.Get("start", found.JobId, found.GoroutineId))
	assert.Equal(t, 1, counter.TotalEvents())
	goroutine1 := found.GoroutineId
	counter.Reset()

	// Wait for the executor to pick-it-up.
	// Max wait time is LeaseExpireDuration + SweepInterval + SweepIntervalJitter.
	err = test.Await(10*time.Second, func() bool {
		return runCount.Load() == int64(2)
	})
	assert.Nil(t, err)

	// Second failure, now due to returning error.
	assertJobFailExec(t, s.h, proc, directJob, 2, 0, startTime)

	// Assert goroutine ID inserted.
	found, _ = s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, 2, len(found.GoroutineIds))
	assert.Equal(t, goroutine1, found.GoroutineIds[0])
	assert.Equal(t, found.GoroutineId, found.GoroutineIds[1])
	assert.Equal(t, 1, counter.Get("start", found.JobId, found.GoroutineId))
	assert.Equal(t, 1, counter.TotalEvents())
	goroutine2 := found.GoroutineId
	counter.Reset()

	// The next time it's picked-up by executor, it should be completed.
	// Max wait time is LeaseExpireDuration + SweepInterval + SweepIntervalJitter.
	s.h.WaitJobStatus(t, 10*time.Second, directJob.JobId, JSDone)
	assertJobDone[*failurePickUpJob](t, s.h, process, directJob, 3, 1, startTime)

	// Assert new goroutine IDs were inserted.
	found, _ = s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, 3, len(found.GoroutineIds))
	assert.Equal(t, goroutine1, found.GoroutineIds[0])
	assert.Equal(t, goroutine2, found.GoroutineIds[1])
	assert.Equal(t, found.GoroutineId, found.GoroutineIds[2])
	assert.Equal(t, 1, counter.Get("start", found.JobId, found.GoroutineId))
	assert.Equal(t, 1, counter.TotalEvents())

	// Assert exec count, etc. is as expected.
	assert.Equal(t, 3, found.ExecCount)
}

func TestSingleSubprocess_RegisterExecuteErrorReturned(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Delay process simulates long-running process.
	// We'll register and execute immediately, make sure it's blocking until done.
	runCount := atomic.Int64{}
	type failurePickUpJob struct {
		*TestJobData
	}
	process := newTestProcess("delay", func() *failurePickUpJob {
		return &failurePickUpJob{}
	})
	counter := &test.Counter{}
	process.subprocesses = []*Subprocess[*failurePickUpJob]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *failurePickUpJob,
				update JobDataUpdater[*failurePickUpJob],
			) SubprocessResult {
				counter.Add("start", job.JobId, goroutineId)
				newRunCount := runCount.Add(1)
				if newRunCount == 1 {
					return SRFailed
				}

				// Otherwise return successful
				return SRSuccess
			},
		},
	}
	var proc Process[*failurePickUpJob] = process

	// Create and run executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 3 * time.Second // Give time for assertions.
	executorA.Start()
	defer executorA.Stop()

	// Register the job. First, failure is due to panic.
	directJob := &failurePickUpJob{
		TestJobData: &TestJobData{JobId: "direct-job"},
	}
	startTime := time.Now()
	traceId := test.UUIDString()
	latest, err := executorA.RegisterExecuteWait(s.h.Ctx, &tr.Trace{TraceId: traceId}, directJob)
	assert.NotNil(t, err)
	assertJobDataIsLatest(t, s.h, proc, latest)
	assert.Equal(
		t, "subprocess 0 failed!",
		err.Error(),
	)

	// Assert job ends up in failure.
	assert.Equal(t, int64(1), runCount.Load())
	assertJobFailExec(t, s.h, proc, directJob, 1, 0, startTime)

	// Assert the trace ID is inserted into goroutine ID.
	found, _ := s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, "ExecutorA-"+traceId, found.GoroutineId)
	assert.Equal(t, "ExecutorA-"+traceId, found.GoroutineIds[0])
	assert.Equal(t, 1, counter.Get("start", found.JobId, found.GoroutineId))
	assert.Equal(t, 1, counter.TotalEvents())
	goroutine1 := found.GoroutineId
	counter.Reset()

	// The next time it's picked-up by executor, it should be completed.
	// Max wait time is LeaseExpireDuration + SweepInterval + SweepIntervalJitter.
	s.h.WaitJobStatus(t, 10*time.Second, directJob.JobId, JSDone)
	assertJobDone[*failurePickUpJob](t, s.h, process, directJob, 2, 1, startTime)

	// Assert new goroutine IDs were inserted.
	found, _ = s.h.GetJob(t, directJob.JobId)
	assert.Equal(t, 2, len(found.GoroutineIds))
	assert.Equal(t, goroutine1, found.GoroutineIds[0])
	assert.Equal(t, found.GoroutineId, found.GoroutineIds[1])
	assert.Equal(t, 1, counter.Get("start", found.JobId, found.GoroutineId))
	assert.Equal(t, 1, counter.TotalEvents())

	// Assert exec count, etc. is as expected.
	assert.Equal(t, 2, found.ExecCount)
}
