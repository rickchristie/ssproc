package ssproc

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"rukita.co/main/be/accessor/db/pg/pgtest"
	"rukita.co/main/be/lib/test"
	"rukita.co/main/be/lib/tr"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecute_PanicOnExecute(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	storage := StorageMockWrapper{Wrapped: s.Storage}
	storage.UpdateJobMock = func(ctx context.Context, job *Job, leaseExpireDuration time.Duration) error {
		panic("example panic on execute")
	}

	proc := newMultiProcess()
	process := proc.(*multiProcess)

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.storage = &storage
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = 2 * time.Second
	executorA.config.MaxExecutionCount = 1
	executorA.Start()
	defer executorA.Stop()

	// Try register execute, error is returned instead of panicking.
	jobData := process.newJobData()
	trace := tr.Trace{TraceId: test.UUIDString()}
	latest, err := executorA.RegisterExecuteWait(s.h.Ctx, &trace, jobData)
	assert.NotNil(t, err)
	assertJobDataIsLatest(t, s.h, proc, latest)
	assert.Equal(
		t,
		true,
		strings.HasPrefix(err.Error(), "ExecutorA-multi: panic on ssproc execute lib: example panic on execute"),
	)

	// Assert heartbeat stops (i.e. it doesn't change even when we wait 1 second, heartbeat is 100ms).
	job, _ := s.h.GetJob(t, jobData.JobId)
	err = test.Await(1*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		return job.GoroutineHeartBeatTs.UnixMicro() != found.GoroutineHeartBeatTs.UnixMicro()
	})
	assert.NotNil(t, err)

	// Job is taken over, but it's not updated to error.
	err = test.Await(4*time.Second, func() bool {
		foundJob, _ := s.h.GetJob(t, jobData.JobId)
		return foundJob.GoroutineId != job.GoroutineId
	})
	assert.Nil(t, err)

	// Job will not be marked as done or error.
	err = test.Await(2*time.Second, func() bool {
		foundJob, _ := s.h.GetJob(t, jobData.JobId)
		return foundJob.Status != JSReady
	})
	assert.NotNil(t, err)

	// Exec count not incremented because we made UpdateJob panic.
	job, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, JSReady, job.Status)
	assert.Equal(t, 0, job.ExecCount)
}

func TestExecute_HeartbeatError(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Use mock storage wrapper, so we can modify heartbeat behavior. We want it to start failing when we change
	// the boolean.
	shouldFailAtomic := atomic.Bool{}
	failCount := atomic.Int64{}
	storage := StorageMockWrapper{Wrapped: s.Storage}
	storage.SendHeartbeatMock = func(
		ctx context.Context,
		goroutineId string,
		jobId string,
		leaseExpireDuration time.Duration,
	) error {
		if shouldFailAtomic.Load() {
			// Fail three times, then flip it back to false.
			count := failCount.Add(1)
			if count >= 3 {
				shouldFailAtomic.Store(false)
			}
			return errors.New("send heartbeat mock failure")
		}
		return s.Storage.SendHeartbeat(ctx, goroutineId, jobId, leaseExpireDuration)
	}

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	cStop := make(chan struct{})
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		// Start sleeping at index 3, simulate processing.
		if subprocessIndex == 2 {
			timer := time.NewTimer(8 * time.Second)
			defer timer.Stop()
			select {
			case <-cStop:
				// No-op.
			case <-timer.C:
				// No-op.
			}
			return SRFailed
		}
		return SRSuccess
	}
	client := NewClient(&storage, proc)

	// Start executor.
	leaseExpireDuration := 1 * time.Second
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.storage = &storage
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = leaseExpireDuration
	executorA.config.ExecutionTimeout = 20 * time.Second
	executorA.config.MaxExecutionCount = 1
	executorA.Start()
	defer executorA.Stop()

	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	// Wait until job is taken over.
	err = test.Await(5*time.Second, func() bool {
		job, _ := s.h.GetJob(t, jobData.JobId)
		return job.GoroutineId != ""
	})
	assert.Nil(t, err)

	job, _ := s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, true, strings.HasPrefix(job.GoroutineId, "ExecutorA"))
	goroutineId := job.GoroutineId

	// Heartbeat gets updated per 100 milliseconds. We verify that heartbeat gets updated.
	// Check for 10 increments.
	expectedTime := time.Now()
	for i := 0; i < 10; i++ {
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

	// Make heartbeat stop. This should cause heartbeat to fail three times, which will then cancel the context.
	shouldFailAtomic.Store(true)

	// We expect context to fail, so the job would fail.
	s.h.WaitJobStatus(t, 5*time.Second, jobData.JobId, JSError)
	assertJobError[*multiJobData](t, s.h, process, jobData, 1, 2, regTime)

	// Stop subprocess goroutine so we can check for goroutine leaks.
	close(cStop)
}

func TestExecute_HeartbeatPanic(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Use mock storage wrapper, so we can modify heartbeat behavior. We want it to start failing when we change
	// the boolean.
	shouldPanicAtomic := atomic.Bool{}
	failCount := atomic.Int64{}
	storage := StorageMockWrapper{Wrapped: s.Storage}
	storage.SendHeartbeatMock = func(
		ctx context.Context,
		goroutineId string,
		jobId string,
		leaseExpireDuration time.Duration,
	) error {
		if shouldPanicAtomic.Load() {
			// Fail three times, then flip it back to false.
			count := failCount.Add(1)
			if count >= 3 {
				shouldPanicAtomic.Store(false)
			}
			panic("example heartbeat panic")
		}
		return s.Storage.SendHeartbeat(ctx, goroutineId, jobId, leaseExpireDuration)
	}

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	cStop := make(chan struct{})
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		// Start sleeping at index 3, simulate processing.
		if subprocessIndex == 2 {
			timer := time.NewTimer(8 * time.Second)
			defer timer.Stop()
			select {
			case <-cStop:
				// No-op.
			case <-timer.C:
				// No-op.
			}
			return SRFailed
		}
		return SRSuccess
	}
	client := NewClient(&storage, proc)

	// Start executor.
	leaseExpireDuration := 1 * time.Second
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.storage = &storage
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = leaseExpireDuration
	executorA.config.ExecutionTimeout = 20 * time.Second
	executorA.config.MaxExecutionCount = 1
	executorA.Start()
	defer executorA.Stop()

	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	// Wait until job is taken over.
	err = test.Await(5*time.Second, func() bool {
		job, _ := s.h.GetJob(t, jobData.JobId)
		return job.GoroutineId != ""
	})
	assert.Nil(t, err)

	job, _ := s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, true, strings.HasPrefix(job.GoroutineId, "ExecutorA"))
	goroutineId := job.GoroutineId

	// Heartbeat gets updated per 100 milliseconds. We verify that heartbeat gets updated.
	// Check for 10 increments.
	expectedTime := time.Now()
	for i := 0; i < 10; i++ {
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

	// Make heartbeat stop. This should cause heartbeat to fail three times, which will then cancel the context.
	shouldPanicAtomic.Store(true)

	// We expect context to fail, so the job would fail.
	s.h.WaitJobStatus(t, 5*time.Second, jobData.JobId, JSError)
	assertJobError[*multiJobData](t, s.h, process, jobData, 1, 2, regTime)

	// Stop subprocess goroutine so we can check for goroutine leaks.
	close(cStop)
}

func TestExecute_HeartbeatFailureContinued(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	// Use mock storage wrapper, so we can modify heartbeat behavior. We want it to start failing when we change
	// the boolean.
	shouldFailAtomic := atomic.Bool{}
	failCount := atomic.Int64{}
	storage := StorageMockWrapper{Wrapped: s.Storage}
	storage.SendHeartbeatMock = func(
		ctx context.Context,
		goroutineId string,
		jobId string,
		leaseExpireDuration time.Duration,
	) error {
		if shouldFailAtomic.Load() {
			// Fail three times, then flip it back to false.
			count := failCount.Add(1)
			if count >= 3 {
				shouldFailAtomic.Store(false)
			}
			return errors.New("send heartbeat mock failure")
		}
		return s.Storage.SendHeartbeat(ctx, goroutineId, jobId, leaseExpireDuration)
	}

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	cStop := make(chan struct{})
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		updater JobDataUpdater[*multiJobData],
	) SubprocessResult {
		// Start sleeping at index 3, simulate processing.
		if subprocessIndex == 2 {
			timer := time.NewTimer(8 * time.Second)
			defer timer.Stop()
			select {
			case <-cStop:
				// No-op.
			case <-timer.C:
				// No-op.
			}
		}
		return SRSuccess
	}
	client := NewClient(&storage, proc)

	// Start executor.
	leaseExpireDuration := 1 * time.Second
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.storage = &storage
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = leaseExpireDuration
	executorA.config.ExecutionTimeout = 20 * time.Second
	executorA.config.MaxExecutionCount = 3
	executorA.Start()
	defer executorA.Stop()

	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	// Wait until job is taken over.
	err = test.Await(5*time.Second, func() bool {
		job, _ := s.h.GetJob(t, jobData.JobId)
		return job.GoroutineId != ""
	})
	assert.Nil(t, err)

	job, _ := s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, true, strings.HasPrefix(job.GoroutineId, "ExecutorA"))
	goroutineId := job.GoroutineId

	// Heartbeat gets updated per 100 milliseconds. We verify that heartbeat gets updated.
	// Check for 10 increments.
	expectedTime := time.Now()
	for i := 0; i < 10; i++ {
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

	// Make heartbeat stop. This should cause heartbeat to fail three times, which will then cancel the context.
	shouldFailAtomic.Store(true)

	// Because the context fails, heartbeat stops and lease will expire, so we wait for the job to be taken over
	// by another goroutine.
	s.h.WaitJobGoroutineIdChanged(t, 3*time.Second, []string{goroutineId}, jobData.JobId)

	job, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, true, strings.HasPrefix(job.GoroutineId, "ExecutorA"))
	goroutineId = job.GoroutineId

	// Allow the subprocess to finish.
	close(cStop)

	// We expect the next execution to succeeds.
	s.h.WaitJobStatus(t, 5*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 2, 3, regTime)
}
