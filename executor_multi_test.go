package ssproc

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"math/rand"
	"rukita.co/main/be/accessor/db/pg/pgtest"
	"rukita.co/main/be/lib/test"
	"rukita.co/main/be/lib/tr"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// todo compensation: add single process compensation test:
//			- Retry until max-exec-count.
//			- Then when failure, run compensation until max-retry-count.
//			- Assert:
//				- Successfully rolled back, status turned to "compensated".
//				- Job turning to error when compensation also error.
// todo compensation: add multi-process transaction error in the middle, then compensation also error, job turned to error.

// todo pr: complete multi subprocess test.

func TestMultiSubprocess_SingleJobError(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient(s.Storage, proc)

	//s.Storage._sendTestLog(100000)

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = 1 * time.Second
	executorA.config.MaxExecutionCount = 3
	executorA.Start()
	defer executorA.Stop()

	t.Run("job fail subprocess 0, error, retried 3 times, then error", func(t *testing.T) {
		process.stub = func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			if subprocessIndex == 0 {
				return SRFailed
			}
			return SRSuccess
		}

		jobData := process.newJobData()
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		// Wait until job fails.
		// Sweep interval is 1 seconds, so should be around 2 x 4 = 4 seconds:
		//		- wait ~1 seconds for next sweep.
		//		- 1st exec error.
		//		- wait 1 second for next sweep.
		//		- 2nd exec error.
		//		- wait 1 second for next sweep.
		//		- 3rd exec error.
		//		- wait 1 second for next sweep.
		//		- 4th exec, max execution count reached. Mark as error.
		s.h.WaitJobStatus(t, 10*time.Second, jobData.JobId, JSError)
		assertJobError[*multiJobData](t, s.h, proc, jobData, 3, 0, regTime)

		process.assertExecCount(t, 0, jobData.JobId, 3)
		process.assertExecCount(t, 1, jobData.JobId, 0)
		process.assertExecCount(t, 2, jobData.JobId, 0)
	})

	//logs := s.Storage._getTestLogs()
	//logs.printAll()

	t.Run("job fail subprocess 1, error, retried 3 times, then error", func(t *testing.T) {
		process.stub = func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			if subprocessIndex == 1 {
				return SRFailed
			}
			return SRSuccess
		}

		jobData := process.newJobData()
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		s.h.WaitJobStatus(t, 10*time.Second, jobData.JobId, JSError)
		assertJobError[*multiJobData](t, s.h, proc, jobData, 3, 1, regTime)

		process.assertExecCount(t, 0, jobData.JobId, 1)
		process.assertExecCount(t, 1, jobData.JobId, 3)
		process.assertExecCount(t, 2, jobData.JobId, 0)
	})

	t.Run("job fail subprocess 2, error, retried 3 times, then error", func(t *testing.T) {
		process.stub = func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			if subprocessIndex == 2 {
				return SRFailed
			}
			return SRSuccess
		}

		jobData := process.newJobData()
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		s.h.WaitJobStatus(t, 10*time.Second, jobData.JobId, JSError)
		assertJobError[*multiJobData](t, s.h, proc, jobData, 3, 2, regTime)

		process.assertExecCount(t, 0, jobData.JobId, 1)
		process.assertExecCount(t, 1, jobData.JobId, 1)
		process.assertExecCount(t, 2, jobData.JobId, 3)

		// Assert time is correct.
		assert.Greater(
			t,
			process.getTime(1, jobData.JobId),
			process.getTime(0, jobData.JobId),
		)
	})
}

func TestMultiSubprocess_SingleJobPanic(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient(s.Storage, proc)

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = 1 * time.Second
	executorA.config.MaxExecutionCount = 3
	executorA.Start()
	defer executorA.Stop()

	t.Run("job fail subprocess 0, error, retried 3 times, then error", func(t *testing.T) {
		process.stub = func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			if subprocessIndex == 0 {
				panic("example panic at index #0")
			}
			return SRSuccess
		}

		jobData := process.newJobData()
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		// Wait until job fails.
		// Sweep interval is 1 seconds, so should be around 2 x 4 = 4 seconds:
		//		- wait ~1 seconds for next sweep.
		//		- 1st exec error.
		//			- wait 1 second.
		//		- 2nd exec error.
		//			- wait 1 second.
		//		- 3rd exec error.
		//			- wait 1 second.
		//		- 4th exec, max execution count reached. Mark as error.
		s.h.WaitJobStatus(t, 10*time.Second, jobData.JobId, JSError)
		assertJobError[*multiJobData](t, s.h, proc, jobData, 3, 0, regTime)

		process.assertExecCount(t, 0, jobData.JobId, 3)
		process.assertExecCount(t, 1, jobData.JobId, 0)
		process.assertExecCount(t, 2, jobData.JobId, 0)
	})

	t.Run("job fail subprocess 1, error, retried 3 times, then error", func(t *testing.T) {
		process.stub = func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			if subprocessIndex == 1 {
				panic("example panic at index #1")
			}
			return SRSuccess
		}

		jobData := process.newJobData()
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		s.h.WaitJobStatus(t, 6*time.Second, jobData.JobId, JSError)
		assertJobError[*multiJobData](t, s.h, proc, jobData, 3, 1, regTime)

		process.assertExecCount(t, 0, jobData.JobId, 1)
		process.assertExecCount(t, 1, jobData.JobId, 3)
		process.assertExecCount(t, 2, jobData.JobId, 0)
	})

	t.Run("job fail subprocess 2, error, retried 3 times, then error", func(t *testing.T) {
		process.stub = func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			if subprocessIndex == 2 {
				panic("example panic at index #2")
			}
			return SRSuccess
		}

		jobData := process.newJobData()
		regTime := time.Now()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		s.h.WaitJobStatus(t, 6*time.Second, jobData.JobId, JSError)
		assertJobError[*multiJobData](t, s.h, proc, jobData, 3, 2, regTime)

		process.assertExecCount(t, 0, jobData.JobId, 1)
		process.assertExecCount(t, 1, jobData.JobId, 1)
		process.assertExecCount(t, 2, jobData.JobId, 3)

		// Assert time is correct.
		assert.Greater(
			t,
			process.getTime(1, jobData.JobId),
			process.getTime(0, jobData.JobId),
		)
	})
}

func TestMultiSubprocess_MultipleJobsSuccess(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	// Insert jobs first, so we don't have race condition on map read/write.
	jobCount := 1000
	insertedJobData := make([]*multiJobData, jobCount)
	regTime := time.Now()
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)

		insertedJobData[i] = jobData
	}

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Wait until all jobs are done.
	s.h.WaitAllJobsDone(t, 5*time.Minute, proc.Id())

	// With single executor, we expect all jobs are executed only once.
	for _, jobData := range insertedJobData {
		assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, regTime)

		// Each job executed only once.
		process.assertExecCount(t, 0, jobData.JobId, 1)
		process.assertExecCount(t, 1, jobData.JobId, 1)
		process.assertExecCount(t, 2, jobData.JobId, 1)

		// Subprocess 0 executed before 1, executed before 2.
		process.assertValidTime(t, jobData.JobId)
	}
}

func TestMultiSubprocess_SaveData_InTheMiddle(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	errInTheMiddle := atomic.Bool{}
	errInTheMiddle.Store(true)
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		// Change data.
		jobData.SavedDataInt += 1
		jobData.SavedDataString += "a"
		if jobData.SavedDataMap == nil {
			jobData.SavedDataMap = make(map[string]int)
		}
		jobData.SavedDataMap[jobData.SavedDataString] = jobData.SavedDataInt
		err := update(jobData)
		if err != nil {
			return SRFailed
		}

		if errInTheMiddle.Load() {
			return SRFailed
		}

		return SRSuccess
	}

	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 1 * time.Second
	executorA.config.LeaseExpireDuration = 2 * time.Second // give time fo r asserts.
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.MaxExecutionCount = 3
	executorA.Start()
	defer executorA.Stop()

	// Register job.
	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	// Wait until JobData is updated.
	err = test.Await(5*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		foundData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		return foundData.SavedDataInt == 1
	})
	assert.Nil(t, err)

	// Assert data is saved, but execution fails.
	found, _ := s.h.GetJob(t, jobData.JobId)
	foundData, err := process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, 1, foundData.SavedDataInt)
	assert.Equal(t, "a", foundData.SavedDataString)
	assert.Equal(t, map[string]int{"a": 1}, foundData.SavedDataMap)
	assertJobFailExec[*multiJobData](t, s.h, process, foundData, 1, 0, regTime)

	// Wait until job is taken over again, we expect data to be updated once again.
	err = test.Await(5*time.Second, func() bool {
		found, _ := s.h.GetJob(t, jobData.JobId)
		foundData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		return foundData.SavedDataInt == 2
	})
	assert.Nil(t, err)

	// Immediately set next execution to not fail.
	errInTheMiddle.Store(false)

	found, _ = s.h.GetJob(t, jobData.JobId)
	foundData, err = process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, 2, foundData.SavedDataInt)
	assert.Equal(t, "aa", foundData.SavedDataString)
	assert.Equal(t, map[string]int{"a": 1, "aa": 2}, foundData.SavedDataMap)
	assertJobFailExec[*multiJobData](t, s.h, process, foundData, 2, 0, regTime)

	// Wait until job is taken over again, this time it should be set to done.
	s.h.WaitJobStatus(t, 5*time.Second, jobData.JobId, JSDone)

	// Data is still saved. Each subprocess adds more data, 2 (failed) + 3 (success) = 5.
	found, _ = s.h.GetJob(t, jobData.JobId)
	foundData, err = process.Deserialize(found.JobData)
	assert.Nil(t, err)
	assert.Equal(t, 5, foundData.SavedDataInt)
	assert.Equal(t, "aaaaa", foundData.SavedDataString)
	assert.Equal(t, map[string]int{
		"a":     1,
		"aa":    2,
		"aaa":   3,
		"aaaa":  4,
		"aaaaa": 5,
	}, foundData.SavedDataMap)

	assertJobDone[*multiJobData](t, s.h, process, foundData, 3, 3, regTime)
}

func TestMultiSubprocess_SaveData_RetriesUntilMaxCount(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	// Each subprocess have 30% chance of failure.
	sum := atomic.Int64{}
	expectedSum := atomic.Int64{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		roll := rand.Intn(100)
		if roll < 30 {
			return SRFailed
		}

		// In the first subprocess, generate random number and save this random number.
		// This will also test that results in subprocess #0 is saved for execution in next subprocess.
		// If this is not the case, due to 30% failure chance, we'll have intermittent errors when running tests.
		if subprocessIndex == 0 {
			jobData.SavedDataInt = roll
			err := update(jobData)
			if err != nil {
				return SRFailed
			}
			expectedSum.Add(int64(roll))
			return SRSuccess
		}

		// Only add to sum at the third and last job.
		if subprocessIndex == 2 {
			if jobData.SavedDataInt == 0 {
				panic("saved data does not appear!")
			}
			sum.Add(int64(jobData.SavedDataInt))
			return SRSuccess
		}

		return SRSuccess
	}

	// Create executors with max exec count of 5000.
	// This should be enough for 1000 jobs (3 subprocesses).
	// At the end of the day, successful execution count is still 1 per subprocess.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 5000
	executorA.config.LeaseExpireDuration = 500 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*multiJobData, 0)
	regTime := time.Now()
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()

		// Insert jobs.
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Wait until all jobs are done.
	s.h.WaitAllJobsDone(t, 5*time.Minute, proc.Id())

	// The sum is as expected (no double or triple execution).
	assert.Equal(t, expectedSum.Load(), sum.Load())

	// All jobs are done, they are updated correctly.
	execCountMoreThan1 := 0
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 3, found.NextSubprocess)
		assert.True(t, found.ExecCount >= 1)
		if found.ExecCount > 1 {
			execCountMoreThan1++
		}

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

		// JobData would be different, as Subprocess #0 saves additional data.
		foundJobData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.NotEqual(t, jobData, foundJobData)

		// Only SavedInt are updated.
		jobData.SavedDataInt = foundJobData.SavedDataInt
		assert.Equal(t, jobData, foundJobData)
	}
	// 30% failure, there should be at least 50 failed executions.
	assert.Equal(t, true, execCountMoreThan1 >= 50)
}

func TestMultiSubprocess_SaveData_RequestContextRandomCanceled(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	// We want to test that the Executor can handle context cancellation well. To simulate this, we'll reduce
	// MaxExecutionTime config to 300ms, and introduce random wait time between 300-400ms for 15% of the time.
	// We expect to see:
	//		- At least once execution for all Subprocesses.
	//		- Some Subprocesses might be executed twice. However, as long as the Subprocess has its own locking
	//		  and checking mechanism, it can ensure each Subprocess is only executed once.
	//		- The locking mechanism that's needed to ensure each Subprocess are only executed once are:
	//			- Lock on the subprocess entity.
	//			- Updating data and releasing the lock happening at the same time.

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	sum := atomic.Int64{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		roll := rand.Intn(100)
		if roll < 15 {
			// This will let the subprocess execution continue, while the context will actually be cancelled.
			time.Sleep(time.Duration(300+rand.Intn(100)) * time.Millisecond)
		}

		// In the first subprocess, generate random number and save this random number.
		// This will also test that results in subprocess #0 is saved for execution in next subprocess.
		// If this is not the case, due to 15% failure chance, we'll have intermittent errors when running tests.
		if subprocessIndex == 0 {
			// If saved data already has int, then this is a retry. Skip.
			if jobData.SavedDataInt != 0 {
				return SRSuccess
			}

			savedInt, err := strconv.Atoi(jobData.SavedDataString)
			if err != nil {
				return SRFailed
			}

			// This part might be executed twice.
			jobData.SavedDataInt = savedInt
			err = update(jobData)
			if err != nil {
				return SRFailed
			}
			return SRSuccess
		}

		// Only add to sum at the third and last job.
		if subprocessIndex == 2 {
			if jobData.SavedDataInt == 0 {
				panic("saved data does not appear!")
			}

			// This part might be executed twice.
			sum.Add(int64(jobData.SavedDataInt))
			return SRSuccess
		}

		return SRSuccess
	}

	// Register 1000 jobs.
	jobCount := 1000
	insertedJobs := make([]*multiJobData, 0)
	regTime := time.Now()
	expectedSum := int64(0)
	for i := 0; i < jobCount; i++ {
		jobData := process.newJobData()
		random := rand.Intn(25) + 10
		expectedSum += int64(random)
		jobData.SavedDataString = strconv.Itoa(random)

		// Insert jobs.
		err := client.Register(s.h.Ctx, jobData)
		assert.Nil(t, err)
		insertedJobs = append(insertedJobs, jobData)
	}

	// Create executors with max exec count of 5000.
	// This should be enough for 1000 jobs (3 subprocesses).
	// At the end of the day, successful execution count is still 1 per subprocess.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.SweepInterval = 200 * time.Millisecond
	executorA.config.MaxJobsPerSweep = 100
	executorA.config.MaxExecutionCount = 5000
	executorA.config.LeaseExpireDuration = 300 * time.Millisecond
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.ExecutionTimeout = 300 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Create another executor.
	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.SweepInterval = 200 * time.Millisecond
	executorB.config.MaxJobsPerSweep = 100
	executorB.config.MaxExecutionCount = 5000
	executorB.config.LeaseExpireDuration = 300 * time.Millisecond
	executorB.config.HeartbeatInterval = 100 * time.Millisecond
	executorB.config.ExecutionTimeout = 300 * time.Millisecond
	executorB.Start()
	defer executorB.Stop()

	// Wait until all jobs are done.
	s.h.WaitAllJobsDone(t, 5*time.Minute, proc.Id())

	// The sum might be larger, because some might be executed successfully more than once (due to
	assert.True(t, sum.Load() >= expectedSum)

	// All jobs are done, they are updated correctly.
	execCountMoreThan1 := 0
	for _, jobData := range insertedJobs {
		found, _ := s.h.GetJob(t, jobData.JobId)
		assert.Equal(t, jobData.JobId, found.JobId)
		assert.Equal(t, JSDone, found.Status)
		assert.NotEmpty(t, found.GoroutineId)
		assert.Equal(t, 3, found.NextSubprocess)
		assert.True(t, found.ExecCount >= 1)
		if found.ExecCount > 1 {
			execCountMoreThan1++
		}

		assert.Equal(t, true, found.GoroutineHeartBeatTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.CreatedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.StartedTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.EndTs.UnixMicro() >= regTime.UnixMicro())
		assert.Equal(t, true, found.LastUpdateTs.UnixMicro() >= regTime.UnixMicro())

		// JobData would be different, as Subprocess #0 saves additional data.
		foundJobData, err := process.Deserialize(found.JobData)
		assert.Nil(t, err)
		assert.NotEqual(t, jobData, foundJobData)

		// Only SavedInt are updated.
		jobData.SavedDataInt = foundJobData.SavedDataInt
		assert.Equal(t, jobData, foundJobData)

		// Each subprocess is executed at least once.
		process.assertExecAtLeastOnce(t, 0, jobData.JobId)
		process.assertExecAtLeastOnce(t, 1, jobData.JobId)
		process.assertExecAtLeastOnce(t, 2, jobData.JobId)
		process.assertValidTime(t, jobData.JobId)
	}
	// 15% failure, there should be at least 25 failed executions.
	assert.Equal(t, true, execCountMoreThan1 >= 25)
}

func TestMultiSubprocess_LeaseExpireIsHonored(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	// We'll make a stub that sleeps a long time, so we can make sure that another executor does not take over the
	// job if lease expiry is still updated.
	delayDuration := atomic.Int64{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		time.Sleep(time.Duration(delayDuration.Load()))
		return SRSuccess
	}
	delayDuration.Add(int64(3 * time.Second))

	// Create and run executor.
	leaseExpireDuration := 1 * time.Second
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.LeaseExpireDuration = leaseExpireDuration
	executorA.config.ExecutionTimeout = 12 * time.Second
	executorA.config.SweepInterval = 1 * time.Second
	executorA.config.SweepIntervalJitter = 10 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Register job and wait until job is executed, assert that the executor is actually running tasks.
	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)

	err = test.Await(5*time.Second, func() bool {
		job, _ := s.h.GetJob(t, jobData.JobId)
		return job.GoroutineId != ""
	})
	assert.Nil(t, err)

	job, _ := s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, true, strings.HasPrefix(job.GoroutineId, "ExecutorA"))
	goroutineId := job.GoroutineId

	// Start another Executor.
	executorB := newExecutor(t, s, proc, "ExecutorB")
	executorB.config.HeartbeatInterval = 100 * time.Millisecond
	executorB.config.LeaseExpireDuration = leaseExpireDuration
	executorB.config.ExecutionTimeout = 12 * time.Second
	executorB.config.SweepInterval = 1 * time.Second
	executorB.config.SweepIntervalJitter = 10 * time.Millisecond
	executorB.Start()
	defer executorB.Stop()

	// What we want to see is that the heartbeat actually gets updated per 100 milliseconds.
	// For 9 seconds (3 * 3) execution time, there should be around ~90 increments.
	//
	// To reduce intermittent test errors, let's just test for 45 increments.
	expectedTime := time.Now()
	for i := 0; i < 45; i++ {
		expectedTime = expectedTime.Add(100 * time.Millisecond)
		err = test.Await(8*time.Second, func() bool {
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

	// Wait until the job gets done.
	s.h.WaitJobStatus(t, 10*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, regTime)

	// Job doesn't get taken over by ExecutorB.
	job, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, goroutineId, job.GoroutineId)
	assert.Equal(t, []string{goroutineId}, job.GoroutineIds)
	assert.Equal(t, true, strings.HasPrefix(job.GoroutineId, "ExecutorA"))
}

func TestMultiSubprocess_EarlyExitDone(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	earlyExitAt := atomic.Int64{}
	shouldEarlyExit := atomic.Bool{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		if shouldEarlyExit.Load() && earlyExitAt.Load() == int64(subprocessIndex) {
			return SREarlyExitDone
		}
		return SRSuccess
	}

	// Create quick executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.SweepIntervalJitter = 10 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Assert job can be completed normally.
	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 1)

	// Exit early at subprocess index 0.
	earlyExitAt.Store(0)
	shouldEarlyExit.Store(true)
	jobData = process.newJobData()
	regTime = time.Now()
	err = client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 1, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 0)
	process.assertExecCount(t, 2, jobData.JobId, 0)

	// Exit early at subprocess index 1.
	earlyExitAt.Store(1)
	jobData = process.newJobData()
	regTime = time.Now()
	err = client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 2, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 0)

	// Exit early at subprocess index 2 - the same as job done normally.
	earlyExitAt.Store(2)
	jobData = process.newJobData()
	regTime = time.Now()
	err = client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 1)
}

func TestMultiSubprocess_EarlyExitError(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	earlyExitAt := atomic.Int64{}
	shouldEarlyExit := atomic.Bool{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		if shouldEarlyExit.Load() && earlyExitAt.Load() == int64(subprocessIndex) {
			return SREarlyExitError
		}
		return SRSuccess
	}

	// Create quick executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.HeartbeatInterval = 100 * time.Millisecond
	executorA.config.SweepInterval = 300 * time.Millisecond
	executorA.config.SweepIntervalJitter = 10 * time.Millisecond
	executorA.Start()
	defer executorA.Stop()

	// Assert job can be completed normally.
	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 1)

	// Exit early error at subprocess index 0.
	earlyExitAt.Store(0)
	shouldEarlyExit.Store(true)
	jobData = process.newJobData()
	regTime = time.Now()
	err = client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSError)
	assertJobError[*multiJobData](t, s.h, process, jobData, 1, 0, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 0)
	process.assertExecCount(t, 2, jobData.JobId, 0)

	// Exit early error at subprocess index 1.
	earlyExitAt.Store(1)
	shouldEarlyExit.Store(true)
	jobData = process.newJobData()
	regTime = time.Now()
	err = client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSError)
	assertJobError[*multiJobData](t, s.h, process, jobData, 1, 1, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 0)

	// Exit early error at subprocess index 2.
	earlyExitAt.Store(2)
	shouldEarlyExit.Store(true)
	jobData = process.newJobData()
	regTime = time.Now()
	err = client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 2*time.Second, jobData.JobId, JSError)
	assertJobError[*multiJobData](t, s.h, process, jobData, 1, 2, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 1)
}

func TestMultiSubprocess_RegisterExecute(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

	// We'll make a stub that sleeps, to test that RegisterExecute blocks until task is completed.
	delayDuration := atomic.Int64{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		time.Sleep(time.Duration(delayDuration.Load()))
		return SRSuccess
	}

	// Create and run executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.Start()
	defer executorA.Stop()

	// Register job and wait until job is executed, assert that the executor is actually running tasks.
	jobData := process.newJobData()
	regTime := time.Now()
	err := client.Register(s.h.Ctx, jobData)
	assert.Nil(t, err)
	s.h.WaitJobStatus(t, 5*time.Second, jobData.JobId, JSDone)

	// Assert job is done.
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, regTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 1)
	process.assertValidTime(t, jobData.JobId)

	// Register another job, now with 2 seconds delay per-subprocess.
	delayDuration.Store(int64(2 * time.Second))
	jobData = process.newJobData()
	startTime := time.Now()
	traceId := test.UUIDString()
	latestData, err := executorA.RegisterExecuteWait(s.h.Ctx, &tr.Trace{TraceId: traceId}, jobData)
	assert.Nil(t, err)
	assertJobDataIsLatest(t, s.h, proc, latestData)

	// Blocks for at least the delay duration (times 3 because for each subprocess, there's a delay).
	assert.GreaterOrEqual(t, time.Since(startTime), time.Duration(delayDuration.Load())*3)

	// Assert job is done.
	assertJobDone[*multiJobData](t, s.h, process, jobData, 1, 3, startTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 1)
	process.assertValidTime(t, jobData.JobId)

	// Assert that RegisterExecute uses the given Trace ID.
	found, _ := s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, []string{"ExecutorA-" + traceId}, found.GoroutineIds)
}

func TestMultiSubprocess_RegisterExecuteTakenOver(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)

	// We'll make a process that fails on the first and second execution, but on the second subprocess.
	runCount := atomic.Int64{}
	process.stub = func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult {
		if subprocessIndex == 1 {
			count := runCount.Add(1)
			if count == 1 {
				return SRFailed
			}
			if count == 2 {
				panic("simulate panic error")
			}
		}

		return SRSuccess
	}

	// Create and run executor.
	executorA := newExecutor(t, s, proc, "ExecutorA")
	executorA.config.LeaseExpireDuration = 2 * time.Second
	executorA.config.SweepInterval = 3 * time.Second
	executorA.Start()
	defer executorA.Stop()

	// RegisterExecute a job, this first execution will end in an error.
	jobData := process.newJobData()
	startTime := time.Now()
	traceId := test.UUIDString()
	latest, err := executorA.RegisterExecuteWait(s.h.Ctx, &tr.Trace{TraceId: traceId}, jobData)
	assert.NotNil(t, err)
	assertJobDataIsLatest(t, s.h, proc, latest)

	// Assert job ends up in failure.
	assert.Equal(t, int64(1), runCount.Load())
	assertJobFailExec(t, s.h, proc, jobData, 1, 1, startTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 1)
	process.assertExecCount(t, 2, jobData.JobId, 0)

	// Assert the trace ID is inserted into goroutine ID.
	found, _ := s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, "ExecutorA-"+traceId, found.GoroutineId)
	assert.Equal(t, "ExecutorA-"+traceId, found.GoroutineIds[0])
	goroutine1 := found.GoroutineId

	// Wait until the executor picks it up.
	// Max wait time is LeaseExpireDuration + SweepInterval + SweepIntervalJitter.
	err = test.Await(10*time.Second, func() bool {
		return runCount.Load() == int64(2)
	})
	assert.Nil(t, err)

	// Assert job ends up in failure.
	assert.Equal(t, int64(2), runCount.Load())
	assertJobFailExec(t, s.h, proc, jobData, 2, 1, startTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 2)
	process.assertExecCount(t, 2, jobData.JobId, 0)

	// Assert goroutine ID inserted.
	found, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, 2, len(found.GoroutineIds))
	assert.Equal(t, goroutine1, found.GoroutineIds[0])
	assert.Equal(t, found.GoroutineId, found.GoroutineIds[1])
	goroutine2 := found.GoroutineId

	// Wait until job is done (next execution).
	s.h.WaitJobStatus(t, 10*time.Second, jobData.JobId, JSDone)
	assertJobDone[*multiJobData](t, s.h, process, jobData, 3, 3, startTime)
	process.assertExecCount(t, 0, jobData.JobId, 1)
	process.assertExecCount(t, 1, jobData.JobId, 3)
	process.assertExecCount(t, 2, jobData.JobId, 1)
	process.assertValidTime(t, jobData.JobId)

	// Assert that RegisterExecute uses the given Trace ID.
	found, _ = s.h.GetJob(t, jobData.JobId)
	assert.Equal(t, 3, len(found.GoroutineIds))
	assert.Equal(t, goroutine1, found.GoroutineIds[0])
	assert.Equal(t, goroutine2, found.GoroutineIds[1])
	assert.Equal(t, found.GoroutineId, found.GoroutineIds[2])
}

func TestMultiSubprocess_StartAfter(t *testing.T) {
	if TestLeak {
		defer goleak.VerifyNone(t)
		defer pgtest.Close()
	} else {
		t.Parallel()
	}

	s := StateCreator().(*State)
	s.Setup(t)
	defer s.TearDown(t)

	proc := newMultiProcess()
	process := proc.(*multiProcess)
	client := NewClient[*multiJobData](s.Storage, proc)

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
	jobData1 := process.newJobData()
	regTime := time.Now()
	startAfter1 := regTime.Add(7 * time.Second)
	err := client.RegisterStartAfter(s.h.Ctx, jobData1, startAfter1)
	assert.Nil(t, err)

	// Schedule job 2 to be executed after 3 seconds.
	jobData2 := process.newJobData()
	startAfter2 := regTime.Add(3 * time.Second)
	err = client.RegisterStartAfter(s.h.Ctx, jobData2, startAfter2)
	assert.Nil(t, err)

	// Wait until second job is executed, wait time is 3 seconds.
	// Max wait time is always LeaseExpireDuration + SweepInterval + SweepIntervalJitter
	s.h.WaitJobStatus(t, 12*time.Second, jobData2.JobId, JSDone)

	// Assert second job is done.
	assertJobDone[*multiJobData](t, s.h, process, jobData2, 1, 3, regTime)
	process.assertExecCount(t, 0, jobData2.JobId, 1)
	process.assertExecCount(t, 1, jobData2.JobId, 1)
	process.assertExecCount(t, 2, jobData2.JobId, 1)
	process.assertValidTime(t, jobData2.JobId)

	// Assert second job is done after the schedule.
	found, _ := s.h.GetJob(t, jobData2.JobId)
	assert.GreaterOrEqual(t, found.StartAfterTs.UnixMicro(), startAfter2.UnixMicro())
	assert.GreaterOrEqual(t, found.StartedTs, startAfter2)
	assert.GreaterOrEqual(t, found.EndTs, startAfter2)

	// Assert only Job 2 is done.
	process.assertExecCount(t, 0, jobData1.JobId, 0)
	process.assertExecCount(t, 1, jobData1.JobId, 0)
	process.assertExecCount(t, 2, jobData1.JobId, 0)

	// Assert first job is still not done.
	assertJobNotTakenOver[*multiJobData](t, s.h, process, jobData1, regTime)

	// Wait until the first job is executed.
	// Max wait time is always LeaseExpireDuration + SweepInterval + SweepIntervalJitter.
	s.h.WaitJobStatus(t, 10*time.Second, jobData1.JobId, JSDone)

	// Assert first job is executed.
	process.assertExecCount(t, 0, jobData1.JobId, 1)
	process.assertExecCount(t, 1, jobData1.JobId, 1)
	process.assertExecCount(t, 2, jobData1.JobId, 1)
	process.assertValidTime(t, jobData1.JobId)

	// Assert first job is done.
	assertJobDone[*multiJobData](t, s.h, process, jobData1, 1, 3, regTime)

	// Assert first job is done after schedule.
	found, _ = s.h.GetJob(t, jobData1.JobId)
	assert.GreaterOrEqual(t, found.StartAfterTs.UnixMicro(), startAfter1.UnixMicro())
	assert.GreaterOrEqual(t, found.StartedTs, startAfter1)
	assert.GreaterOrEqual(t, found.EndTs, startAfter1)
}
