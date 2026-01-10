package ssproc

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
	"github.com/rickchristie/ssproc/internal/timeutil"
)

type cleanupJobData struct {
	Id string
}

func (j *cleanupJobData) GetJobId() string { return j.Id }

func TestExecutor_CleanupSuccessfulJobs(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	processId := "CleanupTest"
	proc := newTestProcess(processId, func() *cleanupJobData {
		return &cleanupJobData{}
	})

	mockTime := &timeutil.MockTime{
		NowStub: func() time.Time {
			return time.Date(2025, 03, 20, 0, 0, 0, 0, time.UTC)
		},
	}

	activeThreshold := mockTime.Now().AddDate(0, 0, -30)

	// insert successful job under the threshold
	s.h.TestJobRows(t, []*TestJobRow{
		{
			Id:        testutil.UUIDString(),
			Data:      testutil.RandomString(100),
			EndTs:     activeThreshold.Add(-10 * time.Second),
			Status:    JSDone,
			ProcessId: processId,
		},
		{
			Id:        testutil.UUIDString(),
			Data:      testutil.RandomString(100),
			EndTs:     activeThreshold.Add(-10 * time.Hour),
			Status:    JSDone,
			ProcessId: processId,
		},
		{
			Id:        testutil.UUIDString(),
			Data:      testutil.RandomString(100),
			EndTs:     activeThreshold.Add(-10 * time.Minute),
			Status:    JSDone,
			ProcessId: processId,
		},
		{
			Id:           "not-deleted-1",
			Data:         "{}",
			StartAfterTs: mockTime.Now().Add(1 * time.Hour),
			EndTs:        activeThreshold.Add(1 * time.Hour),
			Status:       JSReady,
			ProcessId:    processId,
		},
		{
			Id:        "not-deleted-2",
			Data:      testutil.RandomString(100),
			EndTs:     activeThreshold.Add(-1 * time.Hour),
			Status:    JSError,
			ProcessId: processId,
		},
	})

	counter := &testutil.Counter{}
	proc.subprocesses = []*Subprocess[*cleanupJobData]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *cleanupJobData,
				update JobDataUpdater[*cleanupJobData],
			) SubprocessResult {
				counter.Add("executed", job.Id)
				return SRSuccess
			},
		},
	}

	cleanupConfig := ExecutorConfig{
		ExecutorName:        "CleanupTestExecutor",
		MaxWorkers:          3,
		MinWorkers:          1,
		HeartbeatInterval:   500 * time.Millisecond,
		LeaseExpireDuration: 2 * time.Second,
		SweepInterval:       500 * time.Millisecond,
		SweepIntervalJitter: 100 * time.Millisecond,
		MaxJobsPerSweep:     10,
		ExecutionTimeout:    10 * time.Minute,
		MaxExecutionCount:   3,
		EnableCleanup:       true,
		CleanupInterval:     2 * time.Second,
		CleanupThreshold:    30 * 24 * time.Hour,
		CleanupBatchSize:    50,
	}
	executor, err := NewExecutor[*cleanupJobData](s.h.Ctx, proc, s.Storage, cleanupConfig)
	assert.Nil(t, err)
	SetExecutorMockTimeForTest(executor, mockTime)
	s.h.SetPgMockTimeForTest(mockTime)
	client := NewClientSimple[*cleanupJobData](s.Storage, proc)
	SetClientMockTimeForTest(client, mockTime)

	executor.Start()
	defer executor.Stop()

	totalJobs := 100
	jobIds := []string{}
	for i := 0; i < totalJobs; i++ {
		jobId := testutil.UUIDString()
		err = client.Register(s.h.Ctx, &cleanupJobData{
			Id: jobId,
		})
		assert.Nil(t, err)
		jobIds = append(jobIds, jobId)
	}

	// should delete all successful jobs under the activeThreshold
	err = testutil.Await(20*time.Second, func() bool {
		successfulJobBeforeThreshold := s.h.GetSuccessfulJobs(activeThreshold)
		return len(successfulJobBeforeThreshold) == 0
	})
	assert.Nil(t, err)

	expectedNotDeletedIds := []string{"not-deleted-1", "not-deleted-2"}
	latestJobs := s.h.GetAllJobs()
	for _, v := range expectedNotDeletedIds {
		assert.NotNil(t, latestJobs[v])
	}

	// adjust time so the newly created job will be swept
	allJobsSweptTime := mockTime.Now().Add(2 * time.Minute)
	mockTime.NowStub = func() time.Time {
		return allJobsSweptTime
	}
	err = testutil.Await(30*time.Second, func() bool {
		latestJobs = s.h.GetAllJobs()
		for _, v := range jobIds {
			if latestJobs[v].Status != JSDone {
				return false
			}
		}
		return true
	})
	assert.Nil(t, err)

	for _, v := range jobIds {
		assert.Equal(t, 1, counter.Get("executed", v))
	}
	assert.Equal(t, totalJobs, counter.TotalEvents())

	// newly successful job should not be deleted by automator.
	successfulJobs := s.h.GetSuccessfulJobs(time.Now())
	assert.Len(t, successfulJobs, totalJobs)

	// set the current time to 30 days after today so it swept all the existing job
	mockTime.NowStub = func() time.Time {
		return time.Date(2025, 04, 22, 0, 0, 0, 0, time.UTC)
	}
	// should delete all existing jobs
	err = testutil.Await(20*time.Second, func() bool {
		jobs := s.h.GetAllJobs()
		for _, v := range jobs {
			if v.Status == JSDone && v.EndTs.Before(mockTime.Now()) {
				return false
			}
		}

		return true
	})
	assert.Nil(t, err)
	expectedNotDeletedIds = []string{"not-deleted-2"}
	latestJobs = s.h.GetAllJobs()
	for _, v := range expectedNotDeletedIds {
		assert.NotNil(t, latestJobs[v])
	}
}

func TestExecutor_MultipleExecutorCleanup(t *testing.T) {
	t.Parallel()

	s := StateCreator()
	s.Setup(t)
	defer s.TearDown(t)

	processId := "CleanupTest"
	proc := newTestProcess(processId, func() *cleanupJobData {
		return &cleanupJobData{}
	})

	cleanupConfig := ExecutorConfig{
		ExecutorName:        "CleanupTestExecutor",
		MaxWorkers:          3,
		MinWorkers:          1,
		HeartbeatInterval:   500 * time.Millisecond,
		LeaseExpireDuration: 1 * time.Second,
		SweepInterval:       500 * time.Millisecond,
		SweepIntervalJitter: 100 * time.Millisecond,
		MaxJobsPerSweep:     50,
		ExecutionTimeout:    10 * time.Minute,
		MaxExecutionCount:   3,
		EnableCleanup:       true,
		CleanupInterval:     3 * time.Second,
		CleanupThreshold:    500 * time.Millisecond,
		CleanupBatchSize:    30,
	}
	executorA, err := NewExecutor[*cleanupJobData](s.h.Ctx, proc, s.Storage, cleanupConfig)
	assert.Nil(t, err)

	executorB, err := NewExecutor[*cleanupJobData](s.h.Ctx, proc, s.Storage, cleanupConfig)
	assert.Nil(t, err)

	client := NewClientSimple[*cleanupJobData](s.Storage, proc)
	executorA.Start()
	defer executorA.Stop()
	executorB.Start()
	defer executorB.Stop()

	totalJobs := 100
	jobIds := []string{}
	for i := 0; i < totalJobs; i++ {
		jobId := testutil.UUIDString()
		err = client.Register(s.h.Ctx, &cleanupJobData{
			Id: jobId,
		})
		assert.Nil(t, err)
		jobIds = append(jobIds, jobId)
	}
	proc.subprocesses = []*Subprocess[*cleanupJobData]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *cleanupJobData,
				update JobDataUpdater[*cleanupJobData],
			) SubprocessResult {
				time.Sleep(time.Duration(rand.Int64N(500)) * time.Millisecond)
				return SRSuccess
			},
		},
	}

	// expect all job to be deleted since cleanupThreshold is pretty small
	err = testutil.Await(40*time.Second, func() bool {
		res := s.h.GetAllJobs()
		return len(res) == 0
	})
	assert.Nil(t, err)
}
