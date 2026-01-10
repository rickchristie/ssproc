package ssproc

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rickchristie/ssproc/internal/testutil"
)

var _ JobData = (*TestJobData)(nil)
var _ Process[JobData] = (*testProcess[JobData])(nil)

// TestJobData is a simple job data for testing.
type TestJobData struct {
	JobId string
}

func (t *TestJobData) GetJobId() string {
	return t.JobId
}

// testProcess can be used to quickly create new processes without defining new structs.
type testProcess[Data JobData] struct {
	id           string
	newJobData   func() Data
	subprocesses []*Subprocess[Data]
}

func newTestProcess[Data JobData](id string, newJobData func() Data) *testProcess[Data] {
	return &testProcess[Data]{
		id:         id,
		newJobData: newJobData,
	}
}

// NewTestProcess creates a new test process with the given ID for testing.
// This is an exported version for use in other test files.
func NewTestProcess(id string) Process[*TestJobData] {
	return newTestProcess(id, func() *TestJobData {
		return &TestJobData{JobId: testutil.UUIDString()}
	})
}

func (p *testProcess[Data]) Id() string {
	return p.id
}

func (p *testProcess[Data]) GetSubprocesses() []*Subprocess[Data] {
	return p.subprocesses
}

func (p *testProcess[Data]) Serialize(jobData Data) (serialized string, err error) {
	m, err := json.Marshal(jobData)
	return string(m), err
}

func (p *testProcess[Data]) Deserialize(serialized string) (jobData Data, err error) {
	ret := p.newJobData()
	err = json.Unmarshal([]byte(serialized), ret)
	return ret, err
}

// multiJobData is job data for multi-subprocess tests.
type multiJobData struct {
	*TestJobData

	SavedDataString string
	SavedDataInt    int
	SavedDataBool   bool
	SavedDataMap    map[string]int
}

var _ Process[*multiJobData] = (*multiProcess)(nil)

type multiProcess struct {
	*testProcess[*multiJobData]
	mux  *sync.RWMutex
	stub func(
		subprocessIndex int,
		goroutineId string,
		jobData *multiJobData,
		update JobDataUpdater[*multiJobData],
	) SubprocessResult

	// These fields are used to record execution count and execution time of each Subprocess.
	execCounts map[int]map[string]*atomic.Int64
	execTime   map[int]map[string]*atomic.Value
}

func newMultiProcess() Process[*multiJobData] {
	execCounts := map[int]map[string]*atomic.Int64{
		0: {},
		1: {},
		2: {},
	}
	execTime := map[int]map[string]*atomic.Value{
		0: {},
		1: {},
		2: {},
	}

	return &multiProcess{
		testProcess: &testProcess[*multiJobData]{
			id: "multi",
			newJobData: func() *multiJobData {
				return &multiJobData{}
			},
		},
		stub: func(
			subprocessIndex int,
			goroutineId string,
			jobData *multiJobData,
			update JobDataUpdater[*multiJobData],
		) SubprocessResult {
			return SRSuccess
		},
		mux:        &sync.RWMutex{},
		execCounts: execCounts,
		execTime:   execTime,
	}
}

func (m *multiProcess) newJobData() *multiJobData {
	m.mux.Lock()
	defer m.mux.Unlock()

	jobId := testutil.UUIDString()
	for i := 0; i < 3; i++ {
		m.execCounts[i][jobId] = &atomic.Int64{}
		m.execTime[i][jobId] = &atomic.Value{}
	}

	return &multiJobData{
		TestJobData: &TestJobData{JobId: jobId},
	}
}

func (m *multiProcess) recordExec(subProcessIndex int, jobId string) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	m.execTime[subProcessIndex][jobId].Store(time.Now())
	m.execCounts[subProcessIndex][jobId].Add(1)
}

func (m *multiProcess) assertExecCount(t *testing.T, subProcessIndex int, jobId string, expectedCount int64) {
	assert.Equal(t, expectedCount, m.execCounts[subProcessIndex][jobId].Load())
}

func (m *multiProcess) assertExecAtLeastOnce(t *testing.T, subProcessIndex int, jobId string) {
	assert.True(t, m.execCounts[subProcessIndex][jobId].Load() >= int64(1))
}

func (m *multiProcess) assertValidTime(t *testing.T, jobId string) {
	time1 := m.execTime[0][jobId].Load()
	time2 := m.execTime[1][jobId].Load()
	time3 := m.execTime[2][jobId].Load()

	assert.Greater(t, time3, time2)
	assert.Greater(t, time2, time1)
}

func (m *multiProcess) getTime(subProcessIndex int, jobId string) time.Time {
	return m.execTime[subProcessIndex][jobId].Load().(time.Time)
}

func (m *multiProcess) GetSubprocesses() []*Subprocess[*multiJobData] {
	return []*Subprocess[*multiJobData]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *multiJobData,
				update JobDataUpdater[*multiJobData],
			) SubprocessResult {
				// Wait 10 ms before returning. Using this we can ensure that step 2 is always executed before step 1,
				// and so forth. This works because comparison of time is using monotonic time.
				m.recordExec(0, job.JobId)
				time.Sleep(10 * time.Millisecond)

				// Run process.
				return m.stub(0, goroutineId, job, update)
			},
		},
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *multiJobData,
				update JobDataUpdater[*multiJobData],
			) SubprocessResult {
				// Wait 10 ms before returning. Using this we can ensure that step 2 is always executed before step 1,
				// and so forth. This works because comparison of time is using monotonic time.
				m.recordExec(1, job.JobId)
				time.Sleep(10 * time.Millisecond)

				// Run process.
				return m.stub(1, goroutineId, job, update)
			},
		},
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *multiJobData,
				update JobDataUpdater[*multiJobData],
			) SubprocessResult {
				// Wait 10 ms before returning. Using this we can ensure that step 2 is always executed before step 1,
				// and so forth. This works because comparison of time is using monotonic time.
				m.recordExec(2, job.JobId)
				time.Sleep(10 * time.Millisecond)

				// Run process.
				return m.stub(2, goroutineId, job, update)
			},
		},
	}
}

// singleJobData is job data for single-subprocess tests.
type singleJobData struct {
	*TestJobData
}

var _ Process[*singleJobData] = (*singleProcess)(nil)

type singleProcess struct {
	*testProcess[*singleJobData]
	stub func(
		goroutineId string,
		jobData *singleJobData,
		update JobDataUpdater[*singleJobData],
	) SubprocessResult
}

func newSingleProcess() Process[*singleJobData] {
	return &singleProcess{
		testProcess: &testProcess[*singleJobData]{
			id: "single",
			newJobData: func() *singleJobData {
				return &singleJobData{}
			},
		},
		stub: func(
			goroutineId string,
			jobData *singleJobData,
			update JobDataUpdater[*singleJobData],
		) SubprocessResult {
			return SRSuccess
		},
	}
}

func (s *singleProcess) newJobData() *singleJobData {
	jobId := testutil.UUIDString()
	return &singleJobData{
		TestJobData: &TestJobData{JobId: jobId},
	}
}

func (s *singleProcess) GetSubprocesses() []*Subprocess[*singleJobData] {
	return []*Subprocess[*singleJobData]{
		{
			Transaction: func(
				ctx context.Context,
				goroutineId string,
				job *singleJobData,
				update JobDataUpdater[*singleJobData],
			) SubprocessResult {
				return s.stub(goroutineId, job, update)
			},
		},
	}
}

// newExecutor creates an executor for testing.
func newExecutor[Data JobData](t *testing.T, s *State, process Process[Data], id string) *Executor[Data] {
	executor, err := NewExecutor(s.h.Ctx, process, s.Storage, ExecutorConfig{
		ExecutorName:        id,
		MaxWorkers:          100,
		HeartbeatInterval:   100 * time.Millisecond,
		LeaseExpireDuration: 2 * time.Second,
		SweepInterval:       100 * time.Millisecond,
		ExecutionTimeout:    10 * time.Second,
		MaxExecutionCount:   3,
	})
	assert.Nil(t, err)
	return executor
}
