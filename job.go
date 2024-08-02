package ssproc

import "time"

type JobStatus string

const (
	JSReady       JobStatus = "ready"
	JSDone        JobStatus = "done"
	JSCompensated JobStatus = "compensated"
	JSError       JobStatus = "error"
)

type RunType string

const (
	RTNormal       RunType = "normal"
	RTCompensation RunType = "compensation"
)

type Job struct {
	// JobId is the unique ID of the Job. The system will ensure that whenever a Job with the same ID is registered,
	// it will not duplicate the Job.
	JobId string

	// JobData is serialized data containing details for this Job. We serialize to string to make it easier to debug
	// whenever a job encounters issues.
	JobData string

	// ProcessId denotes which Process this Job belongs to. Executor will only take over Job with the same process ID
	// as the one it currently has.
	ProcessId string

	// GoroutineId is the unique identifier of the executor that's currently holding the lease for executing this Job.
	// The Executor must continuously send heartbeat pings so other Executors don't take over the Job.
	GoroutineId string

	// GoroutineHeartBeatTs is the last time Executor reports that it's still working on this.
	GoroutineHeartBeatTs time.Time

	// GoroutineLeaseExpireTs is the time after which GoroutineId's lease on this Job is expired, and another Executor
	// can take over this Job.
	GoroutineLeaseExpireTs time.Time

	Status JobStatus

	// NextSubprocess is the index of Subprocess that should be executed next. When Job is first registered, this field
	// is zero. After the Executor finished the first Subprocess, it will increment the number to 1 before continuing
	// to the next Subprocess.
	NextSubprocess int

	// RunType is the current type of run.
	//		- RTNormal executes Subprocess.Transaction of NextSubprocess index and increments it when done.
	//		- RTCompensation executes Subprocess.Compensation of NextSubprocess index and decrements it when then.
	RunType RunType

	// GoroutineIds contains list of trace IDs of each run to make troubleshooting of JSError Jobs easier.
	GoroutineIds []string

	// ExecCount is the total number of retries that an Executor has done for this Job's Subprocess.Transaction.
	// Starts at zero. When ExecutorConfig.MaxExecutionCount is reached, it will stop retrying.
	ExecCount int

	// CompCount is the total number of retries that an Executor has done for this Job's Subprocess.Compensation.
	// Starts at zero. When ExecutorConfig.MaxCompensationCount is reached, it will stop retrying.
	CompCount int

	// CreatedTs is the time this Job is created.
	CreatedTs time.Time

	// StartAfterTs is the time when this Job can be executed. There is no guarantee that the Executor will execute
	// at exactly this time, first execution should happen between
	// StartAfterTs and StartAfterTs + ExecutorConfig.SweepInterval.
	StartAfterTs time.Time

	// StartedTs is filled with the time this Job is first executed. When zero value, it means this Job hasn't been
	// executed at all.
	StartedTs time.Time

	// EndTs is the time when this Job's status either moves to JSDone or JSError.
	EndTs time.Time

	// LastUpdateTs is the time when this Job is last updated.
	LastUpdateTs time.Time
}
