package ssproc

import (
	"context"
	"time"
)

type Storage interface {
	// RegisterJob registers the Job to database with the given metadata fields.
	//		- Client.Register will register with empty goroutine ID.
	//		- Executor.RegisterExecuteWait will register with goroutine ID and heartbeat already filled, it will
	//		  execute it directly.
	//
	// These fields are ignored and are inserted with default values:
	//		- Job.RunType (RTNormal)
	//		- Job.GoroutineIds (empty)
	//		- Job.ExecCount (0)
	//		- Job.CompCount (0)
	//		- Job.EndTs (nil)
	//		- Job.LastUpdateTs (now)
	//
	// Returns JobIdAlreadyExist if job with the same ID already exists.
	RegisterJob(ctx context.Context, job *Job) error

	// RegisterJobTx registers a job within an existing transaction.
	RegisterJobTx(ctx context.Context, tx QueryExecutor, job *Job) error

	// GetOpenJobCandidates return potentially open jobs (empty Job.GoroutineId, expired leases). Executor will
	// then pass these job IDs to the worker pool, which will try to TakeOverJob and execute, retry execution,
	// or compensate.
	//
	// It will return JSReady jobs where:
	//		- Job.GoroutineId is empty OR
	//		- Job.GoroutineHeartBeatTs < activeThreshold
	GetOpenJobCandidates(
		ctx context.Context,
		processId string,
		maxJobsToReturn int,
	) ([]string, error)

	// TryTakeOverJob marks a Job as being taken by the given goroutineId.
	//		- When non-nil Job and non-nil error is returned, it means:
	//			- The job is leased to the given goroutineId.
	//			- First heartbeat ping also is already registered by Storage.
	//		- Otherwise might return:
	//			- AlreadyLeased - Job is already leased to another Executor.
	//			- AlreadyError - Job status is "error".
	//			- AlreadyDone - Job status is "done".
	//		- Other errors are database errors, etc.
	//
	// TryTakeOverJob should return the Job if it's already leased by the same Goroutine.
	TryTakeOverJob(
		ctx context.Context,
		jobId string,
		goroutineId string,
		leaseExpireDuration time.Duration,
	) (
		job *Job,
		err error,
	)

	// SendHeartbeat updates last heartbeat ping to the storage. It must return AlreadyLeased error when the Job's
	// goroutineId is not the same.
	//
	// When error is returned:
	//		- If the returned error is AlreadyLeased, Executor will cancel the Job's context and will not move to
	//		  execute the next Subprocess.
	//		- Otherwise, Executor will retry after 3 seconds. If SendHeartbeat fails 3 times in a row, Executor will
	//		  cancel the Job's context and not move to execute the next Subprocess.
	SendHeartbeat(ctx context.Context, goroutineId string, jobId string, leaseExpireDuration time.Duration) error

	// GetJob returns latest Job information.
	GetJob(ctx context.Context, jobId string) (*Job, error)

	// UpdateJob updates Job. It must fail and return AlreadyLeased if the Job is already assigned to another
	// goroutineId. Only updates when the goroutineId is the same.
	//
	// Only these fields are updated:
	//		- Job.JobData
	//		- Job.Status
	//		- Job.NextSubprocess
	//		- Job.ExecCount
	//		- Job.StartedTs
	//		- Job.EndTs
	//
	// When error is returned:
	//		- If the returned error is AlreadyLeased, Executor will cancel the Job's context and will not move to
	//		  execute the next Subprocess.
	//		- Otherwise, Executor will retry after 3 seconds. If SendHeartbeat fails 3 times in a row, Executor will
	//		  cancel the Job's context and not move to execute the next Subprocess.
	//
	// When update is successful, it will also automatically update Job.GoroutineHeartBeatTs and
	// Job.GoroutineLeaseExpireTs based on the current time.
	UpdateJob(ctx context.Context, job *Job, leaseExpireDuration time.Duration) error

	// ClearDoneJobs delete jobs that is already done, whose done time is earlier than the given processId, activeThreshold,
	// and will delete up to maxRowsToDelete.
	// It will return the amount of successful jobs deleted.
	ClearDoneJobs(ctx context.Context, processId string, activeThreshold time.Time, maxRowsToDelete int) (int64, error)

	// FilterJobs returns jobs matching the filter criteria with pagination support.
	// Returns array of jobs, total count of matching rows, and error if any.
	FilterJobs(ctx context.Context, input FilterJob, page int, itemsPerPage int) (
		jobs []*Job,
		totalRows int,
		err error,
	)
}

// FilterJob contains filter parameters for querying jobs.
type FilterJob struct {
	// JobId filters by exact job ID match.
	JobId string

	// ProcessId filters by process ID.
	ProcessId string

	// GoroutineId filters by goroutine ID.
	GoroutineId string

	// JobStatus filters by job status.
	JobStatus JobStatus

	// ExecCountGt filters jobs with exec count greater than this value.
	// Use -1 to filter for exec_count >= 0. Zero value means no filter is applied.
	ExecCountGt int

	// ExecCountLt filters jobs with exec count less than this value.
	// Use 1 to filter for exec_count <= 0. Zero value means no filter is applied.
	ExecCountLt int

	// RunType filters by run type.
	RunType RunType

	// CreatedTsGte filters jobs created at or after this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	CreatedTsGte time.Time

	// CreatedTsLte filters jobs created at or before this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	CreatedTsLte time.Time

	// LastUpdateTsGte filters jobs last updated at or after this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	LastUpdateTsGte time.Time

	// LastUpdateTsLte filters jobs last updated at or before this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	LastUpdateTsLte time.Time

	// StartedTsGte filters jobs started at or after this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	StartedTsGte time.Time

	// StartedTsLte filters jobs started at or before this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	StartedTsLte time.Time

	// EndTsGte filters jobs ended at or after this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	EndTsGte time.Time

	// EndTsLte filters jobs ended at or before this time.
	// Zero value (checked with IsZero()) means no filter is applied.
	EndTsLte time.Time
}
