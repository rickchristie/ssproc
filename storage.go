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

	// ClearDoneJobs clears jobs that is already done, whose done time is earlier than the given activeThreshold.
	ClearDoneJobs(ctx context.Context, activeThreshold time.Time) error
}
