package ssproc

import (
	"context"
)

type JobData interface {
	// GetJobId returns Job Id.
	GetJobId() string
}

type Process[Data JobData] interface {
	// Id must return the unique ID of the Process.
	Id() string

	// GetSubprocesses must return list of Subprocess that's ready for usage. The library guarantees that the Subprocess
	// instance will only be use for 1 unique Job, and then discarded. You may choose to return the same slice or create
	// new instances each time, depending on your needs. If you choose to return the same slice each time, make sure
	// your Subprocess is concurrent-safe.
	GetSubprocesses() []*Subprocess[Data]

	// Serialize serializes the JobRequest into string to be inserted to Storage.
	Serialize(jobData Data) (serialized string, err error)

	// Deserialize converts string to Job, so it can be executed.
	Deserialize(serialized string) (jobData Data, err error)
}

// JobDataUpdater is used by Subprocess to update JobData mid-execution. This can be used to save data before finishing
// the Subprocess, so it can be passed to the next Subprocess.
type JobDataUpdater[Data JobData] func(jobData Data) error

type SubprocessResult string

const (
	// SRSuccess means continue process normally to the next subprocess, or if there are none, mark the Job as done.
	SRSuccess SubprocessResult = "success"

	// SRFailed means Subprocess failed to complete. This can be due to an error encountered by Subprocess, or panic
	// on Subprocess execution. Depending on job execution state:
	//		- Subprocess is retried after lease is expired if ExecutorConfig.MaxExecutionCount is still not reached.
	//		- Process execution moves towards RTCompensation if configured to do so.
	//		- Process is set as JSError if there are no compensation, or if max execution for compensation is reached.
	SRFailed SubprocessResult = "failed"

	// SREarlyExitDone means early exit in Process execution, mark the job as done.
	// No need to proceed to the next Subprocess.
	SREarlyExitDone SubprocessResult = "early-exit-done"

	// SREarlyExitError means early exit in Process execution, mark the job as error.
	// No need to proceed to the next Subprocess.
	SREarlyExitError SubprocessResult = "early-exit-error"
)

// Execute is the function signature for subprocess execution.
type Execute[Data JobData] func(
	ctx context.Context,
	goroutineId string,
	job Data,
	update JobDataUpdater[Data],
) SubprocessResult

type Subprocess[Data JobData] struct {
	// Transaction is the main execution function for this subprocess.
	Transaction Execute[Data]

	// Compensation is the rollback function for this subprocess.
	// If nil, the subprocess is skipped during compensation execution.
	// Compensation functions must be idempotent.
	Compensation Execute[Data]
}
