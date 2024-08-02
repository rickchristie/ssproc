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

// Execute returns
type Execute[Data JobData] func(
	ctx context.Context,
	goroutineId string,
	job Data,
	update JobDataUpdater[Data],
) SubprocessResult

type Subprocess[Data JobData] struct {
	// Execute this subprocess. The library will ensure that Execute will be called at least once. Implementations need
	// to handle cases where Execute is called more than once:
	//		1. Check if the Transaction is already executed. If already executed, return early so Executor can process
	//		   the next Subprocess.
	//		2. Otherwise, execute the Transaction, continue the where we left-off if we can detect it.
	//
	// The library will only call Execute more than once when there are exceptional issues, such as:
	//		- Executor gets interrupted (server restart). Another Executor instance will pick up the Job.
	//		- Executor or Subprocess.Execute panics. Another Executor instance will pick up the Job.
	// 		- Subprocess.Execute returns error and Executor is configured to do retries.
	//
	// Please note that this library's main purpose is to guard against process interruption, providing a way to
	// automatically retry or reset system state whenever a multi-transaction process is interrupted. Subprocess
	// implementation MUST do its own locking to ensure serialized execution between multiple Subprocess.
	//
	// In normal operations, there will only be 1 Execute process running. However, because there's no way to forcibly
	// stop a goroutine, once Execute is running, it will continue to run until it stops by itself. The library only
	// guarantees that once heartbeat ping fails three times in a row:
	//		1. The context is cancelled.
	//		2. The next Subprocess in the list will not be executed.
	//		3. One of the workers will pick up the Job and either retry or compensate the process, depending on the
	//		   config of that Executor.
	//
	// This means it's possible to have multiple Subprocess.Execute is running for the same Job. This should be
	// exceptionally rare. An example scenario:
	//		1. Executor A is executing Process X (3 Subprocesses), Job 1.
	//		2. Executor A executes Subprocess X1.
	//		3. Executor A is unable to connect to heartbeat server (Storage).
	//			1. Executor A cancels the context.
	//			2. Executor A stops sending heartbeat pings for Job 1.
	//			3. Subprocess X1 does not have any external service call, so the cancelled context does not cause
	//			   errors in its execution. It continues normally.
	//			4. Subprocess X1 is a long process, so it's still running even after heartbeat timeout.
	//		4. Executor B picks up Process X, Job 1 because heartbeat timeout.
	//		5. Executor B executes Subprocess X1.
	//			- In Executor A, Subprocess X1 for Job 1 is still running.
	//			- In Executor B, Subprocess X1 for Job 1 just started running.
	//		6. Executor A, Subprocess X1 for Job 1 finished executing.
	//			- Executor A does not continue to Subprocess X2 because context is cancelled.
	//			- Executor B Subprocess X1 is redoing the Job
	//
	// For majority of cases, when you're accessing external services, such as databases or external API, requests are
	// made through network calls that will fail because the provided context is cancelled, so this shouldn't be a
	// problem because the next external request on Executor A will fail immediately. If your Job is CPU-heavy and you
	// want execution to stop when heartbeat ping fails, you can regularly check with:
	//
	//		select {
	//			case <-ctx.Done():
	//				return errors.New("context cancelled!")
	//			default:
	//				// No-op. Context is still valid, continue process.
	//		}
	//
	// When there is a change on the given Data, boolean should be returned alongside the newJobData.
	//
	// When there is a change on JobData, Execute must return non-nil JobData. The Executor will then make sure that
	// JobData gets updated, so it's visible for the next Subprocess.
	Transaction Execute[Data]

	// Compensation will be run to undo changes of Subprocess Transaction. There is no guarantee that the corresponding
	// Transaction is already completed when Compensation is triggered. It is triggered when there is at least 1 run
	// attempt on Transaction. This is the preferred behavior because:
	//		1. Transaction might not be a transaction at all, it might be calling an external service.
	//		   Timeout response when calling external service might mean network error, but the call was executed
	//		   successfully by the external service.
	//		2. Even if the Transaction is atomic, the interruption might happen after Transaction is completed, but
	//		   before we updated Job status.
	//
	// With this context, implementation for Compensation is the similar with Transaction:
	//		1. Check if Transaction was executed or not. If it was not executed, then
	//		2. If executed successfully, then cancel the Transaction's effects, but only this Transaction's effects.
	//
	// todo compensation: implement this.
	//Compensation Execute[Data]
}
