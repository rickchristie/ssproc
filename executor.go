package ssproc

import (
	"context"
	cRand "crypto/rand"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math"
	"math/big"
	"math/rand"
	"rukita.co/main/be/data"
	"rukita.co/main/be/lib/job"
	"rukita.co/main/be/lib/mend"
	"rukita.co/main/be/lib/tr"
	"rukita.co/main/be/lib/util"
	"runtime"
	"sort"
	"time"
)

// Executor regularly queries Storage to get list of jobs that are possibly free to execute. Jobs that are free to
// execute are:
//  1. Job.StartAfterTs >= now.
//  2. Job can be taken over when:
//     - Job.GoroutineId is empty OR
//     - Job.GoroutineLeaseExpireTs is already in the past.
//
// New Job can be registered through:
//   - Client.Register
//   - Client.RegisterStartAfter
//   - Executor.RegisterExecuteWait
type Executor[Data JobData] struct {
	name        string
	serviceName string
	processId   string
	logger      mend.Logger
	automator   job.Automator
	storage     Storage
	namedPool   *job.NamedWorkerPool
	process     Process[Data]

	ctx       context.Context
	cancelCtx func()
	timeUtil  util.Time
	config    ExecutorConfig
}

type ExecutorConfig struct {
	// ExecutorName is appended as prefix with UUIDv4 to create unique goroutine ID. Defaults to "Exec" string.
	ExecutorName string

	// MaxWorkers is the maximum amount of parallel background workers that are pooling and working on jobs (doesn't
	// count Executor.DirectExecute).
	MaxWorkers int

	// MinWorkers is the minimum amount of active workers.
	MinWorkers int

	// HeartbeatInterval defaults to every 45 seconds. The smaller this value, the more expensive each Job execution is.
	// If your LeaseExpireDuration is 1 minute, it doesn't make sense to report every 5 seconds, as most report will
	// be ignored.
	HeartbeatInterval time.Duration

	// LeaseExpireDuration defaults to 1 minute. LeaseExpireDuration must be longer than HeartbeatInterval.
	LeaseExpireDuration time.Duration

	// SweepInterval is the time in-between querying for potential Jobs to take over. Defaults to 50 seconds.
	SweepInterval time.Duration

	// SweepIntervalJitter is the random amount of time spent sleeping before starting each sweep. This helps multiple
	// executors that are probably started at the same time to not start sweeping in unison.
	//
	// Defaults to 1 second. If you don't want jitter, set it to very low amount, i.e. 1 nanosecond.
	SweepIntervalJitter time.Duration

	// MaxJobsPerSweep is the max amount of Jobs to be added to the worker pool per sweep. Defaults to 100. Increasing
	// this might increase memory consumption, depending on your worker pool count.
	MaxJobsPerSweep int

	// ExecutionTimeout is the duration the execution context for 1 Job will time out.
	// Defaults to 5 minutes.
	ExecutionTimeout time.Duration

	// MaxExecutionCount is the amount of time Executor will retry the execution before giving up and marking it as
	// error. It will also log as fatal when this happens.
	//
	// Defaults to 5.
	MaxExecutionCount int

	// MaxCompensationCount is the amount of time Executor will retry compensation before giving upa nd marking it as
	// error. It will also log as fatal when this happens.
	//
	// Defaults to 5.
	MaxCompensationCount int
}

func NewExecutor[Data JobData](
	ctx context.Context,
	process Process[Data],
	storage Storage,
	config ExecutorConfig,
) (
	*Executor[Data],
	error,
) {
	if config.ExecutorName == "" {
		config.ExecutorName = "Exec"
	}
	if config.MaxWorkers == 0 {
		config.MaxWorkers = runtime.NumCPU()
	}
	if config.MinWorkers < 0 {
		config.MinWorkers = 0
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 45 * time.Second
	}
	if config.LeaseExpireDuration == 0 {
		config.LeaseExpireDuration = 1 * time.Minute
	}
	if config.LeaseExpireDuration <= config.HeartbeatInterval {
		return nil, mend.Err("LeaseExpireDuration must be more than HeartBeatInterval", true)
	}
	if config.SweepInterval == 0 {
		config.SweepInterval = 50 * time.Second
	}
	if config.SweepIntervalJitter == 0 {
		config.SweepIntervalJitter = 1 * time.Second
	}
	if config.ExecutionTimeout == 0 {
		config.ExecutionTimeout = 5 * time.Minute
	}
	if config.MaxExecutionCount == 0 {
		config.MaxExecutionCount = 5
	}
	if config.MaxCompensationCount == 0 {
		config.MaxCompensationCount = 5
	}

	processId := process.Id()
	if processId == "" {
		return nil, mend.Err("given process has empty process ID", true)
	}

	name := fmt.Sprintf("%v-%v", config.ExecutorName, process.Id())
	logger := mend.NewZerologLogger(name)

	ctx, cancelCtx := context.WithCancel(ctx)
	return &Executor[Data]{
		serviceName: name,
		name:        config.ExecutorName,
		processId:   processId,
		logger:      logger,
		automator: job.NewAutomator(
			fmt.Sprintf("ExecutorSweeper:%v", process.Id()),
			true,
			logger,
		),
		storage: storage,
		namedPool: job.NewNamedWorkerPool(
			fmt.Sprintf("ExecutorPool:%v", process.Id()),
			config.MaxWorkers,
			config.MinWorkers,
		),
		process:   process,
		ctx:       ctx,
		cancelCtx: cancelCtx,
		timeUtil:  util.NewGlobalTime(data.DefaultTimeZone()),
		config:    config,
	}, nil
}

func (s *Executor[Data]) Start() {
	s.logger.InfoNoTrace().Msg(fmt.Sprintf("%v: Start Sweeping", s.serviceName))
	s.automator.StartInterval(s.sweepJobs, s.config.SweepInterval)
}

func (s *Executor[Data]) Stop() {
	defer s.cancelCtx()
	s.automator.Stop()
	s.namedPool.StopAndWait()
}

func (s *Executor[Data]) RegisterExecuteWait(ctx context.Context, trace *tr.Trace, jobData Data) (Data, error) {
	jobId := jobData.GetJobId()
	serialized, err := s.process.Serialize(jobData)
	if err != nil {
		return jobData, err
	}

	// Register JobData, but register it as already being leased to this executor.
	// This prevents other Executor from leasing this new JobData, and allows the caller to wait for the execution.
	// If this execution is interrupted, another Executor will pick it up and continue the execution.
	now := s.timeUtil.Now()
	goroutineId := fmt.Sprintf("%v-%v", s.name, trace.TraceId)
	err = s.storage.RegisterJob(ctx, &Job{
		JobId:                jobId,
		JobData:              serialized,
		ProcessId:            s.processId,
		GoroutineId:          goroutineId,
		GoroutineHeartBeatTs: now,
		Status:               JSReady,
		NextSubprocess:       0,
		ExecCount:            0,
		CreatedTs:            now,
		StartAfterTs:         now,
	})
	if err != nil {
		return jobData, err
	}

	err = s.execute(ctx, trace, goroutineId, jobId)

	// Try getting latest job data.
	latestJob, getErr := s.storage.GetJob(ctx, jobData.GetJobId())
	if getErr != nil {
		msg := fmt.Sprintf(
			"%v: Error getting job after RegisterExecuteWait for job: %v",
			s.serviceName, jobId,
		)
		s.logger.ErrorErr(trace, getErr).Msg(msg)

		// Return the original error.
		return jobData, err
	}

	latestData, getErr := s.process.Deserialize(latestJob.JobData)
	if getErr != nil {
		msg := fmt.Sprintf(
			"%v: Error getting job data after RegisterExecuteWait for job: %v",
			s.serviceName, jobId,
		)
		s.logger.ErrorErr(trace, getErr).Msg(msg)

		// Return the original error.
		return jobData, err
	}

	// Update latest job data, we do this no matter if the execution ends in error or not.
	return latestData, err
}

func (s *Executor[Data]) sweepJobs() {
	// Add jitter wait time.
	time.Sleep(time.Duration(rand.Intn(int(s.config.SweepIntervalJitter))))

	// Query 3 times the amount of MaxJobsPerSweep, we want to randomly select from a big pool.
	jobIds, err := s.storage.GetOpenJobCandidates(s.ctx, s.processId, s.config.MaxJobsPerSweep)
	if err != nil {
		s.logger.ErrorNoTrace().Error(err).Msg(fmt.Sprintf("%v: Error getting job candidates!", s.serviceName))
		return
	}

	if len(jobIds) == 0 {
		// No jobs to execute.
		return
	}

	// NOTE: The goal of randomly picking a subset is we want to reduce amount of clashes between 1 executor and
	// another. When there are multiple instance of executors each trying to get open jobs, chances are they will arrive
	// at wildly different parts of the pool. This will increase the amount of jobs that's being done.
	max := big.NewInt(math.MaxInt)
	seed, err := cRand.Int(cRand.Reader, max)
	if err != nil {
		s.logger.ErrorNoTrace().Error(err).Msg(fmt.Sprintf("%v: Error generating random seed!", s.serviceName))

		return
	}
	random := rand.New(rand.NewSource(seed.Int64()))
	sort.Slice(jobIds, func(i, j int) bool {
		return random.Intn(2) == 1
	})

	// Loop through jobs, but stop when we've added the max amount of jobs.
	jobsAdded := 0
	for _, jobId := range jobIds {
		s.namedPool.Submit(jobId, s.createTask(jobId))

		jobsAdded++
		if jobsAdded >= s.config.MaxJobsPerSweep {
			break
		}
	}

	s.logger.InfoNoTrace().Msg(fmt.Sprintf("%v: Submitted %v jobs to worker pool", s.serviceName, jobsAdded))
}

func (s *Executor[Data]) createTask(jobId string) func() {
	return func() {
		// Generate new trace ID, so we can track this JobData's execution.
		traceId := uuid.New().String()
		goroutineId := fmt.Sprintf("%v-%v", s.name, traceId)
		trace := &tr.Trace{
			TraceId: traceId,
			Start:   s.timeUtil.Now(),
			Request: jobId,
		}
		err := s.execute(s.ctx, trace, goroutineId, jobId)
		if err != nil {
			// JobData will be retried by another Executor.
			s.logger.ErrorErr(trace, err).
				Msg(fmt.Sprintf("%v: Error in job executor when executing %v", s.serviceName, jobId))
		}
	}
}

// execute only returns nil error if the Process is completed successfully. It returns non-nil error otherwise.
// RegisterExecuteWait returns latest Job after it's been completed. This allows clients to deserialize JobData and
// check exactly what went wrong in the execution.
//
// Some errors might not mean errors at all, check with errors.Is():
//   - AlreadyLeased
//   - AlreadyDone
//   - AlreadyError
//   - MarkedAsError
//
// todo pr: add execute test call for more in-depth test of each test execution.
func (s *Executor[Data]) execute(
	parentCtx context.Context,
	trace *tr.Trace,
	goroutineId string,
	jobId string,
) (err error) {
	defer func() {
		// Set error so caller (if any), knows there's an error.
		if pErr := recover(); pErr != nil {
			msg := fmt.Sprintf("%v: panic on ssproc execute lib: %v, jobId: %v", s.serviceName, pErr, jobId)
			err = mend.Err(msg, true)
			s.logger.ErrorErr(trace, err).Msg(msg)
		}
	}()

	// Here are the 3 goroutines with their stop conditions:
	//		1. Main goroutine (this function call), which starts #2 and #3.
	//			- Context cancelled.
	//			- Subprocess execution failure (error or panic).
	//			- Process execution is done.
	//		2. Heartbeat goroutine, regularly sends heartbeat ping.
	//			- Context cancelled.
	//			- Heartbeat ping fails 3 times in a row.
	//		3. Subprocess goroutine, started to execute the subprocess.
	//			- Subprocess execution failure (error or panic).
	//
	// Both #1 and #2 has `defer cancel()`, so if one of the stops, the other one will quit as soon as possible.
	// When both #1 and #2 quit, the Subprocess goroutine might still go on, as there are no way to stop a running
	// goroutine. Subprocess code must therefore use the given context correctly to reduce chances of multiple execution
	// of the same job.
	ctx, cancel := context.WithTimeout(parentCtx, s.config.ExecutionTimeout)
	defer cancel()

	curJob, err := s.storage.TryTakeOverJob(ctx, jobId, goroutineId, s.config.LeaseExpireDuration)
	if err != nil {
		// If error is due to JobData already leased to another executor, exit silently.
		if errors.Is(err, AlreadyLeased) {
			return nil
		}

		return err
	}

	// Defensive coding. Should never happen.
	if curJob.ProcessId != s.process.Id() || curJob.ProcessId != s.processId {
		msg := fmt.Sprintf(
			"process id mismatch, did process id change in-runtime? job: %v, executor: %v, process: %v",
			curJob.ProcessId, s.processId, s.process.Id(),
		)
		return mend.Err(msg, true)
	}

	jobData, err := s.process.Deserialize(curJob.JobData)
	if err != nil {
		return err
	}

	// First check execution count. This is a retry if execution count is more than MaxExecutionCount.
	if curJob.ExecCount >= s.config.MaxExecutionCount {
		// Setting status as error makes sure that this Job will no longer be picked up again.
		err = s.updateJobError(ctx, curJob)
		if err != nil {
			return err
		}

		return mend.Wrap(MarkedAsError, true)
	}

	// Run another goroutine to regularly send heartbeat to keep the lease open.
	go s.pingHeartbeat(ctx, cancel, trace, goroutineId, jobId)

	// Since we're already starting the execution process, we'll increase the execution count. If this is the first
	// execution, this will increase execution count to 1 before the first execution. Incrementing first makes sure
	// that no matter (panic, heartbeat error, etc.), the execution count is tracked correctly.
	//
	// It's always safe to update job metadata because goroutine ID is randomly generated and used as fencing token
	// check when updating.
	curJob.ExecCount++
	if curJob.StartedTs.IsZero() {
		curJob.StartedTs = s.timeUtil.Now()
	}
	curJob.GoroutineIds = append(curJob.GoroutineIds, goroutineId)
	err = s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
	if err != nil {
		return err
	}

	subprocesses := s.process.GetSubprocesses()
	for {
		// If we've reached the end, then break, so we can mark as done.
		if curJob.NextSubprocess > len(subprocesses)-1 {
			break
		}

		select {
		case <-ctx.Done():
			// Context cancelled due to timeout or heartbeat failure.
			return mend.Wrap(ContextCanceled, true)
		default:
			// No signal to stop, continue processing.
		}

		// Execute the next Subprocess.
		next := subprocesses[curJob.NextSubprocess]
		cRet := make(chan *execResult, 1)
		go s.executeSubprocess(ctx, goroutineId, curJob, jobData, next, cRet)

		// Wait until either:
		//		1. Context is cancelled due to timeout or heartbeat ping failure.
		//		2. Execution is done (either successfully, or with error/panic).
		var res *execResult
		select {
		case <-ctx.Done():
			return mend.Wrap(ContextCanceledOngoing, true)
		case res = <-cRet:
			// After this point, all executeSubprocess changes are visible in this goroutine.
		}
		close(cRet)

		// Check the result of the execution. Help log the error if there's a panic. Otherwise, we continue using
		// the given SubprocessResult. When panicking, we always act as if SRFailed is returned.
		if res.panicErr != nil {
			s.logger.FatalErr(trace, err).
				Msg(fmt.Sprintf("%v: Panic in subprocess execution of job %v!", s.serviceName, jobId))
			return mend.Wrap(SubprocessFailed, true).
				Msg(fmt.Sprintf("subprocess %v failed: %v", curJob.NextSubprocess, res.panicErr.Error()))
		}

		switch res.result {
		case SRFailed:
			return mend.Wrap(SubprocessFailed, true).
				Msg(fmt.Sprintf("subprocess %v failed!", curJob.NextSubprocess))
		case SREarlyExitDone:
			// Early exit, break SubprocessLoop and go straight to marking job as done.
			// We want to update NextSubprocess alongside setting the job as done so NextSubprocess is always
			// semantically correct. Technically this Subprocess executes successfully.
			curJob.NextSubprocess++
			return s.updateJobDone(ctx, curJob)
		case SREarlyExitError:
			// We don't need to update NextSubprocess because technically this Subprocess failed to execute.
			// Heartbeat will automatically stop when context is cancelled.
			return s.updateJobError(ctx, curJob)
		}

		// If we reach here, execution of this subprocess is successful, also we should proceed to the next Subprocess.
		// Update metadata, when there's a retry it will start at next subprocess.
		curJob.NextSubprocess++
		err = s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
		if err != nil {
			return err
		}
	}

	// All Subprocesses completed successfully, otherwise it will not exit the main loop.
	curJob.Status = JSDone
	curJob.EndTs = s.timeUtil.Now()
	err = s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
	if err != nil {
		return err
	}

	return nil
}

type execResult struct {
	result   SubprocessResult
	panicErr error
}

// executeSubprocess will update curJob. It won't cause data race because curJob is only read by main execute goroutine,
// waiting for signal from cRet, so happens before is established.
func (s *Executor[Data]) executeSubprocess(
	ctx context.Context,
	goroutineId string,
	curJob *Job,
	jobData Data,
	next *Subprocess[Data],
	cRet chan<- *execResult,
) {
	defer func() {
		if pErr := recover(); pErr != nil {
			msg := fmt.Sprintf("%v: panic on execution: %v", s.serviceName, pErr)
			cRet <- &execResult{
				result:   SRFailed,
				panicErr: mend.Err(msg, true).Str("job", curJob),
			}
		}
	}()

	result := next.Transaction(ctx, goroutineId, jobData, func(jobData Data) error {
		serialized, err := s.process.Serialize(jobData)
		if err != nil {
			return err
		}

		curJob.JobData = serialized
		return s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
	})
	cRet <- &execResult{result: result}
}

func (s *Executor[Data]) pingHeartbeat(
	ctx context.Context,
	ctxCancel func(),
	trace *tr.Trace,
	goroutineId string,
	jobId string,
) {
	// Defer cancelling context, so if for whatever reason heartbeat ping stops, the main execution goroutine also
	// stops as soon as possible.
	defer ctxCancel()

	defer func() {
		// Report whenever there's an error.
		if pErr := recover(); pErr != nil {
			msg := fmt.Sprintf("%v: panic on heartbeat ping: %v, jobId: %v", s.serviceName, pErr, jobId)
			err := mend.Err(msg, true)
			s.logger.ErrorErr(trace, err).Msg(msg)
		}
	}()

	timer := time.NewTimer(s.config.HeartbeatInterval)
	defer timer.Stop()

	for {
		timer.Reset(s.config.HeartbeatInterval)
		select {
		case <-ctx.Done():
			// When main execute function finishes, this goroutine will also finish.
			return
		case <-timer.C:
			failCount := 0
			for i := 0; i < 3; i++ {
				// We use background context to send heartbeat to reduce the amount of "context canceled" errors.
				// This happens when execute is already completed, so context is cancelled, but we're at the middle
				// of sending a heartbeat. It's fine to send the final heartbeat, in the next loop we're going to
				// return.
				err := s.storage.SendHeartbeat(ctx, goroutineId, jobId, s.config.LeaseExpireDuration)
				if err == nil {
					// Successfully sent heartbeat ping.
					// Wait for another interval to send another heartbeat ping.
					break
				}

				// Otherwise there's an error. Return immediately for these errors (cancels the context).
				if errors.Is(err, AlreadyLeased) ||
					errors.Is(err, AlreadyError) ||
					errors.Is(err, AlreadyDone) {
					// Immediately end both heartbeat goroutine (cancels the context).
					return
				}

				// If the error is context canceled, then we return immediately. The context has cancelled while we're
				// sending heartbeat. No need to log error in this case, because the normal execution at execute() will
				// return nil error and  cancel the context.
				if errors.Is(err, context.Canceled) {
					return
				}

				// If the error is caused by context timeout, we don't have to retry, as when we retry the result
				// will be the same. Log this as we don't expect many execution timeout error.
				if errors.Is(err, context.DeadlineExceeded) {
					s.logger.WarnErr(trace, err).
						Msg("Fail to send heartbeat (main context deadline exceeded)")
					return
				}

				failCount++
				ctxErr := ctx.Err()
				s.logger.ErrorErr(trace, err).
					MarshalJson("ctxErr", ctxErr).
					Msg(fmt.Sprintf(
						"%v: Fail to send heartbeat %v (%v): %v",
						s.serviceName,
						i, jobId,
						err.Error(),
					))
			}

			// If we fail to send heartbeat 3 times in a row, consider the lease to be out, don't continue.
			// Returning stops heartbeat goroutine and cancels the context, which will also stop the main goroutine.
			if failCount >= 3 {
				return
			}
		}
	}
}

func (s *Executor[Data]) updateJobError(ctx context.Context, curJob *Job) error {
	curJob.Status = JSError
	curJob.EndTs = s.timeUtil.Now()
	return s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
}

func (s *Executor[Data]) updateJobDone(ctx context.Context, curJob *Job) error {
	curJob.Status = JSDone
	curJob.EndTs = s.timeUtil.Now()
	return s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
}
