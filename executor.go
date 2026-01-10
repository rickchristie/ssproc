package ssproc

import (
	"context"
	cRand "crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"runtime"
	"sort"
	"time"

	"github.com/google/uuid"

	interr "github.com/rickchristie/ssproc/internal/errors"
	"github.com/rickchristie/ssproc/internal/job"
	"github.com/rickchristie/ssproc/internal/timeutil"
)

// Executor regularly queries Storage to get list of jobs that are possibly free to execute.
type Executor[Data JobData] struct {
	name             string
	serviceName      string
	processId        string
	logger           Logger
	automator        job.Automator
	cleanupAutomator job.Automator
	storage          Storage
	namedPool        *job.NamedWorkerPool
	process          Process[Data]

	ctx       context.Context
	cancelCtx func()
	timeUtil  timeutil.Time
	config    ExecutorConfig
}

// ExecutorConfig contains configuration for the Executor.
type ExecutorConfig struct {
	// ExecutorName is appended as prefix with UUIDv4 to create unique goroutine ID. Defaults to "Exec" string.
	ExecutorName string

	// MaxWorkers is the maximum amount of parallel background workers. Defaults to runtime.NumCPU().
	MaxWorkers int

	// MinWorkers is the minimum amount of active workers. Defaults to 0.
	MinWorkers int

	// HeartbeatInterval defaults to every 45 seconds.
	HeartbeatInterval time.Duration

	// LeaseExpireDuration defaults to 1 minute. Must be longer than HeartbeatInterval.
	LeaseExpireDuration time.Duration

	// SweepInterval is the time in-between querying for potential Jobs. Defaults to 50 seconds.
	SweepInterval time.Duration

	// SweepIntervalJitter is random sleep before each sweep. Defaults to 1 second.
	SweepIntervalJitter time.Duration

	// MaxJobsPerSweep is the max amount of Jobs to be added per sweep. Defaults to 100.
	MaxJobsPerSweep int

	// ExecutionTimeout is the duration the execution context for 1 Job will time out. Defaults to 5 minutes.
	ExecutionTimeout time.Duration

	// MaxExecutionCount is the amount of retries before marking as error. Defaults to 5.
	MaxExecutionCount int

	// MaxCompensationCount is the amount of compensation retries. Defaults to 5.
	MaxCompensationCount int

	// EnableCleanup enables automatic cleanup of successful jobs.
	EnableCleanup bool

	// CleanupInterval is the time between cleanup runs.
	CleanupInterval time.Duration

	// CleanupThreshold is the minimum age for a job to be cleaned up.
	CleanupThreshold time.Duration

	// CleanupBatchSize is the number of jobs to delete per batch. Defaults to 500.
	CleanupBatchSize int

	// Location is the timezone for timestamps. Defaults to UTC.
	Location *time.Location

	// Logger is the logger. Defaults to DefaultLogger.
	Logger Logger
}

// NewExecutor creates a new Executor.
func NewExecutor[Data JobData](
	ctx context.Context,
	process Process[Data],
	storage Storage,
	config ExecutorConfig,
) (*Executor[Data], error) {
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
		return nil, interr.Err("LeaseExpireDuration must be more than HeartBeatInterval", true)
	}
	if config.SweepInterval == 0 {
		config.SweepInterval = 50 * time.Second
	}
	if config.SweepIntervalJitter == 0 {
		config.SweepIntervalJitter = 1 * time.Second
	}
	if config.MaxJobsPerSweep == 0 {
		config.MaxJobsPerSweep = 100
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
	if config.CleanupBatchSize == 0 {
		config.CleanupBatchSize = 500
	}
	if config.Location == nil {
		config.Location = time.UTC
	}
	if config.Logger == nil {
		config.Logger = DefaultLogger(fmt.Sprintf("%v-%v", config.ExecutorName, process.Id()))
	}

	processId := process.Id()
	if processId == "" {
		return nil, interr.Err("given process has empty process ID", true)
	}

	name := fmt.Sprintf("%v-%v", config.ExecutorName, process.Id())
	logger := config.Logger

	// Create a logger adapter for the job package
	jobLogger := &jobLoggerAdapter{logger: logger}

	ctx, cancelCtx := context.WithCancel(ctx)
	return &Executor[Data]{
		serviceName: name,
		name:        config.ExecutorName,
		processId:   processId,
		logger:      logger,
		automator: job.NewAutomator(
			fmt.Sprintf("ExecutorSweeper:%v", process.Id()),
			true,
			jobLogger,
		),
		cleanupAutomator: job.NewAutomator(
			fmt.Sprintf("CleanupSweeper:%v", process.Id()),
			true,
			jobLogger,
		),
		storage: storage,
		namedPool: job.NewNamedWorkerPool(
			fmt.Sprintf("ExecutorPool:%v", process.Id()),
			config.MaxWorkers,
			config.MinWorkers,
			jobLogger,
		),
		process:   process,
		ctx:       ctx,
		cancelCtx: cancelCtx,
		timeUtil:  timeutil.NewGlobalTime(config.Location),
		config:    config,
	}, nil
}

// jobLoggerAdapter adapts Logger to job.Logger.
type jobLoggerAdapter struct {
	logger Logger
}

func (a *jobLoggerAdapter) Info(msg string, keyvals ...any)  { a.logger.Info(msg, keyvals...) }
func (a *jobLoggerAdapter) Error(msg string, keyvals ...any) { a.logger.Error(msg, keyvals...) }

// Start starts the executor.
func (s *Executor[Data]) Start() {
	s.logger.Info(fmt.Sprintf("%v: Start Sweeping", s.serviceName))
	s.automator.StartInterval(s.sweepJobs, s.config.SweepInterval)

	if s.config.EnableCleanup {
		if s.config.CleanupInterval == 0 || s.config.CleanupThreshold == 0 {
			panic("cleanup config is not complete")
		}
		s.cleanupAutomator.StartInterval(s.cleanupSuccessfulJobs, s.config.CleanupInterval)
	}
}

// Stop stops the executor.
func (s *Executor[Data]) Stop() {
	defer s.cancelCtx()
	s.automator.Stop()
	s.namedPool.StopAndWait()

	if s.config.EnableCleanup {
		s.cleanupAutomator.Stop()
	}
}

// RegisterExecuteWait registers a job and waits for it to complete.
func (s *Executor[Data]) RegisterExecuteWait(ctx context.Context, traceId string, jobData Data) (Data, error) {
	jobId := jobData.GetJobId()
	serialized, err := s.process.Serialize(jobData)
	if err != nil {
		return jobData, err
	}

	now := s.timeUtil.Now()
	goroutineId := fmt.Sprintf("%v-%v", s.name, traceId)
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

	err = s.execute(ctx, traceId, goroutineId, jobId)

	latestJob, getErr := s.storage.GetJob(ctx, jobData.GetJobId())
	if getErr != nil {
		s.logger.Error(fmt.Sprintf("%v: Error getting job after RegisterExecuteWait for job: %v", s.serviceName, jobId),
			"error", getErr.Error())
		return jobData, err
	}

	latestData, getErr := s.process.Deserialize(latestJob.JobData)
	if getErr != nil {
		s.logger.Error(fmt.Sprintf("%v: Error getting job data after RegisterExecuteWait for job: %v", s.serviceName, jobId),
			"error", getErr.Error())
		return jobData, err
	}

	return latestData, err
}

func (s *Executor[Data]) sweepJobs() {
	time.Sleep(time.Duration(rand.Intn(int(s.config.SweepIntervalJitter))))

	jobIds, err := s.storage.GetOpenJobCandidates(s.ctx, s.processId, s.config.MaxJobsPerSweep)
	if err != nil {
		s.logger.Error(fmt.Sprintf("%v: Error getting job candidates!", s.serviceName), "error", err.Error())
		return
	}

	if len(jobIds) == 0 {
		return
	}

	max := big.NewInt(math.MaxInt)
	seed, err := cRand.Int(cRand.Reader, max)
	if err != nil {
		s.logger.Error(fmt.Sprintf("%v: Error generating random seed!", s.serviceName), "error", err.Error())
		return
	}
	random := rand.New(rand.NewSource(seed.Int64()))
	sort.Slice(jobIds, func(i, j int) bool {
		return random.Intn(2) == 1
	})

	jobsAdded := 0
	for _, jobId := range jobIds {
		s.namedPool.Submit(jobId, s.createTask(jobId))
		jobsAdded++
		if jobsAdded >= s.config.MaxJobsPerSweep {
			break
		}
	}

	s.logger.Info(fmt.Sprintf("%v: Submitted %v jobs to worker pool", s.serviceName, jobsAdded))
}

// ExecuteNow executes a job immediately in the current goroutine.
func (s *Executor[Data]) ExecuteNow(jobId string) {
	task := s.createTask(jobId)
	task()
}

func (s *Executor[Data]) createTask(jobId string) func() {
	return func() {
		traceId := uuid.New().String()
		goroutineId := fmt.Sprintf("%v-%v", s.name, traceId)
		err := s.execute(s.ctx, traceId, goroutineId, jobId)
		if err != nil {
			s.logger.Error(fmt.Sprintf("%v: Error in job executor when executing %v", s.serviceName, jobId),
				"error", err.Error())
		}
	}
}

func (s *Executor[Data]) execute(
	parentCtx context.Context,
	traceId string,
	goroutineId string,
	jobId string,
) (err error) {
	defer func() {
		if pErr := recover(); pErr != nil {
			msg := fmt.Sprintf("%v: panic on ssproc execute lib: %v, jobId: %v", s.serviceName, pErr, jobId)
			err = interr.Err(msg, true)
			s.logger.Error(msg, "error", err.Error())
		}
	}()

	ctx, cancel := context.WithTimeout(parentCtx, s.config.ExecutionTimeout)
	defer cancel()

	curJob, err := s.storage.TryTakeOverJob(ctx, jobId, goroutineId, s.config.LeaseExpireDuration)
	if err != nil {
		if errors.Is(err, AlreadyLeased) {
			return nil
		}
		if errors.Is(err, AlreadyDone) {
			s.logger.Warn(fmt.Sprintf("%v: Job %v is already done, cannot execute", s.serviceName, jobId))
			return nil
		}
		return err
	}

	if curJob.ProcessId != s.process.Id() || curJob.ProcessId != s.processId {
		msg := fmt.Sprintf("process id mismatch, job: %v, executor: %v, process: %v",
			curJob.ProcessId, s.processId, s.process.Id())
		return interr.Err(msg, true)
	}

	jobData, err := s.process.Deserialize(curJob.JobData)
	if err != nil {
		return err
	}

	if curJob.ExecCount >= s.config.MaxExecutionCount {
		err = s.updateJobError(ctx, curJob)
		if err != nil {
			return err
		}
		return interr.Wrap(MarkedAsError, true)
	}

	go s.pingHeartbeat(ctx, cancel, traceId, goroutineId, jobId)

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
		if curJob.NextSubprocess > len(subprocesses)-1 {
			break
		}

		select {
		case <-ctx.Done():
			return interr.Wrap(ContextCanceled, true)
		default:
		}

		next := subprocesses[curJob.NextSubprocess]
		cRet := make(chan *execResult, 1)
		go s.executeSubprocess(ctx, goroutineId, curJob, jobData, next, cRet)

		var res *execResult
		select {
		case <-ctx.Done():
			return interr.Wrap(ContextCanceledOngoing, true)
		case res = <-cRet:
		}
		close(cRet)

		if res.panicErr != nil {
			s.logger.Fatal(fmt.Sprintf("%v: Panic in subprocess execution of job %v!", s.serviceName, jobId),
				"error", res.panicErr.Error())
			return interr.Wrap(SubprocessFailed, true).
				Msg(fmt.Sprintf("subprocess %v failed: %v", curJob.NextSubprocess, res.panicErr.Error()))
		}

		switch res.result {
		case SRFailed:
			return interr.Wrap(SubprocessFailed, true).
				Msg(fmt.Sprintf("subprocess %v failed!", curJob.NextSubprocess))
		case SREarlyExitDone:
			curJob.NextSubprocess++
			return s.updateJobDone(ctx, curJob)
		case SREarlyExitError:
			return s.updateJobError(ctx, curJob)
		}

		curJob.NextSubprocess++
		err = s.storage.UpdateJob(ctx, curJob, s.config.LeaseExpireDuration)
		if err != nil {
			return err
		}
	}

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
				panicErr: interr.Err(msg, true).Str("job", curJob),
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
	traceId string,
	goroutineId string,
	jobId string,
) {
	defer ctxCancel()

	defer func() {
		if pErr := recover(); pErr != nil {
			msg := fmt.Sprintf("%v: panic on heartbeat ping: %v, jobId: %v", s.serviceName, pErr, jobId)
			s.logger.Error(msg)
		}
	}()

	timer := time.NewTimer(s.config.HeartbeatInterval)
	defer timer.Stop()

	for {
		timer.Reset(s.config.HeartbeatInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			failCount := 0
			for i := 0; i < 3; i++ {
				err := s.storage.SendHeartbeat(ctx, goroutineId, jobId, s.config.LeaseExpireDuration)
				if err == nil {
					break
				}

				if errors.Is(err, AlreadyLeased) ||
					errors.Is(err, AlreadyError) ||
					errors.Is(err, AlreadyDone) {
					return
				}

				if errors.Is(err, context.Canceled) {
					return
				}

				if errors.Is(err, context.DeadlineExceeded) {
					s.logger.Warn("Fail to send heartbeat (main context deadline exceeded)", "error", err.Error())
					return
				}

				failCount++
				s.logger.Error(fmt.Sprintf("%v: Fail to send heartbeat %v (%v)", s.serviceName, i, jobId),
					"error", err.Error())
			}

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

func (s *Executor[Data]) cleanupSuccessfulJobs() {
	startCleanup := s.timeUtil.Now()
	cleanupThreshold := startCleanup.Add(-s.config.CleanupThreshold)

	cleanupAmount := int64(0)
	for {
		cleanupJitterDuration := time.Duration(1000+rand.Intn(1001)) * time.Millisecond
		time.Sleep(cleanupJitterDuration)

		res, err := s.storage.ClearDoneJobs(s.ctx, s.processId, cleanupThreshold, s.config.CleanupBatchSize)
		if err != nil {
			s.logger.Error(fmt.Sprintf("%v: clear done jobs failed", s.serviceName), "error", err.Error())
			return
		}

		if res == 0 {
			break
		}
		cleanupAmount += res
	}

	s.logger.Info(fmt.Sprintf("%v: Success on deleting successful jobs", s.serviceName),
		"cleanupThreshold", cleanupThreshold,
		"cleanupAmount", cleanupAmount,
		"cleanupDuration", time.Since(startCleanup))
}

// SetExecutorMockTimeForTest sets a mock time for testing.
func SetExecutorMockTimeForTest[Data JobData](executor *Executor[Data], mockTime timeutil.Time) {
	executor.timeUtil = mockTime
}
