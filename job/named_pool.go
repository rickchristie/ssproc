package job

import (
	"fmt"
	"github.com/alitto/pond"
	"github.com/rickchristie/ssproc/plugs"
	"github.com/rickchristie/ssproc/util"
	"sync"
	"time"
)

type Pool interface {
	Submit(func())
	StopAndWait()
}

type TaskItem struct {
	TaskId string
	Task   func()
}

// NotifyFinished is called by tasks to notify that the given Task ID has finished executing, allowing future tasks with
// the same ID to be submitted to NamedWorkerPool.
type NotifyFinished func(taskId string)

// TaskCreator is a function that creates the task. Instead of submitting created Task, we submit a creator instead,
// to avoid creating many closures that we end up not using.
type TaskCreator func(taskId string) func()

// NamedWorkerPool wraps a worker pool implementation to guarantee that no job with the same ID gets into the pool
// twice. This allows task scheduler to not worry about double and triple running of tasks and resubmit tasks as needed.
type NamedWorkerPool struct {
	logger plugs.Logger
	pool   Pool
	poolId string

	mux          sync.Mutex
	runningTasks map[string]struct{}

	idleTime time.Time
}

func NewNamedWorkerPool(poolId string, maxWorkers int, minWorkers int, logger plugs.Logger) *NamedWorkerPool {
	namedWorkerPool := NamedWorkerPool{
		logger:       logger,
		poolId:       poolId,
		runningTasks: make(map[string]struct{}),
		idleTime:     time.Now(),
	}

	// Using Pond will block in task submission when buffer is full. However it's very unlikely that we'll need to
	// expire 1 million orders in the same time. If we do reach this number, we'll need to change the way we expire
	// Order anyway.
	pool := pond.New(
		maxWorkers, 1000000,
		pond.MinWorkers(minWorkers),
		pond.Strategy(pond.Balanced()),
		pond.PanicHandler(namedWorkerPool.handlePanic),
	)
	namedWorkerPool.pool = pool
	return &namedWorkerPool
}

// IsIdle returns true when there are no tasks in the submission list. If it's idle, the second parameter will return
// the duration it has been idle. Useful if we want to create breaks in between batches of work.
func (w *NamedWorkerPool) IsIdle() (bool, time.Duration) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if len(w.runningTasks) > 0 {
		return false, 0
	}

	return true, time.Since(w.idleTime)
}

func (w *NamedWorkerPool) StopAndWait() {
	w.pool.StopAndWait()
}

func (w *NamedWorkerPool) Submit(taskId string, task func()) {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.submit(taskId, task)
}

func (w *NamedWorkerPool) SubmitBulk(list []*TaskItem) {
	w.mux.Lock()
	defer w.mux.Unlock()

	for _, it := range list {
		w.submit(it.TaskId, it.Task)
	}
}

func (w *NamedWorkerPool) SubmitCreator(taskId string, createTask TaskCreator) {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.submit(taskId, createTask(taskId))
}

// SubmitCreatorBulk performs better as it locks and unlocks only once to write multiple tasks.
func (w *NamedWorkerPool) SubmitCreatorBulk(taskIds []string, createTask TaskCreator) {
	w.mux.Lock()
	defer w.mux.Unlock()

	for _, taskId := range taskIds {
		w.submit(taskId, createTask(taskId))
	}
}

func (w *NamedWorkerPool) submit(taskId string, task func()) {
	_, ok := w.runningTasks[taskId]
	if ok {
		// Task is already submitted. Return immediately.
		return
	}

	// Mark the task to prevent submission of task with the same ID until its completion.
	w.runningTasks[taskId] = struct{}{}

	// Create the task and wrap it to unmark the ID when it's completed/panicked.
	namedTask := createNamedTask(taskId, task, w.logger, w.notifyFinished)

	// Submit named task to the pool.
	w.pool.Submit(namedTask)
}

func (w *NamedWorkerPool) notifyFinished(taskId string) {
	w.mux.Lock()
	defer w.mux.Unlock()

	delete(w.runningTasks, taskId)
	if len(w.runningTasks) <= 0 {
		w.idleTime = time.Now()
	}
}

func (w *NamedWorkerPool) handlePanic(val interface{}) {
	// We don't expect this to ever be triggered as we have our own panic recovery mechanism. Log as Fatal, definitely
	// needs to be checked immediately.
	w.logger.Fatal(
		"", fmt.Sprintf("Panic when running worker pool task: %v", val),
		map[string]any{
			"poolId": w.poolId,
		},
	)
}

func createNamedTask(taskId string, task func(), logger plugs.Logger, finished NotifyFinished) func() {
	return func() {
		// Ensure task is cleared to allow resubmission.
		defer func() {
			if p := recover(); p != nil {
				// We've panicked. Generate stack trace, log and run NotifyFinished to allow resubmission of future
				// task. Because NamedWorkerPool is generally used in a "sweep -> submit task" fashion. The task will
				// be resubmitted in next sweep.
				stack := util.GetStackTrace()
				logger.Error(
					"", fmt.Sprintf("panic when running worker task: %v", p),
					map[string]any{
						"taskId":     taskId,
						"stackTrace": stack,
					},
				)
			}
			finished(taskId)
		}()

		// Run the task.
		task()
	}
}
