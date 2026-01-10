package job

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/alitto/pond"
)

// Pool is an interface for a worker pool.
type Pool interface {
	Submit(func())
	StopAndWait()
}

// TaskItem represents a task with an ID.
type TaskItem struct {
	TaskId string
	Task   func()
}

// NotifyFinished is called when a task finishes.
type NotifyFinished func(taskId string)

// TaskCreator creates a task function for the given task ID.
type TaskCreator func(taskId string) func()

// NamedWorkerPool is a worker pool that prevents duplicate task execution.
type NamedWorkerPool struct {
	logger Logger
	pool   Pool
	poolId string

	mux          sync.Mutex
	runningTasks map[string]struct{}

	idleTime time.Time
}

// NewNamedWorkerPool creates a new named worker pool.
func NewNamedWorkerPool(poolId string, maxWorkers int, minWorkers int, logger Logger) *NamedWorkerPool {
	namedWorkerPool := NamedWorkerPool{
		logger:       logger,
		poolId:       poolId,
		runningTasks: make(map[string]struct{}),
		idleTime:     time.Now(),
	}

	maxCapacity := getMaxCapacityForEnvironment()

	pool := pond.New(
		maxWorkers, maxCapacity,
		pond.MinWorkers(minWorkers),
		pond.Strategy(pond.Balanced()),
		pond.PanicHandler(namedWorkerPool.handlePanic),
	)
	namedWorkerPool.pool = pool
	return &namedWorkerPool
}

func getMaxCapacityForEnvironment() int {
	if os.Getenv("GO_TEST_ENV") == "TRUE" {
		return 10000
	}
	return 1000000
}

// IsIdle returns true if there are no running tasks.
func (w *NamedWorkerPool) IsIdle() (bool, time.Duration) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if len(w.runningTasks) > 0 {
		return false, 0
	}

	return true, time.Since(w.idleTime)
}

// StopAndWait stops the pool and waits for all tasks to complete.
func (w *NamedWorkerPool) StopAndWait() {
	w.pool.StopAndWait()
}

// Submit submits a task to the pool.
func (w *NamedWorkerPool) Submit(taskId string, task func()) {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.submit(taskId, task)
}

// SubmitBulk submits multiple tasks to the pool.
func (w *NamedWorkerPool) SubmitBulk(list []*TaskItem) {
	w.mux.Lock()
	defer w.mux.Unlock()

	for _, it := range list {
		w.submit(it.TaskId, it.Task)
	}
}

// SubmitCreator submits a task created by the given creator.
func (w *NamedWorkerPool) SubmitCreator(taskId string, createTask TaskCreator) {
	w.mux.Lock()
	defer w.mux.Unlock()
	w.submit(taskId, createTask(taskId))
}

// SubmitCreatorBulk submits multiple tasks created by the given creator.
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
		return
	}

	w.runningTasks[taskId] = struct{}{}

	namedTask := createNamedTask(taskId, task, w.logger, w.notifyFinished)
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
	stackTrace := getStackTrace()
	errMsg := fmt.Sprintf("named pool worker panic: %v\n%s", val, strings.Join(stackTrace, "\n"))
	if w.logger != nil {
		w.logger.Error("Failed to run worker pool task",
			"poolId", w.poolId,
			"panic", fmt.Sprintf("%v", val),
			"error", errMsg)
	}
}

func createNamedTask(taskId string, task func(), logger Logger, finished NotifyFinished) func() {
	return func() {
		defer func() {
			if p := recover(); p != nil {
				stackTrace := getStackTrace()
				errMsg := fmt.Sprintf("worker task failed: %v\n%s", p, strings.Join(stackTrace, "\n"))
				if logger != nil {
					logger.Error("Failed to run worker task (panicked)",
						"taskId", taskId,
						"error", errMsg)
				}
			}
			finished(taskId)
		}()

		task()
	}
}
