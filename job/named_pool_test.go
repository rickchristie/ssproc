package job

import (
	"fmt"
	"github.com/alitto/pond"
	"github.com/rickchristie/ssproc/plugs"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolPanicHandler(t *testing.T) {
	handlePanic := func(e interface{}) {
		fmt.Println(e)
	}

	pool := pond.New(
		1, 1000000,
		pond.MinWorkers(1),
		pond.Strategy(pond.Balanced()),
		pond.PanicHandler(handlePanic),
	)

	taskGroup := pool.Group()

	var execCount uint32 = 0

	// Submit three tasks, the first two tasks will panic after some time.
	// See if the third task gets executed.
	taskGroup.Submit(func() {
		time.Sleep(1 * time.Second)
		atomic.AddUint32(&execCount, 1)
		panic("Panic 1!")
	})
	taskGroup.Submit(func() {
		time.Sleep(1 * time.Second)
		atomic.AddUint32(&execCount, 1)
		panic("Panic 2!")
	})
	taskGroup.Submit(func() {
		time.Sleep(3 * time.Second)
		atomic.AddUint32(&execCount, 1)
		fmt.Println("task is completed!")
	})

	taskGroup.Wait()

	assert.Equal(t, uint32(3), execCount)
}

func TestNamedWorkerPool_SubmitTask(t *testing.T) {
	namedPool := NewNamedWorkerPool(
		"test",
		200,
		2,
		plugs.DefaultLogger("TestNamedPool"),
	)

	// Sleep for 1 second.
	time.Sleep(1 * time.Second)

	// When first created, the pool is idle.
	isIdle, idleSince := namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.True(t, idleSince >= 1*time.Second)

	var mux sync.Mutex
	taskCount := make(map[string]int)
	startTask := make(chan struct{}, 10)

	taskCreator := func(taskId string) func() {
		return func() {
			// Wait for signal to run tasks.
			<-startTask

			mux.Lock()
			taskCount[taskId] = taskCount[taskId] + 1
			mux.Unlock()
		}
	}

	// Test submitted task is finished.
	aId := "a"
	namedPool.SubmitCreator(aId, taskCreator)
	assert.Equal(t, 0, taskCount[aId])

	isIdle, idleSince = namedPool.IsIdle()
	assert.False(t, isIdle)
	assert.Equal(t, time.Duration(0), idleSince)

	// Allow 1 task to be run, wait for 1 task to be completed.
	startTask <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, taskCount[aId])

	// Test submit the same task after it finished.
	// Submit multiple times, but only one task should be run.
	namedPool.SubmitCreator(aId, taskCreator)
	namedPool.SubmitCreator(aId, taskCreator)
	namedPool.SubmitCreator(aId, taskCreator)
	namedPool.SubmitCreator(aId, taskCreator)
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, taskCount[aId])

	// All tasks start immediately for next tasks.
	close(startTask)

	// Test submit different names.
	bId := "b"
	cId := "c"
	dId := "d"
	namedPool.SubmitCreator(aId, taskCreator)
	namedPool.SubmitCreator(bId, taskCreator)
	namedPool.SubmitCreator(cId, taskCreator)
	namedPool.SubmitCreator(dId, taskCreator)

	namedPool.StopAndWait()

	assert.Equal(t, 3, taskCount[aId])
	assert.Equal(t, 1, taskCount[bId])
	assert.Equal(t, 1, taskCount[cId])
	assert.Equal(t, 1, taskCount[dId])

	time.Sleep(1 * time.Second)
	isIdle, idleSince = namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.True(t, idleSince >= 1*time.Second)

	time.Sleep(2 * time.Second)
	isIdle, idleSince = namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.True(t, idleSince >= 3*time.Second)
}

func TestNamedWorkerPool_SubmitBulk(t *testing.T) {
	namedPool := NewNamedWorkerPool(
		"test",
		200,
		2,
		plugs.DefaultLogger("TestNamedPool"),
	)

	var mux sync.Mutex
	taskCount := make(map[string]int)
	startTask := make(chan struct{}, 10)

	taskCreator := func(taskId string) func() {
		return func() {
			// Wait for signal to run tasks.
			<-startTask

			mux.Lock()
			taskCount[taskId] = taskCount[taskId] + 1
			mux.Unlock()
		}
	}

	// Test submit bulk tasks.
	namedPool.SubmitCreatorBulk([]string{"a", "b", "a", "c", "d", "e"}, taskCreator)

	isIdle, idleSince := namedPool.IsIdle()
	assert.False(t, isIdle)
	assert.Equal(t, time.Duration(0), idleSince)

	// Give signal to start task.
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}

	// Wait for task to complete.
	namedPool.StopAndWait()

	isIdle, idleSince1 := namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.NotEqual(t, time.Duration(0), idleSince1)

	time.Sleep(500 * time.Millisecond)

	isIdle, idleSince2 := namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.NotEqual(t, time.Duration(0), idleSince2)

	assert.True(t, idleSince2 >= 500*time.Millisecond)
	assert.True(t, idleSince2 > idleSince1)

	assert.Equal(t, 1, taskCount["a"])
	assert.Equal(t, 1, taskCount["b"])
	assert.Equal(t, 1, taskCount["c"])
	assert.Equal(t, 1, taskCount["d"])
	assert.Equal(t, 1, taskCount["e"])
}

func TestNamedWorkerPool_SubmitDirect(t *testing.T) {
	namedPool := NewNamedWorkerPool(
		"test",
		200,
		2,
		plugs.DefaultLogger("TestNamedPool"),
	)

	// Sleep for 1 second.
	time.Sleep(1 * time.Second)

	// When first created, the pool is idle.
	isIdle, idleSince := namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.True(t, idleSince >= 1*time.Second)

	var mux sync.Mutex
	taskCount := make(map[string]int)
	startTask := make(chan struct{}, 10)

	taskCreator := func(taskId string) func() {
		return func() {
			// Wait for signal to run tasks.
			<-startTask

			mux.Lock()
			taskCount[taskId] = taskCount[taskId] + 1
			mux.Unlock()
		}
	}

	// Test submitted task is finished.
	aId := "a"
	namedPool.Submit(aId, taskCreator(aId))
	assert.Equal(t, 0, taskCount[aId])

	isIdle, idleSince = namedPool.IsIdle()
	assert.False(t, isIdle)
	assert.Equal(t, time.Duration(0), idleSince)

	// Allow 1 task to be run, wait for 1 task to be completed.
	startTask <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, taskCount[aId])

	// Test submit the same task after it finished.
	// Submit multiple times, but only one task should be run.
	namedPool.Submit(aId, taskCreator(aId))
	namedPool.Submit(aId, taskCreator(aId))
	namedPool.Submit(aId, taskCreator(aId))
	namedPool.Submit(aId, taskCreator(aId))
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, taskCount[aId])

	// All tasks start immediately for next tasks.
	close(startTask)

	// Test submit different names.
	bId := "b"
	cId := "c"
	dId := "d"
	namedPool.Submit(aId, taskCreator(aId))
	namedPool.Submit(bId, taskCreator(bId))
	namedPool.Submit(cId, taskCreator(cId))
	namedPool.Submit(dId, taskCreator(dId))

	namedPool.StopAndWait()

	assert.Equal(t, 3, taskCount[aId])
	assert.Equal(t, 1, taskCount[bId])
	assert.Equal(t, 1, taskCount[cId])
	assert.Equal(t, 1, taskCount[dId])

	time.Sleep(1 * time.Second)
	isIdle, idleSince = namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.True(t, idleSince >= 1*time.Second)

	time.Sleep(2 * time.Second)
	isIdle, idleSince = namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.True(t, idleSince >= 3*time.Second)
}

func TestNamedWorkerPool_SubmitDirectBulk(t *testing.T) {
	namedPool := NewNamedWorkerPool(
		"test",
		200,
		2,
		plugs.DefaultLogger("TestNamedPool"),
	)

	var mux sync.Mutex
	taskCount := make(map[string]int)
	startTask := make(chan struct{}, 10)

	taskCreator := func(taskId string) func() {
		return func() {
			// Wait for signal to run tasks.
			<-startTask

			mux.Lock()
			taskCount[taskId] = taskCount[taskId] + 1
			mux.Unlock()
		}
	}

	// Test submit bulk tasks.
	namedPool.SubmitBulk([]*TaskItem{
		{TaskId: "a", Task: taskCreator("a")},
		{TaskId: "b", Task: taskCreator("b")},
		{TaskId: "a", Task: taskCreator("a")},
		{TaskId: "c", Task: taskCreator("c")},
		{TaskId: "d", Task: taskCreator("d")},
		{TaskId: "e", Task: taskCreator("e")},
	})

	isIdle, idleSince := namedPool.IsIdle()
	assert.False(t, isIdle)
	assert.Equal(t, time.Duration(0), idleSince)

	// Give signal to start task.
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}
	startTask <- struct{}{}

	// Wait for task to complete.
	namedPool.StopAndWait()

	isIdle, idleSince1 := namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.NotEqual(t, time.Duration(0), idleSince1)

	time.Sleep(500 * time.Millisecond)

	isIdle, idleSince2 := namedPool.IsIdle()
	assert.True(t, isIdle)
	assert.NotEqual(t, time.Duration(0), idleSince2)

	assert.True(t, idleSince2 >= 500*time.Millisecond)
	assert.True(t, idleSince2 > idleSince1)

	assert.Equal(t, 1, taskCount["a"])
	assert.Equal(t, 1, taskCount["b"])
	assert.Equal(t, 1, taskCount["c"])
	assert.Equal(t, 1, taskCount["d"])
	assert.Equal(t, 1, taskCount["e"])
}
