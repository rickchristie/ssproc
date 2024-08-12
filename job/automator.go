package job

import (
	"fmt"
	"github.com/rickchristie/ssproc/plugs"
	"time"
)

type Automator struct {
	logger         plugs.Logger
	AutomatorId    string
	RestartOnPanic bool

	stop    chan struct{}
	stopped chan struct{}

	skipLog bool
}

func NewAutomator(automatorId string, restartOnPanic bool, logger plugs.Logger) Automator {
	return Automator{
		logger:         logger,
		AutomatorId:    automatorId,
		RestartOnPanic: restartOnPanic,
	}
}

// StartInterval will run the given Task after interval duration delay and run it again after delay, repeatedly until
// Stop is called.
//
// Task is the function that needs to be routinely executed. If Task encounters error, it needs to fail silently and
// appropriately log the issue. Automator will continue regularly call the Task, so if Task can no longer be executed,
// mark this in the Task's state and return early.
//
// Please note that Automator expects the task to be synchronous. This way automator prevents multiple Task of the same
// type running in parallel. Interval timing starts after Task is completed.
func (a *Automator) StartInterval(task func(), interval time.Duration) {
	if a.stop != nil {
		// Means a task is already running. Since the caller will expect something to happen, panic and let programmer
		// know about this bug early.
		panic("automator is already running (can't be started twice)")
	}

	if !a.skipLog {
		a.logger.Info("", fmt.Sprintf("Automator %v starting task!", a.AutomatorId), nil)
	}

	a.stop = make(chan struct{})
	a.stopped = make(chan struct{})
	go a.intervalGoroutine(task, interval)
}

func (a *Automator) intervalGoroutine(task func(), interval time.Duration) {
	defer func() {
		if p := recover(); p != nil {
			// Log that the automator is stopped.
			if !a.skipLog {
				a.logger.Error(
					"", fmt.Sprintf("Automator %v panic recovered (stopped)!", a.AutomatorId),
					map[string]any{"error": p},
				)
			}

			// Reset automator. No need to close the channel as channels without references are automatically garbage
			// collected by Go.
			a.stop = nil

			// Restart if configured to do so.
			if a.RestartOnPanic {
				a.StartInterval(task, interval)
			}
		}
	}()

	timer := time.NewTimer(interval)
	for {
		select {
		case <-timer.C:
			// Running the task should block.
			task()
			// Because we've received value from C, it's safe to call reset directly.
			// See: https://golang.org/pkg/time/#Timer.Reset
			timer.Reset(interval)
		case <-a.stop:
			// Stop timer and exit goroutine (channel must be drained).
			// See: https://golang.org/pkg/time/#Timer.Reset
			if !timer.Stop() {
				<-timer.C
			}

			// Notify Stop function that we're already done.
			a.stopped <- struct{}{}
			return
		}
	}
}

// Stop sends stop signal to the goroutine, prompting it to exit. Note that Stop will block until the currently running
// Task is complete. It's safe to block, because Go will schedule the process to work on other goroutines.
func (a *Automator) Stop() {
	if !a.skipLog {
		a.logger.Info("", fmt.Sprintf("Automator %v stopping!", a.AutomatorId), nil)
	}

	// Tells the intervalGoroutine() function to stop processing. A send on channel happens before corresponding receive
	// from that channel completes. This will block until the receive statement is in the middle of processing.
	a.stop <- struct{}{}

	// We want to nullify the a.stop and a.stopped channel, however, there is no guarantee that the next statement is
	// executed after the receive on a.stop is completed. The receive might still be happening on the next statement,
	// so to be safe, we wait for the callback that it's done.
	<-a.stopped

	// Nil channel blocks forever, so this is dangerous, but should be fine because this is executed after we received
	// signal on a.stopped. Because Stop call is blocking, calling Start afterwards is safe.
	// See: https://dave.cheney.net/2014/03/19/channel-axioms
	a.stop = nil
	a.stopped = nil

	if !a.skipLog {
		a.logger.Info("", fmt.Sprintf("Automator %v stopped!", a.AutomatorId), nil)
	}
}
