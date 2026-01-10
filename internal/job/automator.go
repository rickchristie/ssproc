// Package job provides background task utilities for ssproc.
package job

import (
	"fmt"
	"runtime/debug"
	"strings"
	"time"
)

// Logger is a minimal logging interface for the job package.
type Logger interface {
	Info(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
}

// Automator runs a task repeatedly at a given interval.
type Automator struct {
	Logger         Logger
	AutomatorId    string
	RestartOnPanic bool

	stop    chan struct{}
	stopped chan struct{}

	skipLog bool
}

// NewAutomator creates a new automator.
func NewAutomator(automatorId string, restartOnPanic bool, logger Logger) Automator {
	return Automator{
		Logger:         logger,
		AutomatorId:    automatorId,
		RestartOnPanic: restartOnPanic,
	}
}

// StartInterval starts the automator to run the task at the given interval.
func (a *Automator) StartInterval(task func(), interval time.Duration) {
	if a.stop != nil {
		panic("automator is already running (can't be started twice)")
	}

	if !a.skipLog && a.Logger != nil {
		a.Logger.Info(fmt.Sprintf("Automator %v starting task!", a.AutomatorId), "automatorId", a.AutomatorId)
	}

	a.stop = make(chan struct{})
	a.stopped = make(chan struct{})
	go a.intervalGoroutine(task, interval)
}

func (a *Automator) intervalGoroutine(task func(), interval time.Duration) {
	defer func() {
		if p := recover(); p != nil {
			stackTrace := getStackTrace()
			errMsg := fmt.Sprintf("automator panicked: %v\n%s", p, strings.Join(stackTrace, "\n"))

			if !a.skipLog && a.Logger != nil {
				a.Logger.Error("Automator panic recovered (stopped!)",
					"automatorId", a.AutomatorId,
					"error", errMsg)
			}

			a.stop = nil

			if a.RestartOnPanic {
				a.StartInterval(task, interval)
			}
		}
	}()

	timer := time.NewTimer(interval)
	for {
		select {
		case <-timer.C:
			task()
			timer.Reset(interval)
		case <-a.stop:
			if !timer.Stop() {
				<-timer.C
			}
			a.stopped <- struct{}{}
			return
		}
	}
}

// Stop stops the automator and waits for the current task to complete.
func (a *Automator) Stop() {
	if !a.skipLog && a.Logger != nil {
		a.Logger.Info("Automator stopping!", "automatorId", a.AutomatorId)
	}

	a.stop <- struct{}{}
	<-a.stopped
	a.stop = nil
	a.stopped = nil

	if !a.skipLog && a.Logger != nil {
		a.Logger.Info("Automator stopped!", "automatorId", a.AutomatorId)
	}
}

func getStackTrace() []string {
	rawStack := debug.Stack()
	if rawStack == nil {
		return []string{"{nil-stack-trace}"}
	}
	stackStr := string(rawStack)
	split := strings.Split(stackStr, "\n")
	for i, s := range split {
		split[i] = strings.Replace(s, "\t", "    ", 1)
	}
	return split
}
