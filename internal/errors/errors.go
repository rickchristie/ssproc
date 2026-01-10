// Package errors provides simplified error handling for ssproc.
package errors

import (
	"fmt"
	"runtime/debug"
	"strings"
)

// AppErr is a simple error type with stack trace support.
type AppErr struct {
	msg         string
	debugValues map[string]string
	stackTrace  []string
	wrapped     error
}

var _ error = (*AppErr)(nil)

func (e *AppErr) Error() string {
	msg := e.msg
	if e.wrapped != nil && e.wrapped.Error() != msg {
		msg += " caused by: " + e.wrapped.Error()
	}
	for k, v := range e.debugValues {
		msg += fmt.Sprintf("; key:%v value:%v", k, v)
	}
	return e.msg
}

// Str attaches a key-value pair to this error for debugging.
func (e *AppErr) Str(key string, val interface{}) *AppErr {
	e.debugValues[key] = fmt.Sprintf("%v", val)
	return e
}

// DebugValues returns the debug values attached to this error.
func (e *AppErr) DebugValues() map[string]string {
	return e.debugValues
}

// Msg overwrites the error message.
func (e *AppErr) Msg(msg string) *AppErr {
	e.debugValues["__overwritten"] = e.msg
	e.msg = msg
	return e
}

// StackTrace returns the stack trace if available.
func (e *AppErr) StackTrace() []string {
	return e.stackTrace
}

// Unwrap returns the wrapped error.
func (e *AppErr) Unwrap() error {
	return e.wrapped
}

// Err creates a new AppErr with the given message.
func Err(msg string, withStackTrace bool) *AppErr {
	err := AppErr{
		msg:         msg,
		debugValues: make(map[string]string),
	}
	if withStackTrace {
		err.stackTrace = getStackTrace()
	}
	return &err
}

// Wrap wraps an existing error into an AppErr.
func Wrap(err error, withStackTrace bool) *AppErr {
	if err == nil {
		return nil
	}

	// If already an AppErr, add stack trace if needed.
	e, ok := err.(*AppErr)
	if ok {
		if withStackTrace && len(e.stackTrace) == 0 {
			e.stackTrace = getStackTrace()
		}
		return e
	}

	e = Err(err.Error(), false)
	e.wrapped = err

	if withStackTrace {
		e.stackTrace = getStackTrace()
	}

	return e
}

func getStackTrace() []string {
	rawStack := debug.Stack()
	if rawStack == nil {
		return []string{"{nil-stack-trace-returned-from-debug.Stack()}"}
	}

	stackStr := string(rawStack)
	split := strings.Split(stackStr, "\n")
	for i, s := range split {
		split[i] = strings.Replace(s, "\t", "    ", 1)
	}
	return split
}
