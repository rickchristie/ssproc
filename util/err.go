package util

import "fmt"

// AppErr is used to wrap errors with stack trace.
//
// See: https://blog.golang.org/errors-are-values
// See: https://blog.golang.org/go1.13-errors
type AppErr struct {
	// ErrCode can be used to convey and differentiate between different types of errors.
	ErrCode int

	msg         string
	debugValues map[string]string
	stackTrace  []string
	wrapped     error
}

func (e *AppErr) Error() string {
	msg := e.msg
	if e.wrapped != nil && e.wrapped.Error() != msg {
		msg += " caused by: " + e.wrapped.Error()
	}

	// Possible perf issue if the keys and values are long.
	// However, Error() is rarely called, and we must not use AppError.Str for large values anyway.
	for k, v := range e.debugValues {
		msg += fmt.Sprintf("; key:%v value:%v", k, v)
	}
	return e.msg
}

func (e *AppErr) DebugValues() map[string]string {
	return e.debugValues
}

// Msg overwrites the error message of this error. Useful when wrapping third party errors.
// The current message will be inserted to debugValues.
func (e *AppErr) Msg(msg string) *AppErr {
	e.debugValues["__overwritten"] = e.msg
	e.msg = msg
	return e
}

// Code sets the error code for this error. Useful to differentiate between different error types.
func (e *AppErr) Code(errCode int) *AppErr {
	e.ErrCode = errCode
	return e
}

// StackTrace might return nil if AppErr is created without it.
func (e *AppErr) StackTrace() []string {
	return e.stackTrace
}

func (e *AppErr) Unwrap() error {
	return e.wrapped
}

// Err creates new AppErr instance.
func Err(msg string, withStackTrace bool) *AppErr {
	err := AppErr{
		msg:         msg,
		debugValues: make(map[string]string),
	}
	if withStackTrace {
		err.stackTrace = GetStackTrace()
	}
	return &err
}

func WrapErr(err error, withStackTrace bool) *AppErr {
	// If the error provided is already *AppErr, it does not make sense to wrap it further.
	e, ok := err.(*AppErr)
	if ok {
		// Stack trace is required, but it was not generated beforehand. So try generating it now.
		if withStackTrace && len(e.stackTrace) == 0 {
			e.stackTrace = GetStackTrace()
		}
		return e
	}

	e = Err(err.Error(), false)
	e.wrapped = err

	// Generate stack trace here to keep the skip level constant.
	if withStackTrace {
		e.stackTrace = GetStackTrace()
	}

	return e
}
