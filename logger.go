package ssproc

import (
	"fmt"
	"os"
	"time"
)

// Logger is a minimal logging interface for ssproc.
// Implementations can wrap zerolog, slog, logrus, or any logger.
type Logger interface {
	Debug(msg string, keyvals ...any)
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
	Fatal(msg string, keyvals ...any)
}

// defaultLogger is a simple stdout logger.
type defaultLogger struct {
	name string
}

// DefaultLogger returns a simple stdout logger.
func DefaultLogger(name string) Logger {
	return &defaultLogger{name: name}
}

func (l *defaultLogger) log(level, msg string, keyvals ...any) {
	ts := time.Now().Format(time.RFC3339)
	kvStr := ""
	for i := 0; i < len(keyvals)-1; i += 2 {
		kvStr += fmt.Sprintf(" %v=%v", keyvals[i], keyvals[i+1])
	}
	fmt.Printf("%s [%s] %s: %s%s\n", ts, level, l.name, msg, kvStr)
}

func (l *defaultLogger) Debug(msg string, keyvals ...any) {
	l.log("DEBUG", msg, keyvals...)
}

func (l *defaultLogger) Info(msg string, keyvals ...any) {
	l.log("INFO", msg, keyvals...)
}

func (l *defaultLogger) Warn(msg string, keyvals ...any) {
	l.log("WARN", msg, keyvals...)
}

func (l *defaultLogger) Error(msg string, keyvals ...any) {
	l.log("ERROR", msg, keyvals...)
}

func (l *defaultLogger) Fatal(msg string, keyvals ...any) {
	l.log("FATAL", msg, keyvals...)
	os.Exit(1)
}

// noopLogger discards all output.
type noopLogger struct{}

// NoopLogger returns a logger that discards all output.
func NoopLogger() Logger {
	return &noopLogger{}
}

func (l *noopLogger) Debug(msg string, keyvals ...any) {}
func (l *noopLogger) Info(msg string, keyvals ...any)  {}
func (l *noopLogger) Warn(msg string, keyvals ...any)  {}
func (l *noopLogger) Error(msg string, keyvals ...any) {}
func (l *noopLogger) Fatal(msg string, keyvals ...any) {}
