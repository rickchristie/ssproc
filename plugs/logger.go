package plugs

// Logger may be implemented so users can use their own logging implementation.
type Logger interface {
	Info(traceId string, msg string, data map[string]any)
	Warn(traceId string, msg string, data map[string]any)
	Error(traceId string, msg string, data map[string]any)
	Fatal(traceId string, msg string, data map[string]any)
}
