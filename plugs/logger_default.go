package plugs

import (
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var _ Logger = (*ZerologLogger)(nil)

type ZerologLogger struct {
	logger zerolog.Logger
}

func DefaultLogger(loggerId string) *ZerologLogger {
	return &ZerologLogger{
		logger: log.With().Str("loggerId", loggerId).Logger(),
	}
}

func (l *ZerologLogger) Info(traceId string, msg string, data map[string]any) {
	ev := l.logger.WithLevel(zerolog.InfoLevel)
	l.log(ev, traceId, msg, data)
}

func (l *ZerologLogger) Warn(traceId string, msg string, data map[string]any) {
	ev := l.logger.WithLevel(zerolog.WarnLevel)
	l.log(ev, traceId, msg, data)
}

func (l *ZerologLogger) Error(traceId string, msg string, data map[string]any) {
	ev := l.logger.WithLevel(zerolog.ErrorLevel)
	l.log(ev, traceId, msg, data)
}

func (l *ZerologLogger) Fatal(traceId string, msg string, data map[string]any) {
	ev := l.logger.WithLevel(zerolog.FatalLevel)
	l.log(ev, traceId, msg, data)
}

func (l *ZerologLogger) log(ev *zerolog.Event, traceId string, msg string, data map[string]any) {
	ev.Str("traceId", traceId)

	if data != nil {
		bytes, err := json.Marshal(data)
		if err != nil {
			ev.Str("parsingErr", fmt.Sprintf("failed to parse data: %v", err))
		} else {
			ev.RawJSON("data", bytes)
		}
	}

	ev.Msg(msg)
}
