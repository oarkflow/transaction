package logs

import (
	"fmt"
	"log"
)

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	WithFields(fields map[string]any) Logger
	SetLevel(level LogLevel)
	GetLevel() LogLevel
}

type DefaultLogger struct {
	Fields map[string]any
	Level  LogLevel
}

func (l *DefaultLogger) Info(msg string, args ...any) {
	if l.Level > InfoLevel {
		return
	}
	msg = l.prependFields(msg)
	log.Printf("[INFO] "+msg, args...)
}

func (l *DefaultLogger) Debug(msg string, args ...any) {
	if l.Level > DebugLevel {
		return
	}
	msg = l.prependFields(msg)
	log.Printf("[DEBUG] "+msg, args...)
}

func (l *DefaultLogger) Warn(msg string, args ...any) {
	if l.Level > WarnLevel {
		return
	}
	msg = l.prependFields(msg)
	log.Printf("[WARN] "+msg, args...)
}

func (l *DefaultLogger) Error(msg string, args ...any) {
	if l.Level > ErrorLevel {
		return
	}
	msg = l.prependFields(msg)
	log.Printf("[ERROR] "+msg, args...)
}

func (l *DefaultLogger) WithFields(fields map[string]any) Logger {
	newFields := make(map[string]any)
	for k, v := range l.Fields {
		newFields[k] = v
	}
	for k, v := range fields {
		newFields[k] = v
	}
	return &DefaultLogger{Fields: newFields, Level: l.Level}
}

func (l *DefaultLogger) SetLevel(level LogLevel) {
	l.Level = level
}

func (l *DefaultLogger) GetLevel() LogLevel {
	return l.Level
}

func (l *DefaultLogger) prependFields(msg string) string {
	if len(l.Fields) == 0 {
		return msg
	}
	fieldStr := "[FIELDS: "
	first := true
	for k, v := range l.Fields {
		if !first {
			fieldStr += ", "
		}
		fieldStr += fmt.Sprintf("%s=%v", k, v)
		first = false
	}
	fieldStr += "] "
	return fieldStr + msg
}

type AsyncLogger struct {
	underlying Logger
	logCh      chan logEntry
	done       chan struct{}
	level      LogLevel
}

type logEntry struct {
	level string
	msg   string
	args  []any
}

func NewAsyncLogger(underlying Logger) *AsyncLogger {
	al := &AsyncLogger{
		underlying: underlying,
		logCh:      make(chan logEntry, 100),
		done:       make(chan struct{}),
		level:      underlying.GetLevel(),
	}
	go al.processLogs()
	return al
}

func (al *AsyncLogger) processLogs() {
	for entry := range al.logCh {
		switch entry.level {
		case "INFO":
			al.underlying.Info(entry.msg, entry.args...)
		case "ERROR":
			al.underlying.Error(entry.msg, entry.args...)
		case "DEBUG":
			al.underlying.Debug(entry.msg, entry.args...)
		}
	}
	close(al.done)
}

func (al *AsyncLogger) Info(msg string, args ...any) {
	if al.level > InfoLevel {
		return
	}
	al.logCh <- logEntry{"INFO", msg, args}
}

func (al *AsyncLogger) Error(msg string, args ...any) {
	al.logCh <- logEntry{"ERROR", msg, args}
}

func (al *AsyncLogger) Debug(msg string, args ...any) {
	if al.level > DebugLevel {
		return
	}
	al.logCh <- logEntry{"DEBUG", msg, args}
}

func (al *AsyncLogger) WithFields(fields map[string]any) Logger {
	return NewAsyncLogger(al.underlying.WithFields(fields))
}

func (al *AsyncLogger) SetLevel(level LogLevel) {
	al.level = level
	al.underlying.SetLevel(level)
}

func (al *AsyncLogger) GetLevel() LogLevel {
	return al.level
}

func (al *AsyncLogger) Close() {
	close(al.logCh)
	<-al.done
}

type AuditLogger interface {
	LogEvent(event string, attrs map[string]any)
}
type SimpleAuditLogger struct{}

func (sal *SimpleAuditLogger) LogEvent(event string, attrs map[string]any) {
	log.Printf("Audit Event: %s, attrs: %v", event, attrs)
}
