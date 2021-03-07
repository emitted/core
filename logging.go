package core

// LogLevel ...
type LogLevel int

const (
	LogLevelNone LogLevel = iota
	LogLevelDebug
	LogLevelInfo
	LogLevelError
)

// LogEntry represents log entry.
type LogEntry struct {
	Level   LogLevel
	Message string
	Fields  map[string]interface{}
}

// NewLogEntry creates new LogEntry.
func NewLogEntry(level LogLevel, message string, fields ...map[string]interface{}) LogEntry {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	}
	return LogEntry{
		Level:   level,
		Message: message,
		Fields:  f,
	}
}

// LogHandler handles log entries - i.e. writes into correct destination if necessary.
type LogHandler func(LogEntry)

func newLogger(level LogLevel, handler LogHandler) *logger {
	return &logger{
		level:   level,
		handler: handler,
	}
}

// logger can log entries.
type logger struct {
	level   LogLevel
	handler LogHandler
}

// log calls log handler with provided LogEntry.
func (l *logger) log(entry LogEntry) {
	if l == nil {
		return
	}
	if l.enabled(entry.Level) {
		l.handler(entry)
	}
}

// enabled says whether specified Level enabled or not.
func (l *logger) enabled(level LogLevel) bool {
	if l == nil {
		return false
	}
	return level >= l.level && l.level != LogLevelNone
}
