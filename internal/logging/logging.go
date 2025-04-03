package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
)

// Log levels constants.
const (
	None = iota
	Error
	Warning
	Info
	Debug
)

var currentLevel atomic.Int32                  // Stores the current logging level atomically.
var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds) // Global logger instance.

func init() {
	// Default log level is Info.
	currentLevel.Store(Info)
}

// SetLevel atomically sets the global logging level.
// It clamps the input level to the valid range [None, Debug].
func SetLevel(level int) {
	// Clamp level to the valid range.
	if level < None {
		level = None
	} else if level > Debug {
		level = Debug
	}
	currentLevel.Store(int32(level))
	// Log the level change only if the new level is Debug (to avoid noise).
	// This internal log call uses the new level setting immediately.
	if level >= Debug {
		// Use internal logf to bypass public interface depth for runtime.Caller if needed,
		// but here direct call is fine as we don't need caller info for this specific message.
		// Ensure this message itself respects the new level.
		logf(Debug, "Log level set to %d", level)
	}
}

// GetLevel atomically retrieves the current logging level.
func GetLevel() int {
	return int(currentLevel.Load())
}

// ParseLevel converts a log level string (case-insensitive) to its integer representation.
// Returns Info level and an error if the string is invalid.
func ParseLevel(levelStr string) (int, error) {
	switch strings.ToLower(levelStr) {
	case "none":
		return None, nil
	case "error":
		return Error, nil
	case "warn", "warning":
		return Warning, nil
	case "info":
		return Info, nil
	case "debug":
		return Debug, nil
	default:
		// Return Info level as default on parse failure, along with an error.
		return Info, fmt.Errorf("invalid log level string: '%s'", levelStr)
	}
}

// SetupLogging configures the logging level based on an input string.
// Logs a warning and uses Info level if the input string is invalid.
// Returns the finally set log level.
func SetupLogging(levelStr string) int {
	level, err := ParseLevel(levelStr)
	if err != nil {
		// Log the warning using the *current* log level settings.
		// If the current level is below Warning, this message won't show.
		// This might happen if SetupLogging is called multiple times.
		logf(Warning, "Invalid log level '%s' provided, defaulting to 'info'. Error: %v", levelStr, err)
		// Fallback to Info level is already handled by ParseLevel returning Info on error.
	}
	SetLevel(level) // Set the parsed or default level globally.
	return level    // Return the level that was actually set.
}

// SetOutput changes the output destination of the global logger.
func SetOutput(w io.Writer) {
	logger.SetOutput(w)
}

// logf is the internal logging function that handles formatting and level checking.
// It's called by the public Logf function.
func logf(level int, format string, v ...interface{}) {
	// Check if the message level is sufficient to be logged based on the global level.
	if int32(level) > currentLevel.Load() {
		return // Skip logging if level is too low.
	}

	// Determine the standard prefix for the log level.
	// Use consistent single spacing for non-Debug levels.
	var levelPrefix string
	switch level {
	case Error:
		levelPrefix = "[ERROR] "
	case Warning:
		levelPrefix = "[WARN] "
	case Info:
		levelPrefix = "[INFO] "
	case Debug:
		// Debug prefix might be further augmented with caller info.
		levelPrefix = "[DEBUG] "
	default:
		// Should not happen with constants, but handle defensively.
		levelPrefix = "[UNKN] "
	}

	// Initialize the full log prefix, starting with the level indicator.
	fullPrefix := levelPrefix

	// If Debug level, retrieve and prepend caller information (optimized).
	if level == Debug {
		// Retrieve caller info only when necessary (Debug level is active).
		// runtime.Caller(2) gets info about the caller of Logf (our public function).
		pc, file, line, ok := runtime.Caller(2)
		if ok {
			funcName := "???" // Default function name if lookup fails.
			// Attempt to get the function name.
			if f := runtime.FuncForPC(pc); f != nil {
				// Use only the base name part of the function for brevity.
				funcName = filepath.Base(f.Name())
			}
			// Prepend caller info to the debug prefix.
			fullPrefix = fmt.Sprintf("%s%s:%d:%s ", levelPrefix, filepath.Base(file), line, funcName)
		} else {
			// Fallback if caller info cannot be retrieved.
			fullPrefix = fmt.Sprintf("%s???:0:??? ", levelPrefix)
		}
	}

	// Format the actual log message.
	message := fmt.Sprintf(format, v...)

	// Write the final log line using the standard logger.
	// logger.Println prepends its own prefix (date/time/microseconds) and appends a newline.
	logger.Println(fullPrefix + message)
}

// Logf logs a formatted message if the specified level is enabled according to the global setting.
// This is the public logging function intended for use by other packages.
func Logf(level int, format string, v ...interface{}) {
	logf(level, format, v...) // Call the internal implementation.
}