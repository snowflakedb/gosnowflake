package logger

import (
	"fmt"
	"os"
)

// CloseFileOnLoggerReplace closes a log file when the logger is replaced.
// This is used by the easy logging feature to manage log file handles.
func CloseFileOnLoggerReplace(sflog interface{}, file *os.File) error {
	// Try to get the underlying default logger
	if dl, ok := unwrapToDefaultLogger(sflog); ok {
		if c, ok := dl.(EasyLoggingSupport); ok {
			return c.CloseFileOnLoggerReplace(file)
		}
	}
	return fmt.Errorf("logger does not support closeFileOnLoggerReplace")
}

// IsDefaultLogger checks if the given logger is a default logger instance.
// This is used by the easy logging feature to determine if reconfiguration is allowed.
func IsDefaultLogger(sflog interface{}) bool {
	_, ok := unwrapToDefaultLogger(sflog)
	return ok
}

// unwrapToDefaultLogger unwraps a logger to get to the underlying default logger if present
func unwrapToDefaultLogger(sflog interface{}) (interface{}, bool) {
	current := sflog

	// Special case: if this is a LoggerProxy, get the actual global logger
	if _, isProxy := current.(*LoggerProxy); isProxy {
		current = GetLogger()
	}

	// Unwrap all layers
	for {
		if u, ok := current.(Unwrapper); ok {
			current = u.Unwrap()
			continue
		}
		break
	}

	// Check if it's a default logger by checking if it has EasyLoggingSupport
	if _, ok := current.(EasyLoggingSupport); ok {
		return current, true
	}

	return nil, false
}
