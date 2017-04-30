// Package sf is a utility package for Go Snowflake Driver
package sf

import (
	"fmt"
)

const (
	// ErrInvalidOffsetStr is an error code for the case where a offset string is invalid. The input string must
	// consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes
	ErrInvalidOffsetStr = 268002

	errMsgInvalidOffsetStr = "offset must be a string consist of sHHMI where one sign character '+'/'-' followed by zero filled hours and minutes: %v"
)

// SnowflakeError is a error type including various Snowflake specific information.
type SnowflakeError struct {
	Number      int
	Message     string
	MessageArgs []interface{}
}

func (se *SnowflakeError) Error() string {
	message := se.Message
	if len(se.MessageArgs) > 0 {
		message = fmt.Sprintf(se.Message, se.MessageArgs)
	}
	return fmt.Sprintf("%06d (): %s", se.Number, message)
}
