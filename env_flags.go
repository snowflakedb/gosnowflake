package gosnowflake

import (
	"context"
	"os"
	"strconv"
)

// whether and how much to log, for a response-body.
// 0 => don't log anything
// a valid positive number => log response body bytes up to that number
const responseBodySampleSize = "GOSNOWFLAKE_RESPONSE_SAMPLE_SIZE"

// whether to dump details for retries.
const verboseRetryLogging = "GOSNOWFLAKE_VERBOSE_RETRY_LOGGING"

type ctxEnvFlagsKey struct{}

// ReadEnvIntFlag gets an integer value from specified env-var.
func ReadEnvIntFlag(flagName string) int64 {
	flagVal := os.Getenv(flagName)
	if flagVal == "" {
		return 0
	}
	intVal, err := strconv.ParseInt(flagVal, 10, 64)
	if err != nil {
		return 0
	}
	return intVal
}

// ReadEnvBoolFlag gets a boolean value from specified env-var.
func ReadEnvBoolFlag(flagName string) bool {
	flagVal := os.Getenv(flagName)
	if flagVal == "" {
		return false
	}
	boolVal, err := strconv.ParseBool(flagVal)
	if err != nil {
		return false
	}
	return boolVal
}

type envFlags map[string]interface{}

// AddEnvFlags stores a map of env-var values into the supplied context.
func AddEnvFlags(ctx context.Context) context.Context {
	envFlags := make(envFlags)
	envFlags[responseBodySampleSize] = ReadEnvIntFlag(responseBodySampleSize)
	envFlags[verboseRetryLogging] = ReadEnvBoolFlag(verboseRetryLogging)

	logger.WithContext(ctx).Infof("Loaded environment flags: %v", envFlags)

	return context.WithValue(ctx, ctxEnvFlagsKey{}, envFlags)
}

// GetEnvIntFlag retrieves a specified integer flag value from the supplied context.
func GetEnvIntFlag(ctx context.Context, flagName string) (int64, bool) {
	envFlags, ok := ctx.Value(ctxEnvFlagsKey{}).(envFlags)
	if !ok {
		return 0, false
	}
	val, found := envFlags[flagName]
	if !found {
		return 0, false
	}
	i, ok := val.(int64)
	return i, ok
}

// GetEnvBoolFlag retrieves a specified boolean flag value from the supplied context.
func GetEnvBoolFlag(ctx context.Context, flagName string) (bool, bool) {
	envFlags, ok := ctx.Value(ctxEnvFlagsKey{}).(envFlags)
	if !ok {
		return false, false
	}
	val, found := envFlags[flagName]
	if !found {
		return false, false
	}
	b, ok := val.(bool)
	return b, ok
}
