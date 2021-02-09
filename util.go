// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/google/uuid"
)

type contextKey string

const (
	// MultiStatementCount controls the number of queries to execute in a single API call
	MultiStatementCount paramKey = "MULTI_STATEMENT_COUNT"
	// AsyncMode tells the server to not block the request on executing the entire query
	asyncMode paramKey = "ASYNC_MODE_QUERY"
	// QueryIDChan is the channel to receive the query ID from
	QueryIDChan paramKey = "QUERY_ID_CHANNEL"
	// SnowflakeRequestIDKey is optional context key to specify request id
	SnowflakeRequestIDKey contextKey = "SNOWFLAKE_REQUEST_ID"
	// streamChunkDownload determines whether to use a stream based chunk downloader
	streamChunkDownload paramKey = "STREAM_CHUNK_DOWNLOAD"
)

// WithMultiStatement returns a context that allows the user to execute the desired number of sql queries in one query
func WithMultiStatement(ctx context.Context, num int) (context.Context, error) {
	return context.WithValue(ctx, MultiStatementCount, num), nil
}

// WithAsyncMode returns a context that allows execution of query in async mode
func WithAsyncMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, asyncMode, true)
}

// WithQueryIDChan returns a context that contains the channel to receive the query ID
func WithQueryIDChan(ctx context.Context, c chan<- string) context.Context {
	return context.WithValue(ctx, QueryIDChan, c)
}

// WithRequestID returns a new context with the specified snowflake request id
func WithRequestID(ctx context.Context, requestID uuid.UUID) context.Context {
	return context.WithValue(ctx, SnowflakeRequestIDKey, requestID)
}

// WithStreamDownloader returns a context that allows the use of a stream based chunk downloader
func WithStreamDownloader(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamChunkDownload, true)
}

// Get the request ID from the context if specified, otherwise generate one
func getOrGenerateRequestIDFromContext(ctx context.Context) uuid.UUID {
	requestID, ok := ctx.Value(SnowflakeRequestIDKey).(uuid.UUID)
	if ok && requestID != uuid.Nil {
		return requestID
	}
	return uuid.New()
}

// integer min
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// integer max
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// time.Duration max
func durationMax(d1, d2 time.Duration) time.Duration {
	if d1-d2 > 0 {
		return d1
	}
	return d2
}

// time.Duration min
func durationMin(d1, d2 time.Duration) time.Duration {
	if d1-d2 < 0 {
		return d1
	}
	return d2
}

// toNamedValues converts a slice of driver.Value to a slice of driver.NamedValue for Go 1.8 SQL package
func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}
