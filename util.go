package gosnowflake

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
)

type contextKey string

const (
	multiStatementCount    contextKey = "MULTI_STATEMENT_COUNT"
	asyncMode              contextKey = "ASYNC_MODE_QUERY"
	queryIDChannel         contextKey = "QUERY_ID_CHANNEL"
	snowflakeRequestIDKey  contextKey = "SNOWFLAKE_REQUEST_ID"
	fetchResultByID        contextKey = "SF_FETCH_RESULT_BY_ID"
	filePutStream          contextKey = "STREAMING_PUT_FILE"
	fileGetStream          contextKey = "STREAMING_GET_FILE"
	fileTransferOptions    contextKey = "FILE_TRANSFER_OPTIONS"
	enableDecfloat         contextKey = "ENABLE_DECFLOAT"
	arrowAlloc             contextKey = "ARROW_ALLOC"
	queryTag               contextKey = "QUERY_TAG"
	enableStructuredTypes  contextKey = "ENABLE_STRUCTURED_TYPES"
	embeddedValuesNullable contextKey = "EMBEDDED_VALUES_NULLABLE"
	describeOnly           contextKey = "DESCRIBE_ONLY"
	internalQuery          contextKey = "INTERNAL_QUERY"
	cancelRetry            contextKey = "CANCEL_RETRY"
	logQueryText           contextKey = "LOG_QUERY_TEXT"
	logQueryParameters     contextKey = "LOG_QUERY_PARAMETERS"
)

var (
	defaultTimeProvider = &unixTimeProvider{}
)

// WithMultiStatement returns a context that allows the user to execute the desired number of sql queries in one query
func WithMultiStatement(ctx context.Context, num int) context.Context {
	return context.WithValue(ctx, multiStatementCount, num)
}

// WithAsyncMode returns a context that allows execution of query in async mode
func WithAsyncMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, asyncMode, true)
}

// WithQueryIDChan returns a context that contains the channel to receive the query ID
func WithQueryIDChan(ctx context.Context, c chan<- string) context.Context {
	return context.WithValue(ctx, queryIDChannel, c)
}

// WithRequestID returns a new context with the specified snowflake request id
func WithRequestID(ctx context.Context, requestID UUID) context.Context {
	return context.WithValue(ctx, snowflakeRequestIDKey, requestID)
}

// WithFetchResultByID returns a context that allows retrieving the result by query ID
func WithFetchResultByID(ctx context.Context, queryID string) context.Context {
	return context.WithValue(ctx, fetchResultByID, queryID)
}

// WithFilePutStream returns a context that contains the address of the file stream to be PUT
func WithFilePutStream(ctx context.Context, reader io.Reader) context.Context {
	return context.WithValue(ctx, filePutStream, reader)
}

// WithFileGetStream returns a context that contains the address of the file stream to be GET
func WithFileGetStream(ctx context.Context, writer io.Writer) context.Context {
	return context.WithValue(ctx, fileGetStream, writer)
}

// WithFileTransferOptions returns a context that contains the address of file transfer options
func WithFileTransferOptions(ctx context.Context, options *SnowflakeFileTransferOptions) context.Context {
	return context.WithValue(ctx, fileTransferOptions, options)
}

// WithDescribeOnly returns a context that enables a describe only query
func WithDescribeOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, describeOnly, true)
}

// WithHigherPrecision returns a context that enables higher precision by
// returning a *big.Int or *big.Float variable when querying rows for column
// types with numbers that don't fit into its native Golang counterpart
// When used in combination with arrowbatches.WithBatches, original BigDecimal in arrow batches will be preserved.
func WithHigherPrecision(ctx context.Context) context.Context {
	return ia.WithHigherPrecision(ctx)
}

// WithDecfloatMappingEnabled returns a context that enables native support for DECFLOAT.
// Without this context, DECFLOAT columns are returned as strings.
// With this context enabled, DECFLOAT columns are returned as *big.Float or float64 (depending on HigherPrecision setting).
// Keep in mind that both float64 and *big.Float are not able to precisely represent some DECFLOAT values.
// If precision is important, you have to use string representation and use your own library to parse it.
func WithDecfloatMappingEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, enableDecfloat, true)
}

// WithArrowAllocator returns a context embedding the provided allocator
// which will be utilized by chunk downloaders when constructing Arrow
// objects.
func WithArrowAllocator(ctx context.Context, pool memory.Allocator) context.Context {
	return context.WithValue(ctx, arrowAlloc, pool)
}

// WithQueryTag returns a context that will set the given tag as the QUERY_TAG
// parameter on any queries that are run
func WithQueryTag(ctx context.Context, tag string) context.Context {
	return context.WithValue(ctx, queryTag, tag)
}

// WithStructuredTypesEnabled changes how structured types are returned.
// Without this context structured types are returned as strings.
// With this context enabled, structured types are returned as native Go types.
func WithStructuredTypesEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, enableStructuredTypes, true)
}

// WithEmbeddedValuesNullable changes how complex structures are returned.
// Instead of simple values (like string) sql.NullXXX wrappers (like sql.NullString) are used.
// It applies to map values and arrays.
func WithEmbeddedValuesNullable(ctx context.Context) context.Context {
	return context.WithValue(ctx, embeddedValuesNullable, true)
}

// WithInternal sets the internal query flag.
func WithInternal(ctx context.Context) context.Context {
	return context.WithValue(ctx, internalQuery, true)
}

// WithLogQueryText enables logging of the query text.
func WithLogQueryText(ctx context.Context) context.Context {
	return context.WithValue(ctx, logQueryText, true)
}

// WithLogQueryParameters enables logging of the query parameters.
func WithLogQueryParameters(ctx context.Context) context.Context {
	return context.WithValue(ctx, logQueryParameters, true)
}

// Get the request ID from the context if specified, otherwise generate one
func getOrGenerateRequestIDFromContext(ctx context.Context) UUID {
	requestID, ok := ctx.Value(snowflakeRequestIDKey).(UUID)
	if ok && requestID != nilUUID {
		return requestID
	}
	return NewUUID()
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

func int64Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func getMin(arr []int) int {
	if len(arr) == 0 {
		return -1
	}
	min := arr[0]
	for _, v := range arr {
		if v <= min {
			min = v
		}
	}
	return min
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

// TokenAccessor manages the session token and master token
type TokenAccessor interface {
	GetTokens() (token string, masterToken string, sessionID int64)
	SetTokens(token string, masterToken string, sessionID int64)
	Lock() error
	Unlock()
}

type simpleTokenAccessor struct {
	token        string
	masterToken  string
	sessionID    int64
	accessorLock sync.Mutex   // Used to implement accessor's Lock and Unlock
	tokenLock    sync.RWMutex // Used to synchronize SetTokens and GetTokens
}

func getSimpleTokenAccessor() TokenAccessor {
	return &simpleTokenAccessor{sessionID: -1}
}

func (sta *simpleTokenAccessor) Lock() error {
	sta.accessorLock.Lock()
	return nil
}

func (sta *simpleTokenAccessor) Unlock() {
	sta.accessorLock.Unlock()
}

func (sta *simpleTokenAccessor) GetTokens() (token string, masterToken string, sessionID int64) {
	sta.tokenLock.RLock()
	defer sta.tokenLock.RUnlock()
	return sta.token, sta.masterToken, sta.sessionID
}

func (sta *simpleTokenAccessor) SetTokens(token string, masterToken string, sessionID int64) {
	sta.tokenLock.Lock()
	defer sta.tokenLock.Unlock()
	sta.token = token
	sta.masterToken = masterToken
	sta.sessionID = sessionID
}

func safeGetTokens(sr *snowflakeRestful) (token string, masterToken string, sessionID int64) {
	if sr == nil || sr.TokenAccessor == nil {
		logger.Error("safeGetTokens: could not get tokens as TokenAccessor was nil")
		return "", "", 0
	}
	return sr.TokenAccessor.GetTokens()
}

func escapeForCSV(value string) string {
	if value == "" {
		return "\"\""
	}
	if strings.Contains(value, "\"") || strings.Contains(value, "\n") ||
		strings.Contains(value, ",") || strings.Contains(value, "\\") {
		return "\"" + strings.ReplaceAll(value, "\"", "\"\"") + "\""
	}
	return value
}

// GetFromEnv is used to get the value of an environment variable from the system
func GetFromEnv(name string, failOnMissing bool) (string, error) {
	if value := os.Getenv(name); value != "" {
		return value, nil
	}
	if failOnMissing {
		return "", fmt.Errorf("%v environment variable is not set", name)
	}
	return "", nil
}

type currentTimeProvider interface {
	currentTime() int64
}

type unixTimeProvider struct {
}

func (utp *unixTimeProvider) currentTime() int64 {
	return time.Now().UnixMilli()
}

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

func chooseRandomFromRange(min float64, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

func withLowerKeys[T any](in map[string]T) map[string]T {
	out := make(map[string]T)
	for k, v := range in {
		out[strings.ToLower(k)] = v
	}
	return out
}

func findByPrefix(in []string, prefix string) int {
	for i, v := range in {
		if strings.HasPrefix(v, prefix) {
			return i
		}
	}
	return -1
}
