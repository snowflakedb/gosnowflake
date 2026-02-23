package arrowbatches

import (
	"context"

	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
)

// Timestamp option constants.
const (
	UseNanosecondTimestamp  = ia.UseNanosecondTimestamp
	UseMicrosecondTimestamp = ia.UseMicrosecondTimestamp
	UseMillisecondTimestamp = ia.UseMillisecondTimestamp
	UseSecondTimestamp      = ia.UseSecondTimestamp
	UseOriginalTimestamp    = ia.UseOriginalTimestamp
)

// WithArrowBatches returns a context that enables arrow batch mode for queries.
func WithArrowBatches(ctx context.Context) context.Context {
	return ia.EnableArrowBatches(ctx)
}

// WithTimestampOption returns a context that sets the timestamp conversion option
// for arrow batches.
func WithTimestampOption(ctx context.Context, option ia.TimestampOption) context.Context {
	return ia.WithTimestampOption(ctx, option)
}

// WithUtf8Validation returns a context that enables UTF-8 validation for
// string columns in arrow batches.
func WithUtf8Validation(ctx context.Context) context.Context {
	return ia.EnableUtf8Validation(ctx)
}
