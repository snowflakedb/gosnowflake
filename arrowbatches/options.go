package arrowbatches

import (
	"context"

	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
)

// ConversionOptions bundles the options that control how raw Snowflake Arrow data is
// converted into standard Arrow records (see ArrowBatch.Fetch). It is the single source of
// truth for these options: add a new option here and thread it through the conversion
// functions, and everything that carries options forward — including the batch itself —
// gets it without further changes.
type ConversionOptions struct {
	// TimestampOption controls how Snowflake timestamps are converted (see WithTimestampOption).
	TimestampOption ia.TimestampOption
	// HigherPrecision preserves BigDecimal values instead of converting to int64/float64
	// (see WithHigherPrecision).
	HigherPrecision bool
	// Utf8Validation replaces invalid UTF-8 in string columns with the replacement
	// character (see WithUtf8Validation).
	Utf8Validation bool
}

// conversionOptionsFromContext snapshots the conversion options set on ctx via the public
// With* option functions. This is the only place the arrow-batch conversion context keys are
// read, so the option set cannot drift across the code base.
func conversionOptionsFromContext(ctx context.Context) ConversionOptions {
	return ConversionOptions{
		TimestampOption: ia.GetTimestampOption(ctx),
		HigherPrecision: ia.HigherPrecisionEnabled(ctx),
		Utf8Validation:  ia.Utf8ValidationEnabled(ctx),
	}
}
