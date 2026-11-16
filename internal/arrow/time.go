package arrow

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// TimeNanos returns the TIME value at row i of col as nanoseconds since midnight. Snowflake sends TIME
// as an Int64 or Int32 count of 10^-scale seconds since midnight, where scale is the column's
// fractional-second precision.
func TimeNanos(col arrow.Array, i int, scale int) int64 {
	var v int64
	if col.DataType().ID() == arrow.INT64 {
		v = col.(*array.Int64).Value(i)
	} else {
		v = int64(col.(*array.Int32).Value(i))
	}
	return v * int64(math.Pow10(9-scale))
}
