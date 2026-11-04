package arrow

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestTimeNanos(t *testing.T) {
	const wholeSeconds = 13*time.Hour + 45*time.Minute + 30*time.Second
	const withMillis = wholeSeconds + 123*time.Millisecond
	const withNanos = wholeSeconds + 123_456_789*time.Nanosecond

	int64Col := func(v int64) arrow.Array {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		b.Append(v)
		return b.NewArray()
	}
	int32Col := func(v int32) arrow.Array {
		b := array.NewInt32Builder(memory.DefaultAllocator)
		b.Append(v)
		return b.NewArray()
	}

	cases := []struct {
		name  string
		col   arrow.Array
		scale int
		want  time.Duration
	}{
		{"int64, scale 9 (nanoseconds)", int64Col(49_530_123_456_789), 9, withNanos},
		{"int64, scale 3 (milliseconds)", int64Col(49_530_123), 3, withMillis},
		{"int64, scale 0 (seconds)", int64Col(49_530), 0, wholeSeconds},
		{"int32, scale 3 (milliseconds)", int32Col(49_530_123), 3, withMillis},
		{"int32, scale 0 (seconds)", int32Col(49_530), 0, wholeSeconds},
	}
	for _, c := range cases {
		if got := TimeNanos(c.col, 0, c.scale); got != int64(c.want) {
			t.Errorf("%s: got %v, want %v", c.name, time.Duration(got), c.want)
		}
	}
}
