package arrowbatches

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

// wantInts / wantStrs are the fixture values shared by the tests below.
var (
	fixtureInts = []int64{1, 2, 3}
	fixtureStrs = []string{"alpha", "beta", "gamma"}
)

// buildFixtureIPC builds a raw (Snowflake-shaped) Arrow record with a "fixed" Int64
// column and a "text" String column, and returns it encoded as Arrow IPC stream bytes
// plus the matching row-type metadata. All Arrow memory is released before returning;
// the returned bytes are plain Go bytes.
func buildFixtureIPC(t *testing.T) ([]byte, []query.ExecResponseRowType) {
	t.Helper()
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	intB := array.NewInt64Builder(pool)
	defer intB.Release()
	intB.AppendValues(fixtureInts, nil)
	intArr := intB.NewArray()
	defer intArr.Release()

	strB := array.NewStringBuilder(pool)
	defer strB.Release()
	strB.AppendValues(fixtureStrs, nil)
	strArr := strB.NewArray()
	defer strArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: &arrow.Int64Type{}},
		{Name: "s", Type: &arrow.StringType{}},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{intArr, strArr}, int64(len(fixtureInts)))
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(pool))
	if err := w.Write(rec); err != nil {
		t.Fatalf("writing IPC record: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing IPC writer: %v", err)
	}

	rowTypes := []query.ExecResponseRowType{
		{Name: "n", Type: "fixed", Scale: 0, Precision: 18, Nullable: true},
		{Name: "s", Type: "text", Length: 16, Nullable: true},
	}
	return buf.Bytes(), rowTypes
}

func gzipBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(in); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// assertFixture fetches the batch, verifies the values against the fixture, and releases
// the returned records.
func assertFixture(t *testing.T, batch *ArrowBatch) {
	t.Helper()
	records, err := batch.Fetch()
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var got int
	for _, rec := range *records {
		ints := rec.Column(0).(*array.Int64).Int64Values()
		strs := rec.Column(1).(*array.String)
		for i := range ints {
			if ints[i] != fixtureInts[got] {
				t.Fatalf("row %d: int = %d, want %d", got, ints[i], fixtureInts[got])
			}
			if strs.Value(i) != fixtureStrs[got] {
				t.Fatalf("row %d: str = %q, want %q", got, strs.Value(i), fixtureStrs[got])
			}
			got++
		}
		rec.Release()
	}
	if got != len(fixtureInts) {
		t.Fatalf("got %d rows, want %d", got, len(fixtureInts))
	}
}

func TestSerializableArrowBatchInline(t *testing.T) {
	ipcBytes, rowTypes := buildFixtureIPC(t)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	s := SerializableArrowBatch{
		Format:       arrowFormatName,
		InlineData:   base64.StdEncoding.EncodeToString(ipcBytes),
		RowCount:     len(fixtureInts),
		RowTypes:     rowTypes,
		TimezoneName: "UTC",
	}
	// Inline batch needs no HTTP client.
	batch, err := s.ToArrowBatch(WithAllocator(pool))
	if err != nil {
		t.Fatalf("ToArrowBatch: %v", err)
	}
	assertFixture(t, batch)
}

func TestSerializableArrowBatchRemote(t *testing.T) {
	ipcBytes, rowTypes := buildFixtureIPC(t)

	for _, tc := range []struct {
		name    string
		payload []byte
		gzipped bool
	}{
		{name: "plain", payload: ipcBytes},
		{name: "gzip", payload: gzipBytes(t, ipcBytes), gzipped: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotHeaders http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHeaders = r.Header.Clone()
				_, _ = w.Write(tc.payload)
			}))
			defer srv.Close()

			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			s := SerializableArrowBatch{
				Format:   arrowFormatName,
				URL:      srv.URL + "/chunk",
				Headers:  map[string]string{"x-amz-server-side-encryption-customer-key": "test-qrmk"},
				RowCount: len(fixtureInts),
				RowTypes: rowTypes,
			}
			batch, err := s.ToArrowBatch(WithHTTPClient(srv.Client()), WithAllocator(pool))
			if err != nil {
				t.Fatalf("ToArrowBatch: %v", err)
			}
			assertFixture(t, batch)

			if gotHeaders.Get("x-amz-server-side-encryption-customer-key") != "test-qrmk" {
				t.Fatalf("SSE-C header not forwarded to storage; got %v", gotHeaders)
			}
		})
	}
}

func TestSerializableArrowBatchRemoteDefaultsHTTPClient(t *testing.T) {
	_, rowTypes := buildFixtureIPC(t)
	s := SerializableArrowBatch{
		Format:   arrowFormatName,
		URL:      "https://example.invalid/chunk",
		RowCount: len(fixtureInts),
		RowTypes: rowTypes,
	}
	// No WithHTTPClient: a remote batch is reconstructed successfully using http.DefaultClient.
	batch, err := s.ToArrowBatch()
	if err != nil {
		t.Fatalf("ToArrowBatch without a client should succeed (defaults to http.DefaultClient): %v", err)
	}
	if batch == nil {
		t.Fatal("expected a non-nil batch")
	}
}

func TestSerializableArrowBatchExpiredURL(t *testing.T) {
	_, rowTypes := buildFixtureIPC(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "AccessDenied: Request has expired", http.StatusForbidden)
	}))
	defer srv.Close()

	s := SerializableArrowBatch{
		Format:   arrowFormatName,
		URL:      srv.URL + "/chunk",
		RowCount: len(fixtureInts),
		RowTypes: rowTypes,
	}
	batch, err := s.ToArrowBatch(WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("ToArrowBatch: %v", err)
	}
	if _, err := batch.Fetch(); err == nil {
		t.Fatal("expected an error fetching an expired (403) URL")
	}
}

func TestSerializableArrowBatchUnsupportedFormat(t *testing.T) {
	s := SerializableArrowBatch{Format: "json", RowCount: 1}
	if _, err := s.ToArrowBatch(); err == nil {
		t.Fatal("expected error for non-arrow format")
	}
}

func TestSerializableArrowBatchJSONRoundTrip(t *testing.T) {
	ipcBytes, rowTypes := buildFixtureIPC(t)
	// A nested/structured row type to confirm recursive Fields survive JSON.
	rowTypes = append(rowTypes, query.ExecResponseRowType{
		Name: "obj", Type: "object", Nullable: true,
		Fields: []query.FieldMetadata{
			{Name: "inner", Type: "text", Nullable: true},
		},
	})

	orig := SerializableArrowBatch{
		Format:                arrowFormatName,
		InlineData:            base64.StdEncoding.EncodeToString(ipcBytes),
		URL:                   "https://example.invalid/chunk",
		Headers:               map[string]string{"h": "v"},
		RowCount:              len(fixtureInts),
		UncompressedSize:      4096,
		RowTypes:              rowTypes,
		TimezoneName:          "America/New_York",
		TimezoneOffsetSeconds: -5 * 3600,
		ConversionOptions: ConversionOptions{
			HigherPrecision: true,
			TimestampOption: UseMillisecondTimestamp,
			Utf8Validation:  true,
		},
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got SerializableArrowBatch
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.InlineData != orig.InlineData {
		t.Fatal("InlineData did not round-trip")
	}
	if got.TimestampOption != orig.TimestampOption || got.HigherPrecision != orig.HigherPrecision || got.Utf8Validation != orig.Utf8Validation {
		t.Fatalf("conversion flags did not round-trip: %+v", got)
	}
	if len(got.RowTypes) != 3 || len(got.RowTypes[2].Fields) != 1 || got.RowTypes[2].Fields[0].Name != "inner" {
		t.Fatalf("nested RowTypes did not round-trip: %+v", got.RowTypes)
	}
	if got.TimezoneName != "America/New_York" || got.TimezoneOffsetSeconds != -5*3600 {
		t.Fatalf("timezone did not round-trip: %q %d", got.TimezoneName, got.TimezoneOffsetSeconds)
	}
}

func TestLoadLocation(t *testing.T) {
	// Fixed-offset zone (as produced by gosnowflake.Location) — LoadLocation would reject
	// the name, so we must fall back to the stored offset.
	loc := loadLocation("+0300", 3*3600)
	if _, off := time.Now().In(loc).Zone(); off != 3*3600 {
		t.Fatalf("fixed-offset zone: got offset %d, want %d", off, 3*3600)
	}

	// "Local" must not resolve to the worker's local zone; it uses the stored offset.
	loc = loadLocation("Local", -7*3600)
	if _, off := time.Now().In(loc).Zone(); off != -7*3600 {
		t.Fatalf("Local zone: got offset %d, want %d", off, -7*3600)
	}

	// A named IANA zone resolves via tzdata when available.
	loc = loadLocation("UTC", 0)
	if got := loc.String(); got != "UTC" {
		t.Fatalf("named zone: got %q, want UTC", got)
	}

	// Empty name falls back to a fixed zone from the offset.
	loc = loadLocation("", 0)
	if _, off := time.Now().In(loc).Zone(); off != 0 {
		t.Fatalf("empty zone: got offset %d, want 0", off)
	}
}

// TestToSerializableRoundTripInline verifies ToSerializable -> ToArrowBatch for an inline
// batch built directly (exercising both directions offline, without a live connection).
func TestToSerializableRoundTripInline(t *testing.T) {
	ipcBytes, rowTypes := buildFixtureIPC(t)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	inlineB64 := base64.StdEncoding.EncodeToString(ipcBytes)
	src := SerializableArrowBatch{
		Format:       arrowFormatName,
		InlineData:   inlineB64,
		RowCount:     len(fixtureInts),
		RowTypes:     rowTypes,
		TimezoneName: "UTC",
	}
	batch, err := src.ToArrowBatch(WithAllocator(pool))
	if err != nil {
		t.Fatalf("ToArrowBatch: %v", err)
	}
	// Re-serialize the reconstructed batch (before Fetch) and confirm the descriptor survives.
	round, err := batch.ToSerializable()
	if err != nil {
		t.Fatalf("ToSerializable: %v", err)
	}
	if round.InlineData != inlineB64 || round.RowCount != len(fixtureInts) {
		t.Fatalf("re-serialized descriptor mismatch: %+v", round)
	}
	batch2, err := round.ToArrowBatch(WithAllocator(pool))
	if err != nil {
		t.Fatalf("ToArrowBatch (round): %v", err)
	}
	assertFixture(t, batch2)
}

// buildDecimalIPC builds a raw Decimal128 column (as Snowflake sends for NUMBER(_,1))
// encoded as Arrow IPC bytes, plus its scale-1 row-type metadata.
func buildDecimalIPC(t *testing.T) ([]byte, []query.ExecResponseRowType) {
	t.Helper()
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	dt := &arrow.Decimal128Type{Precision: 10, Scale: 1}
	b := array.NewDecimal128Builder(pool, dt)
	defer b.Release()
	b.Append(decimal128.FromU64(1234)) // 123.4 at scale 1
	arr := b.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: dt}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(pool))
	if err := w.Write(rec); err != nil {
		t.Fatalf("write ipc: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close ipc: %v", err)
	}
	return buf.Bytes(), []query.ExecResponseRowType{{Name: "n", Type: "fixed", Scale: 1, Precision: 10, Nullable: true}}
}

// TestSerializableArrowBatchAppliesConversionOptions proves that the ConversionOptions
// carried by a serialized batch are actually applied when it is reconstructed and fetched —
// not merely round-tripped as data. HigherPrecision keeps a scale!=0 NUMBER as Decimal128;
// without it the value is converted to Float64. This guards against a regression where the
// reconstructed batch silently converts with default options.
func TestSerializableArrowBatchAppliesConversionOptions(t *testing.T) {
	ipcBytes, rowTypes := buildDecimalIPC(t)

	for _, tc := range []struct {
		name            string
		higherPrecision bool
		wantType        arrow.Type
	}{
		{name: "higher precision keeps decimal", higherPrecision: true, wantType: arrow.DECIMAL128},
		{name: "no higher precision casts to float64", higherPrecision: false, wantType: arrow.FLOAT64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			s := SerializableArrowBatch{
				Format:            arrowFormatName,
				InlineData:        base64.StdEncoding.EncodeToString(ipcBytes),
				RowCount:          1,
				RowTypes:          rowTypes,
				ConversionOptions: ConversionOptions{HigherPrecision: tc.higherPrecision},
			}
			batch, err := s.ToArrowBatch(WithAllocator(pool))
			if err != nil {
				t.Fatalf("ToArrowBatch: %v", err)
			}
			records, err := batch.Fetch()
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if len(*records) == 0 {
				t.Fatal("expected at least one record")
			}
			if got := (*records)[0].Column(0).DataType().ID(); got != tc.wantType {
				t.Fatalf("column type = %v, want %v", got, tc.wantType)
			}
			for _, rec := range *records {
				rec.Release()
			}
		})
	}
}
