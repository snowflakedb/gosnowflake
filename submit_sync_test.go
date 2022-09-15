package gosnowflake

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
)

func TestSubmitQuerySync(t *testing.T) {
	postMock := func(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string,
		_ []byte, _ time.Duration, _ bool) (*http.Response, error) {
		dd := &execResponseData{}
		er := &execResponse{
			Data:    *dd,
			Message: "",
			Code:    queryInProgressCode,
			Success: true,
		}
		ba, err := json.Marshal(er)
		if err != nil {
			panic(err)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &fakeResponseBody{body: ba},
		}, nil
	}

	sr := &snowflakeRestful{
		FuncPost:            postMock,
		FuncPostQuery:       postRestfulQuery,
		FuncPostQueryHelper: postRestfulQueryHelper,
		TokenAccessor:       getSimpleTokenAccessor(),
	}
	sc := &snowflakeConn{
		cfg: &Config{
			Params: map[string]*string{},
			// Set a long threshold to prevent the monitoring fetch from kicking in.
			MonitoringFetcher: MonitoringFetcherConfig{QueryRuntimeThreshold: 1 * time.Hour},
		},
		rest:      sr,
		telemetry: testTelemetry,
	}

	res, err := sc.SubmitQuerySync(context.TODO(), "")
	if err != nil {
		t.Fatal(err)
	}

	if res.GetStatus() != QueryStatusInProgress {
		t.Errorf("Expected query in progress, got %s", res.GetStatus())
	}
}

func TestSubmitQuerySyncQueryComplete(t *testing.T) {
	postMock := func(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string,
		_ []byte, _ time.Duration, _ bool,
	) (*http.Response, error) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "field", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.NewMetadata([]string{"LOGICALTYPE"}, []string{"int64"})},
		}, &arrow.Metadata{})
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)

		fieldBuilder := builder.Field(0).(*array.Int64Builder)
		fieldBuilder.Append(42)

		rec := builder.NewRecord()

		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
		err := w.Write(rec)
		if err != nil {
			t.Fatal(err)
		}
		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}

		bb := buf.Bytes()

		chunkB64 := base64.StdEncoding.EncodeToString(bb)
		rec.Release()

		dd := &execResponseData{
			RowSetBase64: chunkB64,
			RowType: []execResponseRowType{
				{Name: "field", Type: "int64"},
			},
		}
		er := &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "",
			Success: true,
		}
		ba, err := json.Marshal(er)
		if err != nil {
			panic(err)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(ba)),
		}, nil
	}

	sr := &snowflakeRestful{
		FuncPost:            postMock,
		FuncPostQuery:       postRestfulQuery,
		FuncPostQueryHelper: postRestfulQueryHelper,
		TokenAccessor:       getSimpleTokenAccessor(),
	}
	sc := &snowflakeConn{
		cfg: &Config{
			Params: map[string]*string{},
			// Set a long threshold to prevent the monitoring fetch from kicking in.
			MonitoringFetcher: MonitoringFetcherConfig{QueryRuntimeThreshold: 1 * time.Hour},
		},
		rest:      sr,
		telemetry: testTelemetry,
	}

	res, err := sc.SubmitQuerySync(context.TODO(), "")
	if err != nil {
		t.Fatal(err)
	}

	if res.GetStatus() != QueryStatusComplete {
		t.Errorf("Expected query complete, got %s", res.GetStatus())
	}

	batches, err := res.GetArrowBatches()
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) != 1 {
		t.Fatalf("Expected one batch, got %d", len(batches))
	}

	recs, err := batches[0].Fetch(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(*recs) != 1 {
		t.Fatalf("Expected one record, got %d", len(*recs))
	}
	rec := (*recs)[0]
	if rec.NumCols() != 1 {
		t.Fatalf("Expected one column, got %d", rec.NumCols())
	}
	if rec.NumRows() != 1 {
		t.Fatalf("Expected one row, got %d", rec.NumRows())
	}

	val := rec.Column(0).(*array.Int64).Value(0)
	if val != 42 {
		t.Fatalf("Expected value 42, got %d", val)
	}
}
