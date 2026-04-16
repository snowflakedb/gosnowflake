package gosnowflake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	sferrors "github.com/snowflakedb/gosnowflake/v2/internal/errors"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

// ArrowStreamLoader is a convenience interface for downloading
// Snowflake results via multiple Arrow Record Batch streams.
//
// Some queries from Snowflake do not return Arrow data regardless
// of the settings, such as "SHOW WAREHOUSES" or SQL stored procedures
// using CALL with RETURNS TABLE(). In these cases the server returns
// JSON even though the client requested Arrow. Small JSON results are
// accessible via JSONData(); column metadata is available via RowTypes().
//
// To determine the actual response format, use a type assertion against
// QueryResultFormatProvider.
type ArrowStreamLoader interface {
	GetBatches() ([]ArrowStreamBatch, error)
	NextResultSet(ctx context.Context) error
	TotalRows() int64
	RowTypes() []query.ExecResponseRowType
	Location() *time.Location
	JSONData() [][]*string
}

// QueryResultFormatProvider is an optional interface that an
// ArrowStreamLoader may implement to expose the server-reported result
// format. The returned value is typically "arrow" or "json".
//
//	if p, ok := loader.(gosnowflake.QueryResultFormatProvider); ok {
//	    fmt.Println(p.QueryResultFormat())
//	}
type QueryResultFormatProvider interface {
	QueryResultFormat() string
}

// ArrowStreamBatch is a type describing a potentially yet-to-be-downloaded
// chunk of query result data. The content format depends on the current
// QueryResultFormat: Arrow IPC record batches (use ipc.NewReader) when
// the format is "arrow", or JSON (row fragments) when it is "json".
type ArrowStreamBatch struct {
	idx     int
	numrows int64
	scd     *snowflakeArrowStreamChunkDownloader
	Loc     *time.Location
	rr      io.ReadCloser
}

// NumRows returns the total number of rows that the metadata stated should
// be in this stream of record batches.
func (asb *ArrowStreamBatch) NumRows() int64 { return asb.numrows }

// GetStream downloads the chunk (if not already cached) and returns a
// stream of bytes. The content may be Arrow IPC or JSON (row fragments)
// depending on the current QueryResultFormat. Close should be called
// on the returned stream when done to ensure no leaked memory.
func (asb *ArrowStreamBatch) GetStream(ctx context.Context) (io.ReadCloser, error) {
	if asb.rr == nil {
		if err := asb.downloadChunkStreamHelper(ctx); err != nil {
			return nil, err
		}
	}
	return asb.rr, nil
}

// streamWrapReader wraps an io.Reader so that Close closes the underlying body.
type streamWrapReader struct {
	io.Reader
	wrapped io.ReadCloser
}

func (w *streamWrapReader) Close() error {
	if cl, ok := w.Reader.(io.ReadCloser); ok {
		if err := cl.Close(); err != nil {
			return err
		}
	}
	return w.wrapped.Close()
}

func (asb *ArrowStreamBatch) downloadChunkStreamHelper(ctx context.Context) error {
	headers := make(map[string]string)
	if len(asb.scd.ChunkHeader) > 0 {
		maps.Copy(headers, asb.scd.ChunkHeader)
	} else {
		headers[headerSseCAlgorithm] = headerSseCAes
		headers[headerSseCKey] = asb.scd.Qrmk
	}

	resp, err := asb.scd.FuncGet(ctx, asb.scd.sc, asb.scd.ChunkMetas[asb.idx].URL, headers, asb.scd.sc.rest.RequestTimeout)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		defer func() {
			_ = resp.Body.Close()
		}()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		_ = b
		return &SnowflakeError{
			Number:      ErrFailedToGetChunk,
			SQLState:    SQLStateConnectionFailure,
			Message:     fmt.Sprintf("failed to get chunk. idx: %v", asb.idx),
			MessageArgs: []any{asb.idx},
		}
	}

	defer func() {
		if asb.rr == nil {
			_ = resp.Body.Close()
		}
	}()

	bufStream := bufio.NewReader(resp.Body)
	gzipMagic, err := bufStream.Peek(2)
	if err != nil {
		return err
	}

	if gzipMagic[0] == 0x1f && gzipMagic[1] == 0x8b {
		bufStream0, err := gzip.NewReader(bufStream)
		if err != nil {
			return err
		}
		asb.rr = &streamWrapReader{Reader: bufStream0, wrapped: resp.Body}
	} else {
		asb.rr = &streamWrapReader{Reader: bufStream, wrapped: resp.Body}
	}
	return nil
}

type snowflakeArrowStreamChunkDownloader struct {
	sc                 *snowflakeConn
	ChunkMetas         []query.ExecResponseChunk
	Total              int64
	Qrmk               string
	ChunkHeader        map[string]string
	FuncGet            func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error)
	RowSet             rowSetType
	resultIDs          []string
	queryResultFormat  string
	formatAcknowledged bool
}

func (scd *snowflakeArrowStreamChunkDownloader) Location() *time.Location {
	if scd.sc != nil {
		return getCurrentLocation(&scd.sc.syncParams)
	}
	return nil
}

func (scd *snowflakeArrowStreamChunkDownloader) TotalRows() int64 { return scd.Total }

func (scd *snowflakeArrowStreamChunkDownloader) RowTypes() []query.ExecResponseRowType {
	return scd.RowSet.RowType
}

func (scd *snowflakeArrowStreamChunkDownloader) JSONData() [][]*string {
	return scd.RowSet.JSON
}

// QueryResultFormat returns the server-reported result format for the
// current result set (typically "arrow" or "json"). Calling this method
// acknowledges that the caller is aware of the format and will handle
// non-Arrow data appropriately. Without this acknowledgment, GetBatches
// returns ErrNonArrowResponseInArrowBatches specifically when all of the
// following are true: the result format is not Arrow, no inline JSON
// rows are present (i.e. JSONData() is empty), and there are chunked
// batches to download — which is the case for large result sets from
// SQL/JavaScript stored procedures.
//
// The acknowledgment is reset on each NextResultSet call, so callers
// handling multi-statement queries must re-check the format after
// advancing to a new result set.
func (scd *snowflakeArrowStreamChunkDownloader) QueryResultFormat() string {
	scd.formatAcknowledged = true
	return scd.queryResultFormat
}

func (scd *snowflakeArrowStreamChunkDownloader) maybeFirstBatch() ([]byte, error) {
	if scd.RowSet.RowSetBase64 == "" {
		return nil, nil
	}

	rowSetBytes, err := base64.StdEncoding.DecodeString(scd.RowSet.RowSetBase64)
	if err != nil {
		logger.Warnf("skipping first batch as it is not a valid base64 response. %v", err)
		return nil, err
	}

	if resultFormat(scd.queryResultFormat) == arrowFormat {
		rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes))
		if err != nil {
			return nil, fmt.Errorf("first batch is not a valid Arrow IPC stream: %w", err)
		}
		rr.Release()
	}

	return rowSetBytes, nil
}

func (scd *snowflakeArrowStreamChunkDownloader) GetBatches() (out []ArrowStreamBatch, err error) {
	chunkMetaLen := len(scd.ChunkMetas)
	loc := scd.Location()

	out = make([]ArrowStreamBatch, chunkMetaLen, chunkMetaLen+1)
	toFill := out
	rowSetBytes, err := scd.maybeFirstBatch()
	if err != nil {
		return nil, err
	}
	if len(rowSetBytes) > 0 {
		out = out[:chunkMetaLen+1]
		out[0] = ArrowStreamBatch{
			scd: scd,
			Loc: loc,
			rr:  io.NopCloser(bytes.NewReader(rowSetBytes)),
		}
		toFill = out[1:]
	}

	var totalCounted int64
	for i := range toFill {
		toFill[i] = ArrowStreamBatch{
			idx:     i,
			numrows: int64(scd.ChunkMetas[i].RowCount),
			Loc:     loc,
			scd:     scd,
		}
		totalCounted += int64(scd.ChunkMetas[i].RowCount)
	}

	if len(rowSetBytes) > 0 {
		out[0].numrows = scd.Total - totalCounted
	}

	// Result is JSON, there's no inline rowset in the response, but there are batches (large resultset from SQL & JS stored procedure)
	if resultFormat(scd.queryResultFormat) != arrowFormat && len(scd.RowSet.JSON) == 0 && len(out) > 0 {
		// If the caller did not explicitly acknowledged it knows about the (JSON) result format (through calling QueryResultFormat())
		// we can assume they'll try to parse it as Arrow, which will crash. Let's error out gracefully.
		if !scd.formatAcknowledged {
			return nil, &SnowflakeError{
				Number:  ErrNonArrowResponseInArrowBatches,
				Message: sferrors.ErrMsgNonArrowResponseInArrowBatches,
			}
		}
		logger.Debugf("GetBatches: returning %d JSON batch(es)", len(out))
	}
	return
}

func (scd *snowflakeArrowStreamChunkDownloader) NextResultSet(ctx context.Context) error {
	if !scd.hasNextResultSet() {
		return io.EOF
	}
	resultID := scd.resultIDs[0]
	scd.resultIDs = scd.resultIDs[1:]
	resultPath := fmt.Sprintf(urlQueriesResultFmt, resultID)
	resp, err := scd.sc.getQueryResultResp(ctx, resultPath)
	if err != nil {
		return err
	}
	if !resp.Success {
		code, err := strconv.Atoi(resp.Code)
		if err != nil {
			logger.WithContext(ctx).Errorf("error while parsing code: %v", err)
		}
		return exceptionTelemetry(&SnowflakeError{
			Number:   code,
			SQLState: resp.Data.SQLState,
			Message:  resp.Message,
			QueryID:  resp.Data.QueryID,
		}, scd.sc)
	}
	scd.ChunkMetas = resp.Data.Chunks
	scd.Total = resp.Data.Total
	scd.Qrmk = resp.Data.Qrmk
	scd.ChunkHeader = resp.Data.ChunkHeaders
	scd.queryResultFormat = resp.Data.QueryResultFormat
	scd.formatAcknowledged = false
	scd.RowSet = rowSetType{
		RowType:      resp.Data.RowType,
		JSON:         resp.Data.RowSet,
		RowSetBase64: resp.Data.RowSetBase64,
	}
	return nil
}

func (scd *snowflakeArrowStreamChunkDownloader) hasNextResultSet() bool {
	return len(scd.resultIDs) > 0
}
