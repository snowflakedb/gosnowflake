package gosnowflake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

// ArrowStreamLoader is a convenience interface for downloading
// Snowflake results via multiple Arrow Record Batch streams.
//
// Some queries from Snowflake do not return Arrow data regardless
// of the settings, such as "SHOW WAREHOUSES". In these cases,
// you'll find TotalRows() > 0 but GetBatches returns no batches
// and no errors. In this case, the data is accessible via JSONData
// with the actual types matching up to the metadata in RowTypes.
type ArrowStreamLoader interface {
	GetBatches() ([]ArrowStreamBatch, error)
	NextResultSet(ctx context.Context) error
	TotalRows() int64
	RowTypes() []query.ExecResponseRowType
	Location() *time.Location
	JSONData() [][]*string
}

// ArrowStreamBatch is a type describing a potentially yet-to-be-downloaded
// Arrow IPC stream. Call GetStream to download and retrieve an io.Reader
// that can be used with ipc.NewReader to get record batch results.
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

// GetStream returns a stream of bytes consisting of an Arrow IPC Record
// batch stream. Close should be called on the returned stream when done
// to ensure no leaked memory.
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
		for k, v := range asb.scd.ChunkHeader {
			headers[k] = v
		}
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
	sc          *snowflakeConn
	ChunkMetas  []query.ExecResponseChunk
	Total       int64
	Qrmk        string
	ChunkHeader map[string]string
	FuncGet     func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error)
	RowSet      rowSetType
	resultIDs   []string
}

func (scd *snowflakeArrowStreamChunkDownloader) Location() *time.Location {
	if scd.sc != nil && scd.sc.cfg != nil {
		return getCurrentLocation(scd.sc.cfg.Params)
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

func (scd *snowflakeArrowStreamChunkDownloader) maybeFirstBatch() ([]byte, error) {
	if scd.RowSet.RowSetBase64 == "" {
		return nil, nil
	}

	rowSetBytes, err := base64.StdEncoding.DecodeString(scd.RowSet.RowSetBase64)
	if err != nil {
		logger.Warnf("skipping first batch as it is not a valid base64 response. %v", err)
		return nil, err
	}

	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes))
	if err != nil {
		logger.Warnf("skipping first batch as it is not a valid IPC stream. %v", err)
		return nil, err
	}
	rr.Release()

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
		return (&SnowflakeError{
			Number:   code,
			SQLState: resp.Data.SQLState,
			Message:  resp.Message,
			QueryID:  resp.Data.QueryID,
		}).exceptionTelemetry(scd.sc)
	}
	scd.ChunkMetas = resp.Data.Chunks
	scd.Total = resp.Data.Total
	scd.Qrmk = resp.Data.Qrmk
	scd.ChunkHeader = resp.Data.ChunkHeaders
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
