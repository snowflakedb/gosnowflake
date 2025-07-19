//go:build !nobatch
// +build !nobatch

package gosnowflake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// QueryArrowStream returns batches which can be queried for their raw arrow
// ipc stream of bytes. This way consumers don't need to be using the exact
// same version of Arrow as the connection is using internally in order
// to consume Arrow data.
func (sc *snowflakeConn) QueryArrowStream(ctx context.Context, query string, bindings ...driver.NamedValue) (ArrowStreamLoader, error) {
	ctx = WithArrowBatches(context.WithValue(ctx, asyncMode, false))
	ctx = setResultType(ctx, queryResultType)
	isDesc := isDescribeOnly(ctx)
	isInternal := isInternal(ctx)
	data, err := sc.exec(ctx, query, false, isInternal, isDesc, bindings)
	if err != nil {
		logger.WithContext(ctx).Errorf("error: %v", err)
		if data != nil {
			code, e := strconv.Atoi(data.Code)
			if e != nil {
				return nil, e
			}
			return nil, (&SnowflakeError{
				Number:   code,
				SQLState: data.Data.SQLState,
				Message:  err.Error(),
				QueryID:  data.Data.QueryID,
			}).exceptionTelemetry(sc)
		}
		return nil, err
	}

	return &snowflakeArrowStreamChunkDownloader{
		sc:          sc,
		ChunkMetas:  data.Data.Chunks,
		Total:       data.Data.Total,
		Qrmk:        data.Data.Qrmk,
		ChunkHeader: data.Data.ChunkHeaders,
		FuncGet:     getChunk,
		RowSet: rowSetType{
			RowType:      data.Data.RowType,
			JSON:         data.Data.RowSet,
			RowSetBase64: data.Data.RowSetBase64,
		},
	}, nil
}

// ArrowStreamBatch is a type describing a potentially yet-to-be-downloaded
// Arrow IPC stream. Call `GetStream` to download and retrieve an io.Reader
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

func (asb *ArrowStreamBatch) downloadChunkStreamHelper(ctx context.Context) error {
	headers := make(map[string]string)
	if len(asb.scd.ChunkHeader) > 0 {
		logger.WithContext(ctx).Debug("chunk header is provided")
		for k, v := range asb.scd.ChunkHeader {
			logger.Debugf("adding header: %v, value: %v", k, v)

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
	logger.WithContext(ctx).Debugf("response returned chunk: %v for URL: %v", asb.idx+1, asb.scd.ChunkMetas[asb.idx].URL)
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, asb.scd.ChunkMetas[asb.idx].URL, b)
		logger.WithContext(ctx).Infof("Header: %v", resp.Header)
		return &SnowflakeError{
			Number:      ErrFailedToGetChunk,
			SQLState:    SQLStateConnectionFailure,
			Message:     errMsgFailedToGetChunk,
			MessageArgs: []interface{}{asb.idx},
		}
	}

	defer func() {
		if asb.rr == nil {
			resp.Body.Close()
		}
	}()

	bufStream := bufio.NewReader(resp.Body)
	gzipMagic, err := bufStream.Peek(2)
	if err != nil {
		return err
	}

	if gzipMagic[0] == 0x1f && gzipMagic[1] == 0x8b {
		// detect and uncompress gzip
		bufStream0, err := gzip.NewReader(bufStream)
		if err != nil {
			return err
		}
		// gzip.Reader.Close() does NOT close the underlying
		// reader, so we need to wrap it and ensure close will
		// close the response body. Otherwise we'll leak it.
		asb.rr = &wrapReader{Reader: bufStream0, wrapped: resp.Body}
	} else {
		asb.rr = &wrapReader{Reader: bufStream, wrapped: resp.Body}
	}
	return nil
}

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
	TotalRows() int64
	RowTypes() []execResponseRowType
	Location() *time.Location
	JSONData() [][]*string
}

type snowflakeArrowStreamChunkDownloader struct {
	sc          *snowflakeConn
	ChunkMetas  []execResponseChunk
	Total       int64
	Qrmk        string
	ChunkHeader map[string]string
	FuncGet     func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error)
	RowSet      rowSetType
}

func (scd *snowflakeArrowStreamChunkDownloader) Location() *time.Location {
	if scd.sc != nil && scd.sc.cfg != nil {
		return getCurrentLocation(scd.sc.cfg.Params)
	}
	return nil
}
func (scd *snowflakeArrowStreamChunkDownloader) TotalRows() int64 { return scd.Total }
func (scd *snowflakeArrowStreamChunkDownloader) RowTypes() []execResponseRowType {
	return scd.RowSet.RowType
}
func (scd *snowflakeArrowStreamChunkDownloader) JSONData() [][]*string {
	return scd.RowSet.JSON
}

// the server might have had an empty first batch, check if we can decode
// that first batch, if not we skip it.
func (scd *snowflakeArrowStreamChunkDownloader) maybeFirstBatch() ([]byte, error) {
	if scd.RowSet.RowSetBase64 == "" {
		return nil, nil
	}

	// first batch
	rowSetBytes, err := base64.StdEncoding.DecodeString(scd.RowSet.RowSetBase64)
	if err != nil {
		// match logic in buildFirstArrowChunk
		// assume there's no first chunk if we can't decode the base64 string
		logger.Warnf("skipping first batch as it is not a valid base64 response. %v", err)
		return nil, err
	}

	// verify it's a valid ipc stream, otherwise skip it
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
	// if there was no first batch in the response from the server,
	// skip it and move on. toFill == out
	// otherwise expand out by one to account for the first batch
	// and fill it in. have toFill refer to the slice of out excluding
	// the first batch.
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
		logger.Debugf("batch %v, numrows: %v", i, toFill[i].numrows)
		totalCounted += int64(scd.ChunkMetas[i].RowCount)
	}

	if len(rowSetBytes) > 0 {
		// if we had a first batch, fill in the numrows
		out[0].numrows = scd.Total - totalCounted
		logger.Debugf("first batch, numrows: %v", out[0].numrows)
	}
	return
}
