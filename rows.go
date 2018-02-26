// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	headerSseCAlgorithm = "x-amz-server-side-encryption-customer-algorithm"
	headerSseCKey       = "x-amz-server-side-encryption-customer-key"
	headerSseCAes       = "AES256"
)

var (
	maxChunkDownloadWorkers        = 10
	maxChunkDownloaderErrorCounter = 5
)

type snowflakeRows struct {
	sc              *snowflakeConn
	RowType         []execResponseRowType
	ChunkDownloader *snowflakeChunkDownloader
}

func (rows *snowflakeRows) Close() (err error) {
	glog.V(2).Infoln("Rows.Close")
	return nil
}

type chunkError struct {
	Index int
	Error error
}

type snowflakeChunkDownloader struct {
	sc                 *snowflakeConn
	ctx                context.Context
	Total              int64
	TotalRowIndex      int64
	CurrentChunk       [][]*string
	CurrentChunkIndex  int
	CurrentChunkSize   int
	ChunksMutex        *sync.Mutex
	ChunkMetas         []execResponseChunk
	Chunks             map[int][][]*string
	ChunksChan         chan int
	ChunksError        chan *chunkError
	ChunksErrorCounter int
	ChunksFinalErrors  []*chunkError
	Qrmk               string
	ChunkHeader        map[string]string
	CurrentIndex       int
	FuncDownload       func(*snowflakeChunkDownloader, int)
	FuncDownloadHelper func(context.Context, *snowflakeChunkDownloader, int)
	FuncGet            func(context.Context, *snowflakeChunkDownloader, string, map[string]string, time.Duration) (*http.Response, error)
}

// ColumnTypeDatabaseTypeName returns the database column name.
func (rows *snowflakeRows) ColumnTypeDatabaseTypeName(index int) string {
	return strings.ToUpper(rows.RowType[index].Type)
}

// ColumnTypeLength returns the length of the column
func (rows *snowflakeRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index > len(rows.RowType) {
		return 0, false
	}
	switch rows.RowType[index].Type {
	case "text", "variant", "object", "array", "binary":
		return rows.RowType[index].Length, true
	}
	return 0, false
}

func (rows *snowflakeRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index > len(rows.RowType) {
		return false, false
	}
	return rows.RowType[index].Nullable, true
}

func (rows *snowflakeRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index > len(rows.RowType) {
		return 0, 0, false
	}
	switch rows.RowType[index].Type {
	case "fixed":
		return rows.RowType[index].Precision, rows.RowType[index].Scale, true
	}
	return 0, 0, false
}

func (rows *snowflakeRows) Columns() []string {
	glog.V(3).Infoln("Rows.Columns")
	ret := make([]string, len(rows.RowType))
	for i, n := 0, len(rows.RowType); i < n; i++ {
		ret[i] = rows.RowType[i].Name
	}
	return ret
}

func (rows *snowflakeRows) ColumnTypeScanType(index int) reflect.Type {
	return snowflakeTypeToGo(rows.RowType[index].Type, rows.RowType[index].Scale)
}

func (rows *snowflakeRows) Next(dest []driver.Value) (err error) {
	row, err := rows.ChunkDownloader.Next()
	if err != nil {
		// includes io.EOF
		if err == io.EOF {
			rows.ChunkDownloader.Chunks = nil // detach all chunks. No way to go backward without reinitialize it.
		}
		return err
	}
	for i, n := 0, len(row); i < n; i++ {
		// could move to chunk downloader so that each go routine
		// can convert data
		err := stringToValue(&dest[i], rows.RowType[i], row[i])
		if err != nil {
			return err
		}
	}
	return err
}

func (rows *snowflakeRows) HasNextResultSet() bool {
	if len(rows.ChunkDownloader.ChunkMetas) == 0 {
		return false // no extra chunk
	}
	return rows.ChunkDownloader.hasNextResultSet()
}

func (rows *snowflakeRows) NextResultSet() error {
	if len(rows.ChunkDownloader.ChunkMetas) == 0 {
		return io.EOF
	}
	return rows.ChunkDownloader.nextResultSet()
}

func (scd *snowflakeChunkDownloader) hasNextResultSet() bool {
	return scd.CurrentChunkIndex < len(scd.ChunkMetas)
}

func (scd *snowflakeChunkDownloader) nextResultSet() error {
	// no error at all times as the next chunk/resultset is automatically read
	if scd.CurrentChunkIndex < len(scd.ChunkMetas) {
		return nil
	}
	return io.EOF
}

func (scd *snowflakeChunkDownloader) start() error {
	scd.CurrentChunkSize = len(scd.CurrentChunk) // cache the size
	scd.CurrentIndex = -1                        // initial chunks idx
	scd.CurrentChunkIndex = -1                   // initial chunk

	// start downloading chunks if exists
	chunkMetaLen := len(scd.ChunkMetas)
	if chunkMetaLen > 0 {
		glog.V(2).Infof("chunks: %v", chunkMetaLen)
		scd.ChunksMutex = &sync.Mutex{}
		scd.Chunks = make(map[int][][]*string)
		scd.ChunksChan = make(chan int, chunkMetaLen)
		scd.ChunksError = make(chan *chunkError, maxChunkDownloadWorkers)
		for i := 0; i < chunkMetaLen; i++ {
			glog.V(2).Infof("add chunk to channel ChunksChan: %v", i+1)
			scd.ChunksChan <- i
		}
		for i := 0; i < intMin(maxChunkDownloadWorkers, chunkMetaLen); i++ {
			scd.schedule()
		}
	}
	return nil
}

func (scd *snowflakeChunkDownloader) schedule() {
	select {
	case nextIdx := <-scd.ChunksChan:
		glog.V(2).Infof("schedule chunk: %v", nextIdx+1)
		go scd.FuncDownload(scd, nextIdx)
	default:
		// no more download
		glog.V(2).Info("no more download")
	}
}

func (scd *snowflakeChunkDownloader) checkErrorRetry() (err error) {
	select {
	case errc := <-scd.ChunksError:
		if scd.ChunksErrorCounter < maxChunkDownloaderErrorCounter && errc.Error != context.Canceled {
			// add the index to the chunks channel so that the download will be retried.
			go scd.FuncDownload(scd, errc.Index)
			scd.ChunksErrorCounter++
			glog.V(2).Infof("chunk idx: %v, err: %v. retrying (%v/%v)...",
				errc.Index, errc.Error, scd.ChunksErrorCounter, maxChunkDownloaderErrorCounter)
		} else {
			scd.ChunksFinalErrors = append(scd.ChunksFinalErrors, errc)
			glog.V(2).Infof("chunk idx: %v, err: %v. no further retry", errc.Index, errc.Error)
			return errc.Error
		}
	default:
		glog.V(2).Info("no error is detected.")
	}
	return nil
}
func (scd *snowflakeChunkDownloader) Next() ([]*string, error) {
	for {
		scd.CurrentIndex++
		if scd.CurrentIndex < scd.CurrentChunkSize {
			return scd.CurrentChunk[scd.CurrentIndex], nil
		}
		scd.CurrentChunkIndex++ // next chunk
		scd.CurrentIndex = -1   // reset
		if scd.CurrentChunkIndex >= len(scd.ChunkMetas) {
			break
		}

		scd.ChunksMutex.Lock()
		if scd.CurrentChunkIndex > 1 {
			scd.Chunks[scd.CurrentChunkIndex-1] = nil // detach the previously used chunk
		}
		scd.ChunksMutex.Unlock()

		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			scd.ChunksMutex.Lock()
			err := scd.checkErrorRetry()
			if err != nil {
				scd.ChunksMutex.Unlock()
				ticker.Stop()
				return nil, err
			}
			glog.V(2).Infof("waiting for chunk idx: %v/%v",
				scd.CurrentChunkIndex+1, len(scd.ChunkMetas))
			scd.CurrentChunk = scd.Chunks[scd.CurrentChunkIndex]
			scd.ChunksMutex.Unlock()
			if scd.CurrentChunk != nil {
				ticker.Stop()
				// kick off the next download
				glog.V(2).Infof("ready: chunk %v", scd.CurrentChunkIndex)
				scd.CurrentChunkSize = len(scd.CurrentChunk)
				scd.schedule()
				break
			}
		}
	}

	glog.V(2).Infof("no more data")
	if len(scd.ChunkMetas) > 0 {
		close(scd.ChunksError)
		close(scd.ChunksChan)
	}
	return nil, io.EOF
}

func getChunk(
	ctx context.Context,
	scd *snowflakeChunkDownloader,
	fullURL string,
	headers map[string]string,
	timeout time.Duration) (
	*http.Response, error) {
	return retryHTTP(
		ctx, scd.sc.rest.Client, http.NewRequest,
		"GET", fullURL, headers, nil, timeout, false)
}

/* largeResultSetReader is a reader that wraps the large result set with leading and tailing brackets. */
type largeResultSetReader struct {
	status int
	body   io.Reader
}

func (r *largeResultSetReader) Read(p []byte) (n int, err error) {
	if r.status == 0 {
		p[0] = 0x5b // initial 0x5b ([)
		r.status = 1
		return 1, nil
	}
	if r.status == 1 {
		var len int
		len, err = r.body.Read(p)
		if err == io.EOF {
			r.status = 2
			return len, nil
		}
		if err != nil {
			return 0, err
		}
		return len, nil
	}
	if r.status == 2 {
		p[0] = 0x5d // tail 0x5d (])
		r.status = 3
		return 1, nil
	}
	// ensure no data and EOF
	return 0, io.EOF
}

func downloadChunk(scd *snowflakeChunkDownloader, idx int) {
	glog.V(2).Infof("download start chunk: %v", idx+1)

	execDownloadChan := make(chan struct{})

	go func() {
		scd.FuncDownloadHelper(scd.ctx, scd, idx)
		close(execDownloadChan)
	}()

	select {
	case <-scd.ctx.Done():
		scd.ChunksError <- &chunkError{Index: idx, Error: scd.ctx.Err()}
	case <-execDownloadChan:
	}
}

func downloadChunkHelper(ctx context.Context, scd *snowflakeChunkDownloader, idx int) {

	headers := make(map[string]string)
	if len(scd.ChunkHeader) > 0 {
		glog.V(2).Info("chunk header is provided.")
		for k, v := range scd.ChunkHeader {
			headers[k] = v
		}
	} else {
		headers[headerSseCAlgorithm] = headerSseCAes
		headers[headerSseCKey] = scd.Qrmk
	}

	resp, err := scd.FuncGet(ctx, scd, scd.ChunkMetas[idx].URL, headers, 0)
	if err != nil {
		scd.ChunksError <- &chunkError{Index: idx, Error: err}
		return
	}
	defer resp.Body.Close()
	glog.V(2).Infof("download finish chunk: %v, resp: %v", idx+1, resp)
	if resp.StatusCode == http.StatusOK {
		var respd [][]*string
		st := &largeResultSetReader{
			status: 0,
			body:   resp.Body,
		}
		dec := json.NewDecoder(st)
		for {
			if err := dec.Decode(&respd); err == io.EOF {
				break
			} else if err != nil {
				glog.V(1).Infof(
					"failed to extract HTTP response body. URL: %v, err: %v", scd.ChunkMetas[idx].URL, err)
				glog.Flush()
				scd.ChunksError <- &chunkError{Index: idx, Error: err}
				return
			}
		}
		scd.ChunksMutex.Lock()
		scd.Chunks[idx] = respd
		scd.ChunksMutex.Unlock()
	} else {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			glog.V(1).Infof(
				"failed to extract HTTP response body. URL: %v, err: %v", scd.ChunkMetas[idx].URL, err)
			glog.Flush()
			scd.ChunksError <- &chunkError{Index: idx, Error: err}
			return
		}
		glog.V(1).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, scd.ChunkMetas[idx].URL, b)
		glog.V(1).Infof("Header: %v", resp.Header)
		glog.Flush()
		scd.ChunksError <- &chunkError{
			Index: idx,
			Error: &SnowflakeError{
				Number:      ErrFailedToGetChunk,
				SQLState:    SQLStateConnectionFailure,
				Message:     errMsgFailedToGetChunk,
				MessageArgs: []interface{}{idx},
			}}
	}
}
