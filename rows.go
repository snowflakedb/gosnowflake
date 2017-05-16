// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
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

	"github.com/golang/glog"
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
	CurrentIndex       int
	FuncDownload       func(*snowflakeChunkDownloader, int)
}

// ColumnTypeDatabaseTypeName returns the database column name.
func (rows *snowflakeRows) ColumnTypeDatabaseTypeName(index int) string {
	// TODO: is this canonical name or can be Snowflake specific name?
	return strings.ToUpper(rows.RowType[index].Type)
}

// ColumnTypeLength returns the length of the column
func (rows *snowflakeRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index > len(rows.RowType) {
		return -1, false
	}
	switch rows.RowType[index].Type {
	case "text", "variant", "object", "array", "binary":
		return rows.RowType[index].Length, true
	}
	return -1, false
}

func (rows *snowflakeRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index > len(rows.RowType) {
		return false, false
	}
	return rows.RowType[index].Nullable, true
}

func (rows *snowflakeRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index > len(rows.RowType) {
		return -1, -1, false
	}
	switch rows.RowType[index].Type {
	case "fixed":
		return rows.RowType[index].Precision, rows.RowType[index].Scale, true
	}
	return -1, -1, false
}

func (rows *snowflakeRows) Columns() []string {
	glog.V(2).Infoln("Rows.Columns")
	ret := make([]string, len(rows.RowType))
	for i, n := 0, len(rows.RowType); i < n; i++ {
		ret[i] = rows.RowType[i].Name
	}
	return ret
}

func (rows *snowflakeRows) ColumnTypeScanType(index int) reflect.Type {
	// TODO: implement this.
	return nil
}

func (rows *snowflakeRows) Next(dest []driver.Value) (err error) {
	row, err := rows.ChunkDownloader.Next()
	if err != nil {
		// includes io.EOF
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

func (scd *snowflakeChunkDownloader) start() error {
	scd.CurrentChunkSize = len(scd.CurrentChunk) // cache the size
	scd.CurrentIndex = -1                        // initial chunks idx
	scd.CurrentChunkIndex = -1                   // initial chunk

	// start downloading chunks if exists
	if len(scd.ChunkMetas) > 0 {
		glog.V(2).Infof("chunks: %v", len(scd.ChunkMetas))
		scd.ChunksMutex = &sync.Mutex{}
		scd.Chunks = make(map[int][][]*string)
		scd.ChunksChan = make(chan int, len(scd.ChunkMetas))
		scd.ChunksError = make(chan *chunkError, len(scd.ChunkMetas))
		for i := 0; i < len(scd.ChunkMetas); i++ {
			glog.V(2).Infof("add chunk to channel ChunksChan: %v", i+1)
			scd.ChunksChan <- i
		}
		for i := 0; i < intMin(maxChunkDownloadWorkers, len(scd.ChunkMetas)); i++ {
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
		if scd.ChunksErrorCounter < maxChunkDownloaderErrorCounter {
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
	for true {
		scd.CurrentIndex++
		if scd.CurrentIndex < scd.CurrentChunkSize {
			return scd.CurrentChunk[scd.CurrentIndex], nil
		}
		scd.CurrentChunkIndex++ // next chunk
		scd.CurrentIndex = -1   // reset
		if scd.CurrentChunkIndex >= len(scd.ChunkMetas) {
			break
		}
		ticker := time.Tick(time.Second)
		for range ticker {
			scd.ChunksMutex.Lock()
			err := scd.checkErrorRetry()
			if err != nil {
				return nil, err
			}
			glog.V(2).Infof("waiting for chunk idx: %v/%v",
				scd.CurrentChunkIndex+1, len(scd.ChunkMetas))
			scd.CurrentChunk = scd.Chunks[scd.CurrentChunkIndex]
			scd.ChunksMutex.Unlock()
			if scd.CurrentChunk != nil {
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

func (scd *snowflakeChunkDownloader) get(
	ctx context.Context,
	fullURL string,
	headers map[string]string,
	timeout time.Duration) (
	*http.Response, error) {
	return retryHTTP(ctx, scd.sc.rest.Client, http.NewRequest, "GET", fullURL, headers, nil, timeout)
}

func downloadChunk(scd *snowflakeChunkDownloader, idx int) {
	glog.V(2).Infof("download start chunk: %v", idx+1)
	headers := make(map[string]string)
	headers[headerSseCAlgorithm] = headerSseCAes
	headers[headerSseCKey] = scd.Qrmk
	resp, err := scd.get(scd.ctx, scd.ChunkMetas[idx].URL, headers, 0)
	if err != nil {
		scd.ChunksError <- &chunkError{Index: idx, Error: err}
		return
	}
	defer resp.Body.Close()
	glog.V(2).Infof("download finish chunk: %v, resp: %v", idx+1, resp)
	if resp.StatusCode == http.StatusOK {
		var respd [][]*string
		b, err := ioutil.ReadAll(resp.Body)
		r := []byte{0x5b} // opening bracket
		r = append(r, b...)
		r = append(r, 0x5d) // closing bracket
		err = json.Unmarshal(r, &respd)
		if err != nil {
			glog.V(1).Infof("%v", err)
			scd.ChunksError <- &chunkError{Index: idx, Error: err}
			return
		}
		scd.ChunksMutex.Lock()
		scd.Chunks[idx] = respd
		scd.ChunksMutex.Unlock()
	} else {
		// TODO: better error handing and retry
		b, err := ioutil.ReadAll(resp.Body)
		glog.V(2).Infof("b RESPONSE: %s", b)
		if err != nil {
			glog.V(1).Infof("%v", err)
			scd.ChunksError <- &chunkError{Index: idx, Error: err}
			return
		}
		glog.V(2).Infof("ERROR RESPONSE: %s", b)
		scd.ChunksError <- &chunkError{Index: idx, Error: err}
	}
}
