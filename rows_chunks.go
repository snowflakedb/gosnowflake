// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/golang/glog"
)

const maxPool = 10

type chunkError struct {
	Index int
	Error error
}

type snowflakeChunkDownloader struct {
	Total             int64
	TotalRowIndex     int64
	CurrentChunk      [][]*string
	CurrentChunkIndex int
	CurrentChunkSize  int
	Client            *http.Client
	ChunkMetas        []execResponseChunk
	Chunks            map[int][][]*string
	ChunksMutex       *sync.Mutex
	ChunksChan        chan int
	ChunkErrors       chan *chunkError
	Qrmk              string
	CurrentIndex      int
}

func (scd *snowflakeChunkDownloader) Start() error {
	scd.CurrentChunkSize = len(scd.CurrentChunk) // cache the size

	scd.CurrentIndex = -1      // initial chunks idx
	scd.CurrentChunkIndex = -1 // initial chunk

	// start downloading chunks if exists
	if len(scd.ChunkMetas) > 0 {
		glog.V(2).Infof("chunks: %v", len(scd.ChunkMetas))
		scd.ChunksMutex = &sync.Mutex{}
		scd.Chunks = make(map[int][][]*string)
		scd.ChunksChan = make(chan int, maxPool)
		scd.ChunkErrors = make(chan *chunkError, maxPool)
		for i := 0; i < len(scd.ChunkMetas); i++ {
			glog.V(2).Infof("add chunk: %v", i+1)
			scd.ChunksChan <- i
		}
		for i := 0; i < intMin(maxPool, len(scd.ChunkMetas)); i++ {
			scd.schedule()
		}
		scd.Client = &http.Client{
			Timeout:   60 * time.Second, // each request timeout
			Transport: snowflakeTransport,
		} // create a new client
	}
	return nil
}

func (scd *snowflakeChunkDownloader) schedule() {
	select {
	case nextIdx := <-scd.ChunksChan:
		glog.V(2).Infof("schedule chunk: %v", nextIdx+1)
		go scd.download(nextIdx, scd.ChunkErrors)
	default:
		// no more download
	}
}

func (scd *snowflakeChunkDownloader) Next() ([]*string, error) {
	for true {
		scd.CurrentIndex++
		if scd.CurrentIndex < scd.CurrentChunkSize {
			return scd.CurrentChunk[scd.CurrentIndex], nil
		}
		scd.CurrentChunkIndex++ // next chunk
		scd.CurrentIndex = -1   // reset
		if scd.CurrentChunkIndex < len(scd.ChunkMetas) {
			ticker := time.Tick(time.Millisecond * 100)
			// TODO: Error handle
			for range ticker {
				glog.V(2).Infof(
					"waiting for chunk idx: %v/%v, got chunks: %v",
					scd.CurrentChunkIndex+1, len(scd.ChunkMetas), len(scd.Chunks))
				scd.ChunksMutex.Lock()
				scd.CurrentChunk = scd.Chunks[scd.CurrentChunkIndex]
				scd.ChunksMutex.Unlock()
				if scd.CurrentChunk != nil {
					// kick off the next download
					glog.V(2).Infof("ready: chunk %v", scd.CurrentChunkIndex)
					scd.CurrentChunkSize = len(scd.CurrentChunk)
					break
				}
			}
		} else {
			break
		}
	}
	// no more data
	glog.V(2).Infof("no more data")
	return nil, io.EOF
}

func (scd *snowflakeChunkDownloader) get(
	fullURL string,
	headers map[string]string,
	timeout time.Duration) (
	*http.Response, error) {
	return retryHTTP(scd.Client, "GET", fullURL, headers, nil, timeout)
}

func (scd *snowflakeChunkDownloader) download(idx int, errc chan *chunkError) {
	glog.V(2).Infof("download start chunk: %v", idx+1)
	headers := make(map[string]string)
	headers[headerSseCAlgorithm] = headerSseCAes
	headers[headerSseCKey] = scd.Qrmk
	resp, err := scd.get(scd.ChunkMetas[idx].URL, headers, 0)
	if err != nil {
		errc <- &chunkError{Index: idx, Error: err}
		return
	}
	defer resp.Body.Close()
	glog.V(2).Infof("download finish chunk: %v", idx+1)
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("download: %v, %v", idx+1, resp)
		// TODO: optimize the memory usage
		var respd [][]*string
		b, err := ioutil.ReadAll(resp.Body)
		r := []byte{0x5b}
		r = append(r, b...)
		r = append(r, 0x5d)
		err = json.Unmarshal(r, &respd)
		if err != nil {
			glog.V(1).Infof("%v", err)
			errc <- &chunkError{Index: idx, Error: err}
			return
		}
		scd.ChunksMutex.Lock()
		scd.Chunks[idx] = respd
		scd.ChunksMutex.Unlock()
	} else {
		// TODO: better error handing and retry
		glog.V(2).Infof("download: resp: %v", resp)
		b, err := ioutil.ReadAll(resp.Body)
		glog.V(2).Infof("b RESPONSE: %s", b)
		if err != nil {
			glog.V(1).Infof("%v", err)
			errc <- &chunkError{Index: idx, Error: err}
			return
		}
		glog.V(2).Infof("ERROR RESPONSE: %s", b)
		errc <- &chunkError{Index: idx, Error: err}
	}
}
