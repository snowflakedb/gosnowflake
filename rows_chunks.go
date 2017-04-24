// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
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

func (scd *snowflakeChunkDownloader) min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (scd *snowflakeChunkDownloader) Start() error {
	scd.CurrentChunkSize = len(scd.CurrentChunk) // cache the size

	scd.CurrentIndex = -1      // initial chunks idx
	scd.CurrentChunkIndex = -1 // initial chunk

	// start downloading chunks if exists
	if len(scd.ChunkMetas) > 0 {
		log.Printf("chunks: %v", len(scd.ChunkMetas))
		scd.ChunksMutex = &sync.Mutex{}
		scd.Chunks = make(map[int][][]*string)
		scd.ChunksChan = make(chan int, maxPool)
		scd.ChunkErrors = make(chan *chunkError, maxPool)
		for i := 0; i < len(scd.ChunkMetas); i++ {
			log.Printf("Adding %v", i)
			scd.ChunksChan <- i
		}
		for i := 0; i < scd.min(maxPool, len(scd.ChunkMetas)); i++ {
			scd.schedule()
		}
		scd.Client = &http.Client{Transport: snowflakeTransport} // create a new client
	}
	return nil
}

func (scd *snowflakeChunkDownloader) schedule() {
	select {
	case nextIdx := <-scd.ChunksChan:
		log.Printf("schedule: %v", nextIdx)
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
			ticker := time.Tick(time.Millisecond * 10)
			// TODO: Error handle
			for _ = range ticker {
				log.Printf(
					"Waiting. chunk idx: %v/%v, got chunks: %v",
					scd.CurrentChunkIndex, len(scd.ChunkMetas), len(scd.Chunks))
				scd.ChunksMutex.Lock()
				scd.CurrentChunk = scd.Chunks[scd.CurrentChunkIndex]
				scd.ChunksMutex.Unlock()
				if scd.CurrentChunk != nil {
					// kick off the next download
					log.Printf("ready")
					scd.CurrentChunkSize = len(scd.CurrentChunk)
					break
				}
			}
		} else {
			break
		}
	}
	// no more data
	log.Printf("no more data")
	return nil, io.EOF
}

func (scd *snowflakeChunkDownloader) get(
	fullURL string,
	headers map[string]string) (
	*http.Response, error) {
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return scd.Client.Do(req)
}

func (scd *snowflakeChunkDownloader) download(idx int, errc chan *chunkError) {
	log.Printf("download start: %v", idx)
	headers := make(map[string]string)
	headers[headerSseCAlgorithm] = headerSseCAes
	headers[headerSseCKey] = scd.Qrmk
	resp, err := scd.get(scd.ChunkMetas[idx].URL, headers)
	if err != nil {
		errc <- &chunkError{Index: idx, Error: err}
		return
	}
	defer resp.Body.Close()
	log.Printf("download end: %v", idx)
	if resp.StatusCode == http.StatusOK {
		log.Printf("download: resp: %v", resp)
		// TODO: optimize the memory usage
		var respd [][]*string
		b, err := ioutil.ReadAll(resp.Body)
		r := []byte{0x5b}
		r = append(r, b...)
		r = append(r, 0x5d)
		err = json.Unmarshal(r, &respd)
		if err != nil {
			log.Fatal(err)
			errc <- &chunkError{Index: idx, Error: err}
			return
		}
		scd.ChunksMutex.Lock()
		scd.Chunks[idx] = respd
		scd.ChunksMutex.Unlock()
	} else {
		// TODO: better error handing and retry
		log.Printf("download: resp: %v", resp)
		b, err := ioutil.ReadAll(resp.Body)
		log.Printf("b RESPONSE: %s", b)
		if err != nil {
			log.Fatal(err)
			errc <- &chunkError{Index: idx, Error: err}
			return
		}
		log.Printf("ERROR RESPONSE: %s", b)
		errc <- &chunkError{Index: idx, Error: err}
	}
}
