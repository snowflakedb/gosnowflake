// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

type RowsExtended struct {
	rows      *sql.Rows
	closeChan *chan bool
}

func (rs *RowsExtended) Close() error {
	*rs.closeChan <- true
	close(*rs.closeChan)
	return rs.rows.Close()
}

func (rs *RowsExtended) ColumnTypes() ([]*sql.ColumnType, error) {
	return rs.rows.ColumnTypes()
}

func (rs *RowsExtended) Columns() ([]string, error) {
	return rs.rows.Columns()
}

func (rs *RowsExtended) Err() error {
	return rs.rows.Err()
}

func (rs *RowsExtended) Next() bool {
	return rs.rows.Next()
}

func (rs *RowsExtended) NextResultSet() bool {
	return rs.rows.NextResultSet()
}

func (rs *RowsExtended) Scan(dest ...interface{}) error {
	return rs.rows.Scan(dest...)
}

// test variables
var (
	rowsInChunk = 123
)

// Special cases where rows are already closed
func TestRowsClose(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows, err := dbt.query("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}
		if err = rows.Close(); err != nil {
			dbt.Fatal(err)
		}

		if rows.Next() {
			dbt.Fatal("unexpected row after rows.Close()")
		}
		if err = rows.Err(); err != nil {
			dbt.Fatal(err)
		}
	})
}

func TestResultNoRows(t *testing.T) {
	// DDL
	runDBTest(t, func(dbt *DBTest) {
		row, err := dbt.exec("CREATE OR REPLACE TABLE test(c1 int)")
		if err != nil {
			t.Fatalf("failed to execute DDL. err: %v", err)
		}
		if _, err = row.RowsAffected(); err == nil {
			t.Fatal("should have failed to get RowsAffected")
		}
		if _, err = row.LastInsertId(); err == nil {
			t.Fatal("should have failed to get LastInsertID")
		}
	})
}

func TestRowsWithoutChunkDownloader(t *testing.T) {
	sts1 := "1"
	sts2 := "Test1"
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 10; i++ {
		cc = append(cc, []*string{&sts1, &sts2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := []execResponseChunk{}
	rows := new(snowflakeRows)
	sc := &snowflakeConn{
		cfg: &Config{
			Params: make(map[string]*string),
		},
	}
	rows.sc = sc
	rows.ctx = context.Background()
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:                 sc,
		ctx:                context.Background(),
		Total:              int64(len(cc)),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "",
		FuncDownload:       nil,
		FuncDownloadHelper: nil,
		RowSet:             rowSetType{RowType: rt, JSON: cc},
		QueryResultFormat:  "json",
	}
	err := rows.ChunkDownloader.start()
	assertNilF(t, err)
	dest := make([]driver.Value, 2)
	for i = 0; i < len(cc); i++ {
		if err := rows.Next(dest); err != nil {
			t.Fatalf("failed to get value. err: %v", err)
		}
		if dest[0] != sts1 {
			t.Fatalf("failed to get value. expected: %v, got: %v", sts1, dest[0])
		}
		if dest[1] != sts2 {
			t.Fatalf("failed to get value. expected: %v, got: %v", sts2, dest[1])
		}
	}
	if err := rows.Next(dest); err != io.EOF {
		t.Fatalf("failed to finish getting data. err: %v", err)
	}
	logger.Infof("dest: %v", dest)

}

func downloadChunkTest(ctx context.Context, scd *snowflakeChunkDownloader, idx int) {
	d := make([][]*string, 0)
	for i := 0; i < rowsInChunk; i++ {
		v1 := fmt.Sprintf("%v", idx*1000+i)
		v2 := fmt.Sprintf("testchunk%v", idx*1000+i)
		d = append(d, []*string{&v1, &v2})
	}
	scd.ChunksMutex.Lock()
	scd.Chunks[idx] = make([]chunkRowType, len(d))
	populateJSONRowSet(scd.Chunks[idx], d)
	scd.DoneDownloadCond.Broadcast()
	scd.ChunksMutex.Unlock()
}

func TestRowsWithChunkDownloader(t *testing.T) {
	numChunks := 12
	// changed the workers
	backupMaxChunkDownloadWorkers := MaxChunkDownloadWorkers
	MaxChunkDownloadWorkers = 2
	logger.Info("START TESTS")
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 100; i++ {
		v1 := fmt.Sprintf("%v", i)
		v2 := fmt.Sprintf("Test%v", i)
		cc = append(cc, []*string{&v1, &v2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := make([]execResponseChunk, 0)
	for i = 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf("dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	rows := new(snowflakeRows)
	sc := &snowflakeConn{
		cfg: &Config{
			Params: make(map[string]*string),
		},
	}
	rows.sc = sc
	rows.ctx = context.Background()
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:            sc,
		ctx:           context.Background(),
		Total:         int64(len(cc) + numChunks*rowsInChunk),
		ChunkMetas:    cm,
		TotalRowIndex: int64(-1),
		Qrmk:          "HAHAHA",
		FuncDownload:  downloadChunkTest,
		RowSet:        rowSetType{RowType: rt, JSON: cc},
	}
	assertNilF(t, rows.ChunkDownloader.start())
	cnt := 0
	dest := make([]driver.Value, 2)
	var err error
	for err != io.EOF {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to get value. err: %v", err)
		}
		cnt++
	}
	if cnt != len(cc)+numChunks*rowsInChunk {
		t.Fatalf("failed to get all results. expected:%v, got:%v", len(cc)+numChunks*rowsInChunk, cnt)
	}
	logger.Infof("dest: %v", dest)
	MaxChunkDownloadWorkers = backupMaxChunkDownloadWorkers
	logger.Info("END TESTS")
}

func downloadChunkTestError(ctx context.Context, scd *snowflakeChunkDownloader, idx int) {
	// fail to download 6th and 10th chunk, and retry up to N times and success
	// NOTE: zero based index
	scd.ChunksMutex.Lock()
	defer scd.ChunksMutex.Unlock()
	if (idx == 6 || idx == 10) && scd.ChunksErrorCounter < maxChunkDownloaderErrorCounter {
		scd.ChunksError <- &chunkError{
			Index: idx,
			Error: fmt.Errorf(
				"dummy error. idx: %v, errCnt: %v", idx+1, scd.ChunksErrorCounter)}
		scd.DoneDownloadCond.Broadcast()
		return
	}
	d := make([][]*string, 0)
	for i := 0; i < rowsInChunk; i++ {
		v1 := fmt.Sprintf("%v", idx*1000+i)
		v2 := fmt.Sprintf("testchunk%v", idx*1000+i)
		d = append(d, []*string{&v1, &v2})
	}
	scd.Chunks[idx] = make([]chunkRowType, len(d))
	populateJSONRowSet(scd.Chunks[idx], d)
	scd.DoneDownloadCond.Broadcast()
}

func TestRowsWithChunkDownloaderError(t *testing.T) {
	numChunks := 12
	// changed the workers
	backupMaxChunkDownloadWorkers := MaxChunkDownloadWorkers
	MaxChunkDownloadWorkers = 3
	logger.Info("START TESTS")
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 100; i++ {
		v1 := fmt.Sprintf("%v", i)
		v2 := fmt.Sprintf("Test%v", i)
		cc = append(cc, []*string{&v1, &v2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := make([]execResponseChunk, 0)
	for i = 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf("dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	rows := new(snowflakeRows)
	sc := &snowflakeConn{
		cfg: &Config{
			Params: make(map[string]*string),
		},
	}
	rows.sc = sc
	rows.ctx = context.Background()
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:            sc,
		ctx:           context.Background(),
		Total:         int64(len(cc) + numChunks*rowsInChunk),
		ChunkMetas:    cm,
		TotalRowIndex: int64(-1),
		Qrmk:          "HOHOHO",
		FuncDownload:  downloadChunkTestError,
		RowSet:        rowSetType{RowType: rt, JSON: cc},
	}
	assertNilF(t, rows.ChunkDownloader.start())
	cnt := 0
	dest := make([]driver.Value, 2)
	var err error
	for err != io.EOF {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to get value. err: %v", err)
		}
		// fmt.Printf("data: %v\n", dest)
		cnt++
	}
	if cnt != len(cc)+numChunks*rowsInChunk {
		t.Fatalf("failed to get all results. expected:%v, got:%v", len(cc)+numChunks*rowsInChunk, cnt)
	}
	logger.Infof("dest: %v", dest)
	MaxChunkDownloadWorkers = backupMaxChunkDownloadWorkers
	logger.Info("END TESTS")
}

func downloadChunkTestErrorFail(ctx context.Context, scd *snowflakeChunkDownloader, idx int) {
	// fail to download 6th and 10th chunk, and retry up to N times and fail
	// NOTE: zero based index
	scd.ChunksMutex.Lock()
	defer scd.ChunksMutex.Unlock()
	if idx == 6 && scd.ChunksErrorCounter <= maxChunkDownloaderErrorCounter {
		scd.ChunksError <- &chunkError{
			Index: idx,
			Error: fmt.Errorf(
				"dummy error. idx: %v, errCnt: %v", idx+1, scd.ChunksErrorCounter)}
		scd.DoneDownloadCond.Broadcast()
		return
	}
	d := make([][]*string, 0)
	for i := 0; i < rowsInChunk; i++ {
		v1 := fmt.Sprintf("%v", idx*1000+i)
		v2 := fmt.Sprintf("testchunk%v", idx*1000+i)
		d = append(d, []*string{&v1, &v2})
	}
	scd.Chunks[idx] = make([]chunkRowType, len(d))
	populateJSONRowSet(scd.Chunks[idx], d)
	scd.DoneDownloadCond.Broadcast()
}

func TestRowsWithChunkDownloaderErrorFail(t *testing.T) {
	numChunks := 12
	// changed the workers
	logger.Info("START TESTS")
	var i int
	cc := make([][]*string, 0)
	for i = 0; i < 100; i++ {
		v1 := fmt.Sprintf("%v", i)
		v2 := fmt.Sprintf("Test%v", i)
		cc = append(cc, []*string{&v1, &v2})
	}
	rt := []execResponseRowType{
		{Name: "c1", ByteLength: 10, Length: 10, Type: "FIXED", Scale: 0, Nullable: true},
		{Name: "c2", ByteLength: 100000, Length: 100000, Type: "TEXT", Scale: 0, Nullable: false},
	}
	cm := make([]execResponseChunk, 0)
	for i = 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf("dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	rows := new(snowflakeRows)
	sc := &snowflakeConn{
		cfg: &Config{
			Params: make(map[string]*string),
		},
	}
	rows.sc = sc
	rows.ctx = context.Background()
	rows.ChunkDownloader = &snowflakeChunkDownloader{
		sc:            sc,
		ctx:           context.Background(),
		Total:         int64(len(cc) + numChunks*rowsInChunk),
		ChunkMetas:    cm,
		TotalRowIndex: int64(-1),
		Qrmk:          "HOHOHO",
		FuncDownload:  downloadChunkTestErrorFail,
		RowSet:        rowSetType{RowType: rt, JSON: cc},
	}
	assertNilF(t, rows.ChunkDownloader.start())
	cnt := 0
	dest := make([]driver.Value, 2)
	var err error
	for err != io.EOF {
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Infof(
				"failure was expected by the number of rows is wrong. expected: %v, got: %v", 715, cnt)
			break
		}
		cnt++
	}
}

func getChunkTestInvalidResponseBody(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (
	*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func TestDownloadChunkInvalidResponseBody(t *testing.T) {
	numChunks := 2
	cm := make([]execResponseChunk, 0)
	for i := 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf(
			"dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	scd := &snowflakeChunkDownloader{
		sc: &snowflakeConn{
			rest: &snowflakeRestful{RequestTimeout: defaultRequestTimeout},
		},
		ctx:                context.Background(),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "HOHOHO",
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet:            getChunkTestInvalidResponseBody,
	}
	scd.ChunksMutex = &sync.Mutex{}
	scd.DoneDownloadCond = sync.NewCond(scd.ChunksMutex)
	scd.Chunks = make(map[int][]chunkRowType)
	scd.ChunksError = make(chan *chunkError, 1)
	scd.FuncDownload(scd.ctx, scd, 1)
	select {
	case errc := <-scd.ChunksError:
		if errc.Index != 1 {
			t.Fatalf("the error should have caused with chunk idx: %v", errc.Index)
		}
	default:
		t.Fatal("should have caused an error and queued in scd.ChunksError")
	}
}

func getChunkTestErrorStatus(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (
	*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func TestDownloadChunkErrorStatus(t *testing.T) {
	numChunks := 2
	cm := make([]execResponseChunk, 0)
	for i := 0; i < numChunks; i++ {
		cm = append(cm, execResponseChunk{URL: fmt.Sprintf(
			"dummyURL%v", i+1), RowCount: rowsInChunk})
	}
	scd := &snowflakeChunkDownloader{
		sc: &snowflakeConn{
			rest: &snowflakeRestful{RequestTimeout: defaultRequestTimeout},
		},
		ctx:                context.Background(),
		ChunkMetas:         cm,
		TotalRowIndex:      int64(-1),
		Qrmk:               "HOHOHO",
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet:            getChunkTestErrorStatus,
	}
	scd.ChunksMutex = &sync.Mutex{}
	scd.DoneDownloadCond = sync.NewCond(scd.ChunksMutex)
	scd.Chunks = make(map[int][]chunkRowType)
	scd.ChunksError = make(chan *chunkError, 1)
	scd.FuncDownload(scd.ctx, scd, 1)
	select {
	case errc := <-scd.ChunksError:
		if errc.Index != 1 {
			t.Fatalf("the error should have caused with chunk idx: %v", errc.Index)
		}
		serr, ok := errc.Error.(*SnowflakeError)
		if !ok {
			t.Fatalf("should have been snowflake error. err: %v", errc.Error)
		}
		if serr.Number != ErrFailedToGetChunk {
			t.Fatalf("message error code is not correct. msg: %v", serr.Number)
		}
	default:
		t.Fatal("should have caused an error and queued in scd.ChunksError")
	}
}

func TestWithArrowBatchesNotImplementedForResult(t *testing.T) {
	ctx := WithArrowBatches(context.Background())
	runSnowflakeConnTest(t, func(sct *SCTest) {

		sct.mustExec("create or replace table testArrowBatches (a int, b int)", nil)
		defer sct.mustExec("drop table if exists testArrowBatches", nil)

		result := sct.mustExecContext(ctx, "insert into testArrowBatches values (1, 2), (3, 4), (5, 6)", []driver.NamedValue{})

		_, err := result.(*snowflakeResult).GetArrowBatches()
		if err == nil {
			t.Fatal("should have raised an error")
		}
		driverErr, ok := err.(*SnowflakeError)
		if !ok {
			t.Fatalf("should be snowflake error. err: %v", err)
		}
		if driverErr.Number != ErrNotImplemented {
			t.Fatalf("unexpected error code. expected: %v, got: %v", ErrNotImplemented, driverErr.Number)
		}
	})
}

func TestLocationChangesAfterAlterSession(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE location_timestamp_ltz (val timestamp_ltz)")
		defer dbt.mustExec("DROP TABLE location_timestamp_ltz")
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'")
		dbt.mustExec("INSERT INTO location_timestamp_ltz VALUES('2023-08-09 10:00:00')")
		rows1 := dbt.mustQuery("SELECT * FROM location_timestamp_ltz")
		defer func() {
			assertNilF(t, rows1.Close())
		}()
		if !rows1.Next() {
			t.Fatalf("cannot read a record")
		}
		var t1 time.Time
		assertNilF(t, rows1.Scan(&t1))
		if t1.Location().String() != "Europe/Warsaw" {
			t.Fatalf("should return time in Warsaw timezone")
		}
		dbt.mustExec("ALTER SESSION SET TIMEZONE = 'Pacific/Honolulu'")
		rows2 := dbt.mustQuery("SELECT * FROM location_timestamp_ltz")
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		if !rows2.Next() {
			t.Fatalf("cannot read a record")
		}
		var t2 time.Time
		assertNilF(t, rows2.Scan(&t2))
		if t2.Location().String() != "Pacific/Honolulu" {
			t.Fatalf("should return time in Honolulu timezone")
		}
	})
}
