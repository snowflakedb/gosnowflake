// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
)

func TestBadChunkData(t *testing.T) {
	testDecodeErr(t, "")
	testDecodeErr(t, "null")
	testDecodeErr(t, "42")
	testDecodeErr(t, "\"null\"")
	testDecodeErr(t, "{}")

	testDecodeErr(t, "[[]")
	testDecodeErr(t, "[null]")
	testDecodeErr(t, `[[hello world]]`)

	testDecodeErr(t, `[[""hello world""]]`)
	testDecodeErr(t, `[["\"hello world""]]`)
	testDecodeErr(t, `[[""hello world\""]]`)
	testDecodeErr(t, `[["hello world`)
	testDecodeErr(t, `[["hello world"`)
	testDecodeErr(t, `[["hello world"]`)

	testDecodeErr(t, `[["\uQQQQ"]]`)

	for b := byte(0); b < ' '; b++ {
		testDecodeErr(t, string([]byte{
			'[', '[', '"', b, '"', ']', ']',
		}))
	}
}

func TestValidChunkData(t *testing.T) {
	testDecodeOk(t, "[]")
	testDecodeOk(t, "[  ]")
	testDecodeOk(t, "[[]]")
	testDecodeOk(t, "[ [  ]   ]")
	testDecodeOk(t, "[[],[],[],[]]")
	testDecodeOk(t, "[[] , []  , [], []  ]")

	testDecodeOk(t, "[[null]]")
	testDecodeOk(t, "[[\n\t\r null]]")
	testDecodeOk(t, "[[null,null]]")
	testDecodeOk(t, "[[ null , null ]]")
	testDecodeOk(t, "[[null],[null],[null]]")
	testDecodeOk(t, "[[null],[ null  ] ,  [null]]")

	testDecodeOk(t, `[[""]]`)
	testDecodeOk(t, `[["false"]]`)
	testDecodeOk(t, `[["true"]]`)
	testDecodeOk(t, `[["42"]]`)

	testDecodeOk(t, `[[""]]`)
	testDecodeOk(t, `[["hello"]]`)
	testDecodeOk(t, `[["hello world"]]`)

	testDecodeOk(t, `[["/ ' \\ \b \t \n \f \r \""]]`)
	testDecodeOk(t, `[["❄"]]`)
	testDecodeOk(t, `[["\u2744"]]`)
	testDecodeOk(t, `[["\uFfFc"]]`)       // consume replacement chars
	testDecodeOk(t, `[["\ufffd"]]`)       // consume replacement chars
	testDecodeOk(t, `[["\u0000"]]`)       // yes, this is valid
	testDecodeOk(t, `[["\uD834\uDD1E"]]`) // surrogate pair
	testDecodeOk(t, `[["\uD834\u0000"]]`) // corrupt surrogate pair

	testDecodeOk(t, `[["$"]]`)      // "$"
	testDecodeOk(t, `[["\u0024"]]`) // "$"

	testDecodeOk(t, `[["\uC2A2"]]`) // "¢"
	testDecodeOk(t, `[["¢"]]`)      // "¢"

	testDecodeOk(t, `[["\u00E2\u82AC"]]`) // "€"
	testDecodeOk(t, `[["€"]]`)            // "€"

	testDecodeOk(t, `[["\uF090\u8D88"]]`) // "𐍈"
	testDecodeOk(t, `[["𐍈"]]`)            // "𐍈"
}

func TestSmallBufferChunkData(t *testing.T) {
	r := strings.NewReader(`[
	  [null,"hello world"],
	  ["foo bar", null],
	  [null, null] ,
	  ["foo bar",   "hello world" ]
	]`)

	lcd := largeChunkDecoder{
		r, 0, 0,
		0, 0,
		make([]byte, 1),
		bytes.NewBuffer(make([]byte, defaultStringBufferSize)),
		nil,
	}

	if _, err := lcd.decode(); err != nil {
		t.Fatalf("failed with small buffer: %s", err)
	}
}

func TestEnsureBytes(t *testing.T) {
	// the content here doesn't matter
	r := strings.NewReader("0123456789")

	lcd := largeChunkDecoder{
		r, 0, 0,
		3, 8189,
		make([]byte, 8192),
		bytes.NewBuffer(make([]byte, defaultStringBufferSize)),
		nil,
	}

	lcd.ensureBytes(4)

	// we expect the new remainder to be 3 + 10 (length of r)
	if lcd.rem != 13 {
		t.Fatalf("buffer was not refilled correctly")
	}
}

func testDecodeOk(t *testing.T, s string) {
	var rows [][]*string
	if err := json.Unmarshal([]byte(s), &rows); err != nil {
		t.Fatalf("test case is not valid json / [][]*string: %s", s)
	}

	// NOTE we parse and stringify the expected result to
	// remove superficial differences, like whitespace
	expect, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("unreachable: %s", err)
	}

	rows, err = decodeLargeChunk(strings.NewReader(s), 0, 0)
	if err != nil {
		t.Fatalf("expected decode to succeed: %s", err)
	}

	actual, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("json marshal failed: %s", err)
	}
	if string(actual) != string(expect) {
		t.Fatalf(`
		result did not match expected result
		  expect=%s
		   bytes=(%v)

		  acutal=%s
		   bytes=(%v)`,
			string(expect), expect,
			string(actual), actual,
		)
	}
}

func testDecodeErr(t *testing.T, s string) {
	if _, err := decodeLargeChunk(strings.NewReader(s), 0, 0); err == nil {
		t.Fatalf("expected decode to fail for input: %s", s)
	}
}

type mockStreamChunkFetcher struct {
	chunks map[string][][]*string
}

func (f *mockStreamChunkFetcher) fetch(url string, stream chan<- []*string) error {
	for _, row := range f.chunks[url] {
		stream <- row
	}
	return nil
}

func TestStreamChunkDownloaderFirstRows(t *testing.T) {
	fetcher := &mockStreamChunkFetcher{}
	firstRows := generateStreamChunkRows(10, 4)
	downloader := newStreamChunkDownloader(
		context.Background(),
		fetcher,
		int64(len(firstRows)),
		[]execResponseRowType{},
		firstRows,
		[]execResponseChunk{})
	if err := downloader.start(); err != nil {
		t.Fatalf("chunk download start failed. err: %v", err)
	}
	for i := 0; i < len(firstRows); i++ {
		if !downloader.hasNextResultSet() {
			t.Error("failed to retrieve next result set")
		}
		if err := downloader.nextResultSet(); err != nil {
			t.Fatalf("failed to retrieve data. err: %v", err)
		}
		row, err := downloader.next()
		if err != nil {
			t.Fatalf("failed to retrieve data. err: %v", err)
		}
		assertEqualRows(firstRows[i], row)
	}
	row, err := downloader.next()
	if !assertEmptyChunkRow(row) {
		t.Fatal("row should be empty")
	}
	if err != io.EOF {
		t.Fatalf("failed to finish getting data. err: %v", err)
	}
	if downloader.hasNextResultSet() {
		t.Error("downloader has next result set. expected none.")
	}
	if downloader.nextResultSet() != io.EOF {
		t.Fatalf("failed to finish getting data. err: %v", err)
	}
}

func TestStreamChunkDownloaderChunks(t *testing.T) {
	chunks, responseChunks := generateStreamChunkDownloaderChunks([]string{"foo", "bar"}, 4, 4)
	fetcher := &mockStreamChunkFetcher{chunks}
	firstRows := generateStreamChunkRows(2, 4)
	downloader := newStreamChunkDownloader(
		context.Background(),
		fetcher,
		int64(len(firstRows)),
		[]execResponseRowType{},
		firstRows,
		responseChunks)
	if err := downloader.start(); err != nil {
		t.Fatalf("chunk download start failed. err: %v", err)
	}
	for i := 0; i < len(firstRows); i++ {
		if !downloader.hasNextResultSet() {
			t.Error("failed to retrieve next result set")
		}
		if err := downloader.nextResultSet(); err != nil {
			t.Fatalf("failed to retrieve data. err: %v", err)
		}
		row, err := downloader.next()
		if err != nil {
			t.Fatalf("failed to retrieve data. err: %v", err)
		}
		assertEqualRows(firstRows[i], row)
	}
	for _, chunk := range responseChunks {
		for _, chunkRow := range chunks[chunk.URL] {
			if !downloader.hasNextResultSet() {
				t.Error("failed to retrieve next result set")
			}
			row, err := downloader.next()
			if err != nil {
				t.Fatalf("failed to retrieve data. err: %v", err)
			}
			assertEqualRows(chunkRow, row)
		}
	}
	row, err := downloader.next()
	if !assertEmptyChunkRow(row) {
		t.Fatal("row should be empty")
	}
	if err != io.EOF {
		t.Fatalf("failed to finish getting data. err: %v", err)
	}
	if downloader.hasNextResultSet() {
		t.Error("downloader has next result set. expected none.")
	}
	if downloader.nextResultSet() != io.EOF {
		t.Fatalf("failed to finish getting data. err: %v", err)
	}
}

func TestCopyChunkStream(t *testing.T) {
	foo := "foo"
	bar := "bar"

	r := strings.NewReader(`["foo","bar",null],["bar",null,"foo"],[]`)
	c := make(chan []*string, 3)
	if err := copyChunkStream(r, c); err != nil {
		t.Fatalf("error while copying chunk stream. err: %v", err)
	}
	assertEqualRows([]*string{&foo, &bar, nil}, <-c)
	assertEqualRows([]*string{&bar, nil, &foo}, <-c)
	assertEqualRows([]*string{}, <-c)
}

func TestCopyChunkStreamInvalid(t *testing.T) {
	var r io.Reader
	var c chan []*string
	var err error

	r = strings.NewReader("oops")
	c = make(chan []*string, 1)
	if err = copyChunkStream(r, c); err == nil {
		t.Fatalf("should fail to retrieve data. err: %v", err)
	}

	r = strings.NewReader(`[["foo"], ["bar"]]`)
	c = make(chan []*string, 1)
	if err = copyChunkStream(r, c); err == nil {
		t.Fatalf("should fail to retrieve data. err: %v", err)
	}

	r = strings.NewReader(`{"foo": "bar"}`)
	c = make(chan []*string, 1)
	if err = copyChunkStream(r, c); err == nil {
		t.Fatalf("should fail to retrieve data. err: %v", err)
	}
}

func generateStreamChunkDownloaderChunks(urls []string, numRows, numCols int) (map[string][][]*string, []execResponseChunk) {
	chunks := map[string][][]*string{}
	var responseChunks []execResponseChunk
	for _, url := range urls {
		rows := generateStreamChunkRows(numRows, numCols)
		chunks[url] = rows
		responseChunks = append(responseChunks, execResponseChunk{url, len(rows), -1, -1})
	}
	return chunks, responseChunks
}

func generateStreamChunkRows(numRows, numCols int) [][]*string {
	rows := [][]*string{}
	for i := 0; i < numRows; i++ {
		cols := []*string{}
		for j := 0; j < numCols; j++ {
			col := fmt.Sprintf("%d", rand.Intn(1000))
			cols = append(cols, &col)
		}
		rows = append(rows, cols)
	}
	return rows
}

func assertEqualRows(expected []*string, actual interface{}) bool {
	switch v := actual.(type) {
	case chunkRowType:
		for i := range expected {
			if expected[i] != v.RowSet[i] {
				return false
			}
		}
		return len(expected) == len(v.RowSet)
	case []*string:
		for i := range expected {
			if expected[i] != v[i] {
				return false
			}
		}
		return len(expected) == len(v)
	}
	return false
}

func assertEmptyChunkRow(row chunkRowType) bool {
	return assertEqualRows(make([]*string, len(row.RowSet)), row)
}

func TestWithStreamDownloader(t *testing.T) {
	ctx := WithStreamDownloader(context.Background())
	numrows := 100000
	cnt := 0
	var idx int
	var v string

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(forceJSON)
		rows := dbt.mustQueryContext(ctx, fmt.Sprintf(selectRandomGenerator, numrows))
		defer rows.Close()

		// Next() will block and wait until results are available
		for rows.Next() {
			if err := rows.Scan(&idx, &v); err != nil {
				t.Fatal(err)
			}
			cnt++
		}
		logger.Infof("NextResultSet: %v", rows.NextResultSet())

		if cnt != numrows {
			t.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
		}
	})
}
