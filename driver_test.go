// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

var (
	username         string
	pass             string
	account          string
	dbname           string
	schemaname       string
	warehouse        string
	rolename         string
	dsn              string
	host             string
	port             string
	protocol         string
	customPrivateKey bool            // Whether user has specified the private key path
	testPrivKey      *rsa.PrivateKey // Valid private key used for all test cases
)

const (
	selectNumberSQL       = "SELECT %s::NUMBER(%v, %v) AS C"
	selectVariousTypes    = "SELECT 1.0::NUMBER(30,2) as C1, 2::NUMBER(38,0) AS C2, 't3' AS C3, 4.2::DOUBLE AS C4, 'abcd'::BINARY AS C5, true AS C6"
	selectRandomGenerator = "SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))"
	PSTLocation           = "America/Los_Angeles"
)

// The tests require the following parameters in the environment variables.
// SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PASSWORD, SNOWFLAKE_TEST_ACCOUNT,
// SNOWFLAKE_TEST_DATABASE, SNOWFLAKE_TEST_SCHEMA, SNOWFLAKE_TEST_WAREHOUSE.
// Optionally you may specify SNOWFLAKE_TEST_PROTOCOL, SNOWFLAKE_TEST_HOST
// and SNOWFLAKE_TEST_PORT to specify the endpoint.
func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	username = env("SNOWFLAKE_TEST_USER", "testuser")
	pass = env("SNOWFLAKE_TEST_PASSWORD", "testpassword")
	account = env("SNOWFLAKE_TEST_ACCOUNT", "testaccount")
	dbname = env("SNOWFLAKE_TEST_DATABASE", "testdb")
	schemaname = env("SNOWFLAKE_TEST_SCHEMA", "public")
	rolename = env("SNOWFLAKE_TEST_ROLE", "sysadmin")
	warehouse = env("SNOWFLAKE_TEST_WAREHOUSE", "testwarehouse")

	protocol = env("SNOWFLAKE_TEST_PROTOCOL", "https")
	host = os.Getenv("SNOWFLAKE_TEST_HOST")
	port = env("SNOWFLAKE_TEST_PORT", "443")
	if host == "" {
		host = fmt.Sprintf("%s.snowflakecomputing.com", account)
	} else {
		host = fmt.Sprintf("%s:%s", host, port)
	}

	setupPrivateKey()

	createDSN("UTC")
}

func createDSN(timezone string) {
	dsn = fmt.Sprintf("%s:%s@%s/%s/%s", username, pass, host, dbname, schemaname)

	parameters := url.Values{}
	parameters.Add("timezone", timezone)
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	if warehouse != "" {
		parameters.Add("warehouse", warehouse)
	}
	if rolename != "" {
		parameters.Add("role", rolename)
	}

	if len(parameters) > 0 {
		dsn += "?" + parameters.Encode()
	}
}

// setup creates a test schema so that all tests can run in the same schema
func setup() (string, error) {
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	orgSchemaname := schemaname
	if env("GITHUB_WORKFLOW", "") != "" {
		githubRunnerID := env("RUNNER_TRACKING_ID", "GITHUB_RUNNER_ID")
		githubRunnerID = strings.ReplaceAll(githubRunnerID, "-", "_")
		githubSha := env("GITHUB_SHA", "GITHUB_SHA")
		schemaname = fmt.Sprintf("%v_%v", githubRunnerID, githubSha)
	} else {
		schemaname = fmt.Sprintf("golang_%v", time.Now().UnixNano())
	}
	var db *sql.DB
	var err error
	if db, err = sql.Open("snowflake", dsn); err != nil {
		return "", fmt.Errorf("failed to open db. err: %v", err)
	}
	defer db.Close()
	if _, err = db.Exec(fmt.Sprintf("CREATE OR REPLACE SCHEMA %v", schemaname)); err != nil {
		return "", fmt.Errorf("failed to create schema. %v", err)
	}
	createDSN("UTC")
	return orgSchemaname, nil
}

// teardown drops the test schema
func teardown() error {
	var db *sql.DB
	var err error
	if db, err = sql.Open("snowflake", dsn); err != nil {
		return fmt.Errorf("failed to open db. %v, err: %v", dsn, err)
	}
	defer db.Close()
	if _, err = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %v", schemaname)); err != nil {
		return fmt.Errorf("failed to create schema. %v", err)
	}
	return nil
}

func TestMain(m *testing.M) {
	flag.Parse()
	signal.Ignore(syscall.SIGQUIT)
	if value := os.Getenv("SKIP_SETUP"); value != "" {
		os.Exit(m.Run())
	}

	if _, err := setup(); err != nil {
		panic(err)
	}
	ret := m.Run()
	if err := teardown(); err != nil {
		panic(err)
	}
	os.Exit(ret)
}

type DBTest struct {
	*testing.T
	db *sql.DB
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *RowsExtended) {
	// handler interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	c0 := make(chan bool, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		select {
		case <-c:
			fmt.Println("Caught signal, canceling...")
			cancel()
		case <-ctx.Done():
			fmt.Println("Done")
		case <-c0:
		}
		close(c)
	}()

	rs, err := dbt.db.QueryContext(ctx, query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return &RowsExtended{
		rows:      rs,
		closeChan: &c0,
	}
}

func (dbt *DBTest) mustQueryContext(ctx context.Context, query string, args ...interface{}) (rows *RowsExtended) {
	// handler interrupt signal
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	c0 := make(chan bool, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		select {
		case <-c:
			fmt.Println("Caught signal, canceling...")
			cancel()
		case <-ctx.Done():
			fmt.Println("Done")
		case <-c0:
		}
		close(c)
	}()

	rs, err := dbt.db.QueryContext(ctx, query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return &RowsExtended{
		rows:      rs,
		closeChan: &c0,
	}
}

func (dbt *DBTest) mustQueryAssertCount(query string, expected int, args ...interface{}) {
	rows := dbt.mustQuery(query, args...)
	cnt := 0
	for rows.Next() {
		cnt++
	}
	if cnt != expected {
		dbt.Fatalf("expected %v, got %v", expected, cnt)
	}
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s [%s]: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustExecContext(ctx context.Context, query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.ExecContext(ctx, query, args...)
	if err != nil {
		dbt.fail("exec context", query, err)
	}
	return res
}

func (dbt *DBTest) mustDecimalSize(ct *sql.ColumnType) (pr int64, sc int64) {
	var ok bool
	pr, sc, ok = ct.DecimalSize()
	if !ok {
		dbt.Fatalf("failed to get decimal size. %v", ct)
	}
	return pr, sc
}

func (dbt *DBTest) mustFailDecimalSize(ct *sql.ColumnType) {
	var ok bool
	if _, _, ok = ct.DecimalSize(); ok {
		dbt.Fatalf("should not return decimal size. %v", ct)
	}
}

func (dbt *DBTest) mustLength(ct *sql.ColumnType) (cLen int64) {
	var ok bool
	cLen, ok = ct.Length()
	if !ok {
		dbt.Fatalf("failed to get length. %v", ct)
	}
	return cLen
}

func (dbt *DBTest) mustFailLength(ct *sql.ColumnType) {
	var ok bool
	if _, ok = ct.Length(); ok {
		dbt.Fatalf("should not return length. %v", ct)
	}
}

func (dbt *DBTest) mustNullable(ct *sql.ColumnType) (canNull bool) {
	var ok bool
	canNull, ok = ct.Nullable()
	if !ok {
		dbt.Fatalf("failed to get length. %v", ct)
	}
	return canNull
}

func (dbt *DBTest) mustPrepare(query string) (stmt *sql.Stmt) {
	stmt, err := dbt.db.Prepare(query)
	if err != nil {
		dbt.fail("prepare", query, err)
	}
	return stmt
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	if _, err = db.Exec("DROP TABLE IF EXISTS test"); err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

func runningOnAWS() bool {
	return os.Getenv("CLOUD_PROVIDER") == "AWS"
}

func runningOnGCP() bool {
	return os.Getenv("CLOUD_PROVIDER") == "GCP"
}

func TestBogusUserPasswordParameters(t *testing.T) {
	invalidDNS := fmt.Sprintf("%s:%s@%s", "bogus", pass, host)
	invalidUserPassErrorTests(invalidDNS, t)
	invalidDNS = fmt.Sprintf("%s:%s@%s", username, "INVALID_PASSWORD", host)
	invalidUserPassErrorTests(invalidDNS, t)
}

func invalidUserPassErrorTests(invalidDNS string, t *testing.T) {
	parameters := url.Values{}
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	invalidDNS += "?" + parameters.Encode()
	db, err := sql.Open("snowflake", invalidDNS)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	// actual connection won't happen until run a query
	defer db.Close()
	if _, err = db.Exec("SELECT 1"); err == nil {
		t.Fatal("should cause an error.")
	}
	if driverErr, ok := err.(*SnowflakeError); ok {
		if driverErr.Number != 390100 {
			t.Fatalf("wrong error code: %v", driverErr)
		}
		if !strings.Contains(driverErr.Error(), "390100") {
			t.Fatalf("error message should included the error code. got: %v", driverErr.Error())
		}
	} else {
		t.Fatalf("wrong error code: %v", err)
	}
}

func TestBogusHostNameParameters(t *testing.T) {
	invalidDNS := fmt.Sprintf("%s:%s@%s", username, pass, "INVALID_HOST:1234")
	invalidHostErrorTests(invalidDNS, []string{"no such host", "verify account name is correct", "HTTP Status: 403", "Temporary failure in name resolution"}, t)
	invalidDNS = fmt.Sprintf("%s:%s@%s", username, pass, "INVALID_HOST")
	invalidHostErrorTests(invalidDNS, []string{"read: connection reset by peer", "EOF", "verify account name is correct", "HTTP Status: 403", "Temporary failure in name resolution"}, t)
}

func invalidHostErrorTests(invalidDNS string, mstr []string, t *testing.T) {
	parameters := url.Values{}
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	parameters.Add("loginTimeout", "10")
	invalidDNS += "?" + parameters.Encode()
	db, err := sql.Open("snowflake", invalidDNS)
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	// actual connection won't happen until run a query
	defer db.Close()
	if _, err = db.Exec("SELECT 1"); err == nil {
		t.Fatal("should cause an error.")
	}
	found := false
	for _, m := range mstr {
		if strings.Contains(err.Error(), m) {
			found = true
		}
	}
	if !found {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestCommentOnlyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		query := "--"
		// just a comment, no query
		rows, err := dbt.db.Query(query)
		if err == nil {
			rows.Close()
			dbt.fail("query", query, err)
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != 900 { // syntax error
				dbt.fail("query", query, err)
			}
		}
	})
}

func TestEmptyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		query := "select 1 from dual where 1=0"
		// just a comment, no query
		rows := dbt.db.QueryRow(query)
		var v1 interface{}
		if err := rows.Scan(&v1); err != sql.ErrNoRows {
			dbt.Errorf("should fail. err: %v", err)
		}
		rows = dbt.db.QueryRowContext(context.Background(), query)
		if err := rows.Scan(&v1); err != sql.ErrNoRows {
			dbt.Errorf("should fail. err: %v", err)
		}
	})
}

func TestEmptyQueryWithRequestID(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		query := "select 1"
		ctx := WithRequestID(context.Background(), NewUUID())
		rows := dbt.db.QueryRowContext(ctx, query)
		var v1 interface{}
		if err := rows.Scan(&v1); err != nil {
			dbt.Errorf("should not have failed with valid request id. err: %v", err)
		}
	})
}

func TestCRUD(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE test (value BOOLEAN)")

		// Test for unexpected Data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM test")
		defer rows.Close()
		if rows.Next() {
			dbt.Error("unexpected Data in empty table")
		}

		// Create Data
		res := dbt.mustExec("INSERT INTO test VALUES (true)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != -1 {
			dbt.Fatalf(
				"expected InsertId -1, got %d. Snowflake doesn't support last insert ID", id)
		}

		// Read
		rows = dbt.mustQuery("SELECT value FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if !out {
				dbt.Errorf("%t should be true", out)
			}
			if rows.Next() {
				dbt.Error("unexpected Data")
			}
		} else {
			dbt.Error("no Data")
		}

		// Update
		res = dbt.mustExec("UPDATE test SET value = ? WHERE value = ?", false, true)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if out {
				dbt.Errorf("%t should be true", out)
			}
			if rows.Next() {
				dbt.Error("unexpected Data")
			}
		} else {
			dbt.Error("no Data")
		}

		// Delete
		res = dbt.mustExec("DELETE FROM test WHERE value = ?", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM test")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 0 {
			dbt.Fatalf("expected 0 affected row, got %d", count)
		}
	})
}

func TestInt(t *testing.T) {
	testInt(t, false)
}

func testInt(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"INT", "INTEGER"}
		in := int64(42)
		var out int64
		var rows *RowsExtended

		// SIGNED
		for _, v := range types {
			if json {
				dbt.mustExec(forceJSON)
			}
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %d != %d", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}

			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat32(t *testing.T) {
	testFloat32(t, false)
}

func testFloat32(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		in := float32(42.23)
		var out float32
		var rows *RowsExtended
		for _, v := range types {
			if json {
				dbt.mustExec(forceJSON)
			}
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				err := rows.Scan(&out)
				if err != nil {
					dbt.Errorf("failed to scan data: %v", err)
				}
				if in != out {
					dbt.Errorf("%s: %g != %g", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestFloat64(t *testing.T) {
	testFloat64(t, false)
}

func testFloat64(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *RowsExtended
		for _, v := range types {
			if json {
				dbt.mustExec(forceJSON)
			}
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (42.23)")
			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}
	})
}

func TestString(t *testing.T) {
	testString(t, false)
}

func testString(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		types := []string{"CHAR(255)", "VARCHAR(255)", "TEXT", "STRING"}
		in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
		var out string
		var rows *RowsExtended

		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM test")
			defer rows.Close()
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %s != %s", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			dbt.mustExec("DROP TABLE IF EXISTS test")
		}

		// BLOB (Snowflake doesn't support BLOB type but STRING covers large text data)
		dbt.mustExec("CREATE TABLE test (id int, value STRING)")

		id := 2
		in = `Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam
			nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam
			erat, sed diam voluptua. At vero eos et accusam et justo duo
			dolores et ea rebum. Stet clita kasd gubergren, no sea takimata
			sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet,
			consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt
			ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero
			eos et accusam et justo duo dolores et ea rebum. Stet clita kasd
			gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.`
		dbt.mustExec("INSERT INTO test VALUES (?, ?)", id, in)

		if err := dbt.db.QueryRow("SELECT value FROM test WHERE id = ?", id).Scan(&out); err != nil {
			dbt.Fatalf("Error on BLOB-Query: %s", err.Error())
		} else if out != in {
			dbt.Errorf("BLOB: %s != %s", in, out)
		}
	})
}

type tcDateTimeTimestamp struct {
	dbtype  string
	tlayout string
	tests   []timeTest
}

type timeTest struct {
	s string    // source date time string
	t time.Time // expected fetched data
}

func (tt timeTest) genQuery() string {
	return "SELECT '%s'::%s"
}

func (tt timeTest) run(t *testing.T, dbt *DBTest, dbtype, tlayout string) {
	var rows *RowsExtended
	query := fmt.Sprintf(tt.genQuery(), tt.s, dbtype)
	rows = dbt.mustQuery(query)
	defer rows.Close()
	var err error
	if !rows.Next() {
		err = rows.Err()
		if err == nil {
			err = fmt.Errorf("no data")
		}
		dbt.Errorf("%s: %s", dbtype, err)
		return
	}

	var dst interface{}
	if err = rows.Scan(&dst); err != nil {
		dbt.Errorf("%s: %s", dbtype, err)
		return
	}
	switch val := dst.(type) {
	case []uint8:
		str := string(val)
		if str == tt.s {
			return
		}
		dbt.Errorf("%s to string: expected %q, got %q",
			dbtype,
			tt.s,
			str,
		)
	case time.Time:
		if val.UnixNano() == tt.t.UnixNano() {
			return
		}
		t.Logf("source:%v, expected: %v, got:%v", tt.s, tt.t, val)
		dbt.Errorf("%s to string: expected %q, got %q",
			dbtype,
			tt.s,
			val.Format(tlayout),
		)
	default:
		dbt.Errorf("%s: unhandled type %T (is '%v')",
			dbtype, val, val,
		)
	}
}

func TestSimpleDateTimeTimestampFetch(t *testing.T) {
	testSimpleDateTimeTimestampFetch(t, false)
}

func testSimpleDateTimeTimestampFetch(t *testing.T, json bool) {
	var scan = func(rows *RowsExtended, cd interface{}, ct interface{}, cts interface{}) {
		if err := rows.Scan(cd, ct, cts); err != nil {
			t.Fatal(err)
		}
	}
	var fetchTypes = []func(*RowsExtended){
		func(rows *RowsExtended) {
			var cd, ct, cts time.Time
			scan(rows, &cd, &ct, &cts)
		},
		func(rows *RowsExtended) {
			var cd, ct, cts time.Time
			scan(rows, &cd, &ct, &cts)
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		for _, f := range fetchTypes {
			rows := dbt.mustQuery("SELECT CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIMESTAMP()")
			defer rows.Close()
			if rows.Next() {
				f(rows)
			} else {
				t.Fatal("no results")
			}
		}
	})
}

func TestDateTime(t *testing.T) {
	testDateTime(t, false)
}

func testDateTime(t *testing.T, json bool) {
	afterTime := func(t time.Time, d string) time.Time {
		dur, err := time.ParseDuration(d)
		if err != nil {
			panic(err)
		}
		return t.Add(dur)
	}
	t0 := time.Time{}
	tstr0 := "0000-00-00 00:00:00.000000000"
	testcases := []tcDateTimeTimestamp{
		{"DATE", format[:10], []timeTest{
			{t: time.Date(2011, 11, 20, 0, 0, 0, 0, time.UTC)},
			{t: time.Date(2, 8, 2, 0, 0, 0, 0, time.UTC), s: "0002-08-02"},
		}},
		{"TIME", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(0)", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(1)", format[11:21], []timeTest{
			{t: afterTime(t0, "12345600ms")},
			{t: t0, s: tstr0[11:21]},
		}},
		{"TIME(6)", format[11:], []timeTest{
			{t: t0, s: tstr0[11:]},
		}},
		{"DATETIME", format[:19], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
		}},
		{"DATETIME(0)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
		}},
		{"DATETIME(1)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 100000000, time.UTC)},
		}},
		{"DATETIME(6)", format, []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 123456000, time.UTC)},
		}},
		{"DATETIME(9)", format, []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 123456789, time.UTC)},
		}},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		for _, setups := range testcases {
			for _, setup := range setups.tests {
				if setup.s == "" {
					// fill time string wherever Go can reliable produce it
					setup.s = setup.t.Format(setups.tlayout)
				}
				setup.run(t, dbt, setups.dbtype, setups.tlayout)
			}
		}
	})
}

func TestTimestampLTZ(t *testing.T) {
	testTimestampLTZ(t, false)
}

func testTimestampLTZ(t *testing.T, json bool) {
	// Set session time zone in Los Angeles, same as machine
	createDSN(PSTLocation)
	location, err := time.LoadLocation(PSTLocation)
	if err != nil {
		t.Error(err)
	}
	testcases := []tcDateTimeTimestamp{
		{
			dbtype:  "TIMESTAMP_LTZ(9)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2016-12-30 05:02:03",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0, location),
				},
				{
					s: "2016-12-30 05:02:03 -00:00",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0, time.UTC),
				},
				{
					s: "2017-05-12 00:51:42",
					t: time.Date(2017, 5, 12, 0, 51, 42, 0, location),
				},
				{
					s: "2017-03-12 01:00:00",
					t: time.Date(2017, 3, 12, 1, 0, 0, 0, location),
				},
				{
					s: "2017-03-13 04:00:00",
					t: time.Date(2017, 3, 13, 4, 0, 0, 0, location),
				},
				{
					s: "2017-03-13 04:00:00.123456789",
					t: time.Date(2017, 3, 13, 4, 0, 0, 123456789, location),
				},
			},
		},
		{
			dbtype:  "TIMESTAMP_LTZ(8)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2017-03-13 04:00:00.123456789",
					t: time.Date(2017, 3, 13, 4, 0, 0, 123456780, location),
				},
			},
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		for _, setups := range testcases {
			for _, setup := range setups.tests {
				if setup.s == "" {
					// fill time string wherever Go can reliable produce it
					setup.s = setup.t.Format(setups.tlayout)
				}
				setup.run(t, dbt, setups.dbtype, setups.tlayout)
			}
		}
	})
	// Revert timezone to UTC, which is default for the test suit
	createDSN("UTC")
}

func TestTimestampTZ(t *testing.T) {
	testTimestampTZ(t, false)
}

func testTimestampTZ(t *testing.T, json bool) {
	sflo := func(offsets string) (loc *time.Location) {
		r, err := LocationWithOffsetString(offsets)
		if err != nil {
			return time.UTC
		}
		return r
	}
	testcases := []tcDateTimeTimestamp{
		{
			dbtype:  "TIMESTAMP_TZ(9)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2016-12-30 05:02:03 +07:00",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0,
						sflo("+0700")),
				},
				{
					s: "2017-05-23 03:56:41 -09:00",
					t: time.Date(2017, 5, 23, 3, 56, 41, 0,
						sflo("-0900")),
				},
			},
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		for _, setups := range testcases {
			for _, setup := range setups.tests {
				if setup.s == "" {
					// fill time string wherever Go can reliable produce it
					setup.s = setup.t.Format(setups.tlayout)
				}
				setup.run(t, dbt, setups.dbtype, setups.tlayout)
			}
		}
	})
}

func TestNULL(t *testing.T) {
	testNULL(t, false)
}

func testNULL(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		nullStmt, err := dbt.db.Prepare("SELECT NULL")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nullStmt.Close()

		nonNullStmt, err := dbt.db.Prepare("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nonNullStmt.Close()

		// NullBool
		var nb sql.NullBool
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if nb.Valid {
			dbt.Error("valid NullBool which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if !nb.Valid {
			dbt.Error("invalid NullBool which should be valid")
		} else if !nb.Bool {
			dbt.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
		}

		// NullFloat64
		var nf sql.NullFloat64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if nf.Valid {
			dbt.Error("valid NullFloat64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if !nf.Valid {
			dbt.Error("invalid NullFloat64 which should be valid")
		} else if nf.Float64 != float64(1) {
			dbt.Errorf("unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
		}

		// NullInt64
		var ni sql.NullInt64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if ni.Valid {
			dbt.Error("valid NullInt64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if !ni.Valid {
			dbt.Error("invalid NullInt64 which should be valid")
		} else if ni.Int64 != int64(1) {
			dbt.Errorf("unexpected NullInt64 value: %d (should be 1)", ni.Int64)
		}

		// NullString
		var ns sql.NullString
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if ns.Valid {
			dbt.Error("valid NullString which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if !ns.Valid {
			dbt.Error("invalid NullString which should be valid")
		} else if ns.String != `1` {
			dbt.Error("unexpected NullString value:" + ns.String + " (should be `1`)")
		}

		// nil-bytes
		var b []byte
		// Read nil
		if err = nullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil []byte which should be nil")
		}
		// Read non-nil
		if err = nonNullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil []byte which should be non-nil")
		}
		// Insert nil
		b = nil
		success := false
		if err = dbt.db.QueryRow("SELECT ? IS NULL", b).Scan(&success); err != nil {
			dbt.Fatal(err)
		}
		if !success {
			dbt.Error("inserting []byte(nil) as NULL failed")
			t.Fatal("stopping")
		}
		// Check input==output with input==nil
		b = nil
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil echo from nil input")
		}
		// Check input==output with input!=nil
		b = []byte("")
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil echo from non-nil input")
		}

		// Insert NULL
		dbt.mustExec("CREATE TABLE test (dummmy1 int, value int, dummy2 int)")
		dbt.mustExec("INSERT INTO test VALUES (?, ?, ?)", 1, nil, 2)

		var out interface{}
		rows := dbt.mustQuery("SELECT * FROM test")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if out != nil {
				dbt.Errorf("%v != nil", out)
			}
		} else {
			dbt.Error("no data")
		}
	})
}

func TestVariant(t *testing.T) {
	testVariant(t, false)
}

func testVariant(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		rows := dbt.mustQuery(`select parse_json('[{"id":1, "name":"test1"},{"id":2, "name":"test2"}]')`)
		defer rows.Close()
		var v string
		if rows.Next() {
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
		} else {
			t.Fatal("no rows")
		}
	})
}

func TestArray(t *testing.T) {
	testArray(t, false)
}

func testArray(t *testing.T, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		rows := dbt.mustQuery(`select as_array(parse_json('[{"id":1, "name":"test1"},{"id":2, "name":"test2"}]'))`)
		defer rows.Close()
		var v string
		if rows.Next() {
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
		} else {
			t.Fatal("no rows")
		}
	})
}

func TestLargeSetResult(t *testing.T) {
	CustomJSONDecoderEnabled = false
	testLargeSetResult(t, 100000, false)
}

func testLargeSetResult(t *testing.T, numrows int, json bool) {
	runTests(t, dsn, func(dbt *DBTest) {
		if json {
			dbt.mustExec(forceJSON)
		}
		rows := dbt.mustQuery(fmt.Sprintf(selectRandomGenerator, numrows))
		defer rows.Close()
		cnt := 0
		var idx int
		var v string
		for rows.Next() {
			if err := rows.Scan(&idx, &v); err != nil {
				t.Fatal(err)
			}
			cnt++
		}
		logger.Infof("NextResultSet: %v", rows.NextResultSet())

		if cnt != numrows {
			dbt.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
		}
	})
}

func TestPingpongQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		numrows := 1
		rows := dbt.mustQuery("SELECT DISTINCT 1 FROM TABLE(GENERATOR(TIMELIMIT=> 60))")
		defer rows.Close()
		cnt := 0
		for rows.Next() {
			cnt++
		}
		if cnt != numrows {
			dbt.Errorf("number of rows didn't match. expected: %v, got: %v", numrows, cnt)
		}
	})
}

func TestDML(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test(c1 int, c2 string)")
		if err := insertData(dbt, false); err != nil {
			dbt.Fatalf("failed to insert data: %v", err)
		}
		results, err := queryTest(dbt)
		if err != nil {
			dbt.Fatalf("failed to query test table: %v", err)
		}
		if len(*results) != 0 {
			dbt.Fatalf("number of returned data didn't match. expected 0, got: %v", len(*results))
		}
		if err = insertData(dbt, true); err != nil {
			dbt.Fatalf("failed to insert data: %v", err)
		}
		results, err = queryTest(dbt)
		if err != nil {
			dbt.Fatalf("failed to query test table: %v", err)
		}
		if len(*results) != 2 {
			dbt.Fatalf("number of returned data didn't match. expected 2, got: %v", len(*results))
		}
	})
}

func insertData(dbt *DBTest, commit bool) error {
	tx, err := dbt.db.Begin()
	if err != nil {
		dbt.Fatalf("failed to begin transaction: %v", err)
	}
	res, err := tx.Exec("INSERT INTO test VALUES(1, 'test1'), (2, 'test2')")
	if err != nil {
		dbt.Fatalf("failed to insert value into test: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		dbt.Fatalf("failed to rows affected: %v", err)
	}
	if n != 2 {
		dbt.Fatalf("failed to insert value into test. expected: 2, got: %v", n)
	}
	results, err := queryTestTx(tx)
	if err != nil {
		dbt.Fatalf("failed to query test table: %v", err)
	}
	if len(*results) != 2 {
		dbt.Fatalf("number of returned data didn't match. expected 2, got: %v", len(*results))
	}
	if commit {
		if err = tx.Commit(); err != nil {
			return err
		}
	} else {
		if err = tx.Rollback(); err != nil {
			return err
		}
	}
	return err
}

func queryTestTx(tx *sql.Tx) (*map[int]string, error) {
	var c1 int
	var c2 string
	rows, err := tx.Query("SELECT c1, c2 FROM test")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make(map[int]string, 2)
	for rows.Next() {
		if err = rows.Scan(&c1, &c2); err != nil {
			return nil, err
		}
		results[c1] = c2
	}
	return &results, nil
}

func queryTest(dbt *DBTest) (*map[int]string, error) {
	var c1 int
	var c2 string
	rows, err := dbt.db.Query("SELECT c1, c2 FROM test")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make(map[int]string, 2)
	for rows.Next() {
		if err = rows.Scan(&c1, &c2); err != nil {
			return nil, err
		}
		results[c1] = c2
	}
	return &results, nil
}

func TestCancelQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := dbt.db.QueryContext(ctx, "SELECT DISTINCT 1 FROM TABLE(GENERATOR(TIMELIMIT=> 100))")
		if err == nil {
			dbt.Fatal("No timeout error returned")
		}
		if err.Error() != "context deadline exceeded" {
			dbt.Fatalf("Timeout error mismatch: expect %v, receive %v", context.DeadlineExceeded, err.Error())
		}
	})
}

func TestPing(t *testing.T) {
	db := openDB(t)
	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping. err: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("failed to ping with context. err: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close db. err: %v", err)
	}
	if err := db.Ping(); err == nil {
		t.Fatal("should have failed to ping")
	}
	if err := db.PingContext(context.Background()); err == nil {
		t.Fatal("should have failed to ping with context")
	}
}

func TestDoubleDollar(t *testing.T) {
	// no escape is required for dollar signs
	runTests(t, dsn, func(dbt *DBTest) {
		sql := `create or replace function dateErr(I double) returns date
language javascript strict
as $$
  var x = [
    0, "1400000000000",
    "2013-04-05",
    [], [1400000000000],
    "x1234",
    Number.NaN, null, undefined,
    {},
    [1400000000000,1500000000000]
  ];
  return x[I];
$$
;`
		dbt.mustExec(sql)
	})
}

func TestTimezoneSessionParameter(t *testing.T) {
	createDSN(PSTLocation)
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query("SHOW PARAMETERS LIKE 'TIMEZONE'")
	if err != nil {
		t.Errorf("failed to run show parameters. err: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("failed to get timezone.")
	}

	p, err := ScanSnowflakeParameter(rows)
	if err != nil {
		t.Errorf("failed to run get timezone value. err: %v", err)
	}
	if p.Value != PSTLocation {
		t.Errorf("failed to get an expected timezone. got: %v", p.Value)
	}
	createDSN("UTC")
}

func TestLargeSetResultCancel(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		c := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			// attempt to run a 100 seconds query, but it should be canceled in 1 second
			timelimit := 100
			rows, err := dbt.db.QueryContext(
				ctx,
				fmt.Sprintf("SELECT COUNT(*) FROM TABLE(GENERATOR(timelimit=>%v))", timelimit))
			if err != nil {
				c <- err
				return
			}
			defer rows.Close()
			c <- nil
		}()
		// cancel after 1 second
		time.Sleep(time.Second)
		cancel()
		ret := <-c
		if ret.Error() != "context canceled" {
			t.Fatalf("failed to cancel. err: %v", ret)
		}
		close(c)
	})
}

func TestValidateDatabaseParameter(t *testing.T) {
	baseDSN := fmt.Sprintf("%s:%s@%s", username, pass, host)
	testcases := []struct {
		dsn       string
		params    map[string]string
		errorCode int
	}{
		{
			dsn:       baseDSN + fmt.Sprintf("/%s/%s", "NOT_EXISTS", "NOT_EXISTS"),
			errorCode: ErrObjectNotExistOrAuthorized,
		},
		{
			dsn:       baseDSN + fmt.Sprintf("/%s/%s", dbname, "NOT_EXISTS"),
			errorCode: ErrObjectNotExistOrAuthorized,
		},
		{
			dsn: baseDSN + fmt.Sprintf("/%s/%s", dbname, schemaname),
			params: map[string]string{
				"warehouse": "NOT_EXIST",
			},
			errorCode: ErrObjectNotExistOrAuthorized,
		},
		{
			dsn: baseDSN + fmt.Sprintf("/%s/%s", dbname, schemaname),
			params: map[string]string{
				"role": "NOT_EXIST",
			},
			errorCode: ErrRoleNotExist,
		},
	}
	for idx, tc := range testcases {
		newDSN := tc.dsn
		parameters := url.Values{}
		if protocol != "" {
			parameters.Add("protocol", protocol)
		}
		if account != "" {
			parameters.Add("account", account)
		}
		for k, v := range tc.params {
			parameters.Add(k, v)
		}
		newDSN += "?" + parameters.Encode()
		db, err := sql.Open("snowflake", newDSN)
		// actual connection won't happen until run a query
		if err != nil {
			t.Fatalf("error creating a connection object: %s", err.Error())
		}
		defer db.Close()
		if _, err = db.Exec("SELECT 1"); err == nil {
			t.Fatal("should cause an error.")
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != tc.errorCode { // not exist error
				t.Errorf("got unexpected error: %v in %v", err, idx)
			}
		}
	}
}

func TestSpecifyWarehouseDatabase(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@%s/%s", username, pass, host, dbname)
	parameters := url.Values{}
	parameters.Add("account", account)
	parameters.Add("warehouse", warehouse)
	// parameters.Add("role", "nopublic") TODO: create nopublic role for test
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	db, err := sql.Open("snowflake", dsn+"?"+parameters.Encode())
	if err != nil {
		t.Fatalf("error creating a connection object: %s", err.Error())
	}
	defer db.Close()
	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Fatalf("failed to execute a select 1: %v", err)
	}
}

func TestFetchNil(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT * FROM values(3,4),(null, 5) order by 2")
		defer rows.Close()
		var c1 sql.NullInt64
		var c2 sql.NullInt64

		var results []sql.NullInt64
		for rows.Next() {
			if err := rows.Scan(&c1, &c2); err != nil {
				dbt.Fatal(err)
			}
			results = append(results, c1)
		}
		if results[1].Valid {
			t.Errorf("First element of second row must be nil (NULL). %v", results)
		}
	})
}

func TestPingInvalidHost(t *testing.T) {
	config := Config{
		Account:      "NOT_EXISTS",
		User:         "BOGUS_USER",
		Password:     "barbar",
		LoginTimeout: 10 * time.Second,
	}

	testURL, err := DSN(&config)
	if err != nil {
		t.Fatalf("failed to parse config. config: %v, err: %v", config, err)
	}

	db, err := sql.Open("snowflake", testURL)
	if err != nil {
		t.Fatalf("failed to initalize the connetion. err: %v", err)
	}
	ctx := context.Background()
	if err = db.PingContext(ctx); err == nil {
		t.Fatal("should cause an error")
	}
	if driverErr, ok := err.(*SnowflakeError); !ok || ok && driverErr.Number != ErrCodeFailedToConnect {
		// Failed to connect error
		t.Fatalf("error didn't match")
	}
}

func TestOpenWithConfig(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn. err: %v", err)
	}
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	if err != nil {
		t.Fatalf("failed to open with config. config: %v, err: %v", config, err)
	}
	db.Close()
}

type CountingTransport struct {
	requests int
}

func (t *CountingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.requests++
	return snowflakeInsecureTransport.RoundTrip(r)
}

func TestOpenWithTransport(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn. err: %v", err)
	}
	countingTransport := CountingTransport{}
	var transport http.RoundTripper = &countingTransport
	config.Transporter = transport
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	if err != nil {
		t.Fatalf("failed to open with config. config: %v, err: %v", config, err)
	}
	conn := db.(*snowflakeConn)
	if conn.rest.Client.Transport != transport {
		t.Fatal("transport doesn't match")
	}
	db.Close()
	if countingTransport.requests == 0 {
		t.Fatal("transport did not receive any requests")
	}

	// Test that transport override also works in insecure mode
	countingTransport.requests = 0
	config.InsecureMode = true
	db, err = driver.OpenWithConfig(context.Background(), *config)
	if err != nil {
		t.Fatalf("failed to open with config. config: %v, err: %v", config, err)
	}
	conn = db.(*snowflakeConn)
	if conn.rest.Client.Transport != transport {
		t.Fatal("transport doesn't match")
	}
	db.Close()
	if countingTransport.requests == 0 {
		t.Fatal("transport did not receive any requests")
	}
}

func createDSNWithClientSessionKeepAlive() {
	dsn = fmt.Sprintf("%s:%s@%s/%s/%s", username, pass, host, dbname, schemaname)

	parameters := url.Values{}
	parameters.Add("client_session_keep_alive", "true")
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	if warehouse != "" {
		parameters.Add("warehouse", warehouse)
	}
	if rolename != "" {
		parameters.Add("role", rolename)
	}
	if len(parameters) > 0 {
		dsn += "?" + parameters.Encode()
	}
}

func TestClientSessionKeepAliveParameter(t *testing.T) {
	// This test doesn't really validate the CLIENT_SESSION_KEEP_ALIVE functionality but simply checks
	// the session parameter.
	createDSNWithClientSessionKeepAlive()
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("SHOW PARAMETERS LIKE 'CLIENT_SESSION_KEEP_ALIVE'")
		if !rows.Next() {
			t.Fatal("failed to get timezone.")
		}

		p, err := ScanSnowflakeParameter(rows.rows)
		if err != nil {
			t.Errorf("failed to run get client_session_keep_alive value. err: %v", err)
		}
		if p.Value != "true" {
			t.Fatalf("failed to get an expected client_session_keep_alive. got: %v", p.Value)
		}

		rows = dbt.mustQuery("select count(*) from table(generator(timelimit=>30))")
		defer rows.Close()
	})
}

func TestTimePrecision(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table z3 (t1 time(5))")
		rows := dbt.mustQuery("select * from z3")
		cols, err := rows.ColumnTypes()
		if err != nil {
			t.Error(err)
		}
		if pres, _, ok := cols[0].DecimalSize(); pres != 5 || !ok {
			t.Fatalf("Wrong value returned. Got %v instead of 5.", pres)
		}
	})
}
