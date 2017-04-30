package gosnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	user       string
	pass       string
	account    string
	dbname     string
	schemaname string
	warehouse  string
	rolename   string
	dsn        string
	host       string
	port       string
	protocol   string
)

// The tests require the following parameters in the environment variables.
// SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PASSWORD, SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_DATABASE,
// SNOWFLAKE_TEST_SCHEMA, SNOWFLAKE_TEST_WAREHOUSE.
// Optionally you may specify SNOWFLAKE_TEST_PROTOCOL, SNOWFLAKE_TEST_HOST and SNOWFLAKE_TEST_PORT to specify
// the endpoint.
func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user = env("SNOWFLAKE_TEST_USER", "testuser")
	pass = env("SNOWFLAKE_TEST_PASSWORD", "testpassword")
	account = env("SNOWFLAKE_TEST_ACCOUNT", "testaccount")
	dbname = env("SNOWFLAKE_TEST_DATABASE", "testdb")
	schemaname = env("SNOWFLAKE_TEST_SCHEMA", "public")
	rolename = env("SNOWFLAKE_TEST_ROLE", "sysadmin")
	warehouse = env("SNOWFLAKE_TEST_WAREHOUSE", "testwarehouse")

	protocol = env("SNOWFLAKE_TEST_PROTOCOL", "https")
	host = os.Getenv("SNOWFLAKE_TEST_HOST")
	port = os.Getenv("SNOWFLAKE_TEST_PORT")
	if host == "" {
		host = fmt.Sprintf("%s.snowflakecomputing.com", account)
	} else {
		host = fmt.Sprintf("%s:%s", host, port)
	}

	dsn = fmt.Sprintf("%s:%s@%s/%s/%s", user, pass, host, dbname, schemaname)

	parameters := url.Values{}
	parameters.Add("timezone", "UTC") // TODO: do we want to support this? This is good for tests.
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

type DBTest struct {
	*testing.T
	db *sql.DB
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
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

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	_, err = db.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		t.Fatalf("failed to drop table: %v", err)
	}

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
	}
}

func TestBogusUserPasswordParameters(t *testing.T) {
	invalidDNS := fmt.Sprintf("%s:%s@%s", "bogus", pass, host)
	invalidUserPassErrorTests(invalidDNS, t)
	invalidDNS = fmt.Sprintf("%s:%s@%s", user, "INVALID_PASSWORD", host)
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
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("should cause an error.")
	}
	if driverErr, ok := err.(*SnowflakeError); ok {
		if driverErr.Number != 390100 {
			t.Fatalf("wrong error code: %v", driverErr)
		}
	} else {
		t.Fatalf("wrong error code: %v", err)
	}
}

func TestBogusHostNameParameters(t *testing.T) {
	invalidDNS := fmt.Sprintf("%s:%s@%s", user, pass, "INVALID_HOST:1234")
	invalidHostErrorTests(invalidDNS, "no such host", "", t)
	invalidDNS = fmt.Sprintf("%s:%s@%s", user, pass, "INVALID_HOST")
	invalidHostErrorTests(invalidDNS, "read: connection reset by peer.", "EOF", t)
}
func invalidHostErrorTests(invalidDNS string, match1 string, match2 string, t *testing.T) {
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
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("should cause an error.")
	}
	if !strings.Contains(err.Error(), match1) && !strings.Contains(err.Error(), match2) {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestEmptyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		query := "--"
		// just a comment, no query
		_, err := dbt.db.Query(query)
		if err == nil {
			dbt.fail("query", query, err)
		}
		if driverErr, ok := err.(*SnowflakeError); ok {
			if driverErr.Number != 900 { // syntax error
				dbt.fail("query", query, err)
			}
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
		if rows.Next() {
			rows.Scan(&out)
			if true != out {
				dbt.Errorf("true != %t", out)
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
		if rows.Next() {
			rows.Scan(&out)
			if false != out {
				dbt.Errorf("false != %t", out)
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

func TestSchemaWarehouseIncludingSpace(t *testing.T) {
	newSchemaName := "TEST SCHEMA"
	newWarehouseName := "TEST WAREHOUSE"
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf(`CREATE OR REPLACE SCHEMA "%v"`, newSchemaName))
		dbt.mustExec(fmt.Sprintf(`CREATE OR REPLACE WAREHOUSE "%v"`, newWarehouseName))
	})
	newDSN := fmt.Sprintf("%s:%s@%s/%s/%s", user, pass, host, dbname, url.QueryEscape(newSchemaName))
	parameters := url.Values{}
	parameters.Add("warehouse", newWarehouseName)
	if protocol != "" {
		parameters.Add("protocol", protocol)
	}
	if account != "" {
		parameters.Add("account", account)
	}
	newDSN += "?" + parameters.Encode()
	db, err := sql.Open("snowflake", newDSN)
	if err != nil {
		t.Fatalf("failed to connect. DSN: %v", newDSN)
	}
	defer db.Close()
	rows, err := db.Query("SELECT CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("failed to get the current database")
	}
	var gotSchemaName string
	var gotWarehouseName string
	if err := rows.Scan(&gotSchemaName, &gotWarehouseName); err != nil {
		t.Fatalf("failed to scan schema and warehouse names. err: %v", err)
	}
	if gotSchemaName != newSchemaName {
		t.Fatalf("failed to match schema name. expected: %v, got: %v", newSchemaName, gotSchemaName)
	}
	if gotWarehouseName != newWarehouseName {
		t.Fatalf("failed to match warehouse name. expected: %v, got: %v", newWarehouseName, gotWarehouseName)
	}
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(fmt.Sprintf(`DROP WAREHOUSE IF EXISTS "%v"`, newWarehouseName))
		dbt.mustExec(fmt.Sprintf(`DROP SCHEMA IF EXISTS "%v"`, newSchemaName))
	})
}

func TestInt(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"INT", "INTEGER"}
		in := int64(42)
		var out int64
		var rows *sql.Rows

		// SIGNED
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
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
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		in := float32(42.23)
		var out float32
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM test")
			if rows.Next() {
				rows.Scan(&out)
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
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")
			dbt.mustExec("INSERT INTO test VALUES (42.23)")
			rows = dbt.mustQuery("SELECT value FROM test")
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

func TestFloat64Placeholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		expected := 42.23
		var out float64
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec(fmt.Sprintf("CREATE TABLE test (id int, value %v)", v))
			dbt.mustExec("INSERT INTO test VALUES (1, ?)", expected)
			rows = dbt.mustQuery("SELECT value FROM test WHERE id = ?", 1)
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

func TestDateTimeTimestampPlaceholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec(
			"CREATE OR REPLACE TABLE tztest (id int, ntz timestamp_ntz, ltz timestamp_ltz, dt date, tm time)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz,dt,tm) VALUES(1,?,?,?,?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec(
			DataTypeTimestampNtz, expected,
			DataTypeTimestampLtz, expected,
			DataTypeDate, expected,
			DataTypeTime, expected)
		if err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT ntz,ltz,dt,tm FROM tztest WHERE id=?", 1)
		defer rows.Close()
		var ntz, vltz, dt, tm time.Time
		if rows.Next() {
			rows.Scan(&ntz, &vltz, &dt, &tm)
			if expected.UnixNano() != ntz.UnixNano() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, ntz.UnixNano(), ntz)
			}
			if expected.UnixNano() != vltz.UnixNano() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, vltz.UnixNano(), vltz)
			}
			if expected.Year() != dt.Year() || expected.Month() != dt.Month() || expected.Day() != dt.Day() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.Unix()*1000, expected, dt.Unix()*1000, dt)
			}
			if expected.Hour() != tm.Hour() || expected.Minute() != tm.Minute() || expected.Second() != tm.Second() || expected.Nanosecond() != tm.Nanosecond() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, tm.UnixNano(), tm)
			}
			// fmt.Printf("returned value: %v, %v, %v\n", v, v.UnixNano(), expected.UnixNano())
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})
}

/*
TODO: not working as TIMESTAMP_TZ binding is not supported yet
func TestTimestampTZPlaceholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		expected := time.Now()
		dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, tz timestamp_tz)")
		stmt, err := dbt.db.Prepare("INSERT INTO tztest(id,tz) VALUES(1, ?)")
		if err != nil {
			dbt.Fatal(err.Error())
		}
		defer stmt.Close()
		_, err = stmt.Exec(DataTypeTimestampTz, expected)
		if err != nil {
			dbt.Fatal(err)
		}
		rows := dbt.mustQuery("SELECT tz FROM tztest WHERE id=?", 1)
		defer rows.Close()
		var v time.Time
		if rows.Next() {
			rows.Scan(&v)
			if expected.UnixNano() != v.UnixNano() {
				dbt.Errorf("returned value didn't match. expected: %v:%v, got: %v:%v",
					expected.UnixNano(), expected, v.UnixNano(), v)
			}
			// fmt.Printf("returned value: %v, %v, %v\n", v, v.UnixNano(), expected.UnixNano())
		} else {
			dbt.Error("no data")
		}
		dbt.mustExec("DROP TABLE tztest")
	})
}
*/

func TestString(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := []string{"CHAR(255)", "VARCHAR(255)", "TEXT", "STRING"}
		in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
		var out string
		var rows *sql.Rows

		for _, v := range types {
			dbt.mustExec("CREATE TABLE test (value " + v + ")")

			dbt.mustExec("INSERT INTO test VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM test")
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
		in = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. " +
			"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
		dbt.mustExec("INSERT INTO test VALUES (?, ?)", id, in)

		err := dbt.db.QueryRow("SELECT value FROM test WHERE id = ?", id).Scan(&out)
		if err != nil {
			dbt.Fatalf("Error on BLOB-Query: %s", err.Error())
		} else if out != in {
			dbt.Errorf("BLOB: %s != %s", in, out)
		}
	})
}

type timeTests struct {
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
	var rows *sql.Rows
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
	err = rows.Scan(&dst)
	if err != nil {
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
		if val == tt.t {
			return
		}
		t.Logf("source:%v, expected: %v, got:%v", tt.s, tt.t, val)
		dbt.Errorf("%s to string: expected %q, got %q",
			dbtype,
			tt.s,
			val.Format(tlayout),
		)
	default:
		fmt.Printf("%#v\n", []interface{}{dbtype, tlayout, tt.s, tt.t})
		dbt.Errorf("%s: unhandled type %T (is '%v')",
			dbtype, val, val,
		)
	}
}

func TestSimpleDateTimeTimestampFetch(t *testing.T) {
	var scan = func(rows *sql.Rows, cd interface{}, ct interface{}, cts interface{}) {
		err := rows.Scan(cd, ct, cts)
		if err != nil {
			t.Fatal(err)
		}
		// fmt.Printf("cd: %v, ct: %v, cts: %v", cd, ct, cts)
		// no error should occurs
	}
	var fetchTypes = []func(*sql.Rows){
		func(rows *sql.Rows) {
			var cd, ct, cts time.Time
			scan(rows, &cd, &ct, &cts)
		},
		func(rows *sql.Rows) {
			var cd, ct, cts time.Time
			scan(rows, &cd, &ct, &cts)
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
		for _, f := range fetchTypes {
			rows := dbt.mustQuery("SELECT CURRENT_DATE(), CURRENT_TIME(), CURRENT_TIMESTAMP()")
			if rows.Next() {
				f(rows)
			} else {
				t.Fatal("no results")
			}
		}
	})
}

func TestDateTime(t *testing.T) {
	afterTime := func(t time.Time, d string) time.Time {
		dur, err := time.ParseDuration(d)
		if err != nil {
			panic(err)
		}
		return t.Add(dur)
	}
	format := "2006-01-02 15:04:05.999999999"
	t0 := time.Time{}
	tstr0 := "0000-00-00 00:00:00.000000000"
	testcases := []timeTests{
		{"DATE", format[:10], []timeTest{
			{t: time.Date(2011, 11, 20, 0, 0, 0, 0, time.UTC)},
			{t: time.Date(2, 8, 2, 0, 0, 0, 0, time.UTC), s: "0002-08-02"},
			// 0000-00-00 is not supported but returns a consistent result
			{t: time.Date(2, 11, 30, 0, 0, 0, 0, time.UTC), s: "0000-00-00"},
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
	format := "2006-01-02 15:04:05.999999999"
	testcases := []timeTests{
		{
			dbtype:  "TIMESTAMP_LTZ(9)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2016-12-30 05:02:03",
					t: time.Date(2016, 12, 30, 5, 2, 3, 0, time.Local),
				},
				{
					s: "2017-05-12 00:51:42",
					t: time.Date(2017, 5, 12, 0, 51, 42, 0, time.Local),
				},
				{
					s: "2017-03-12 01:00:00",
					t: time.Date(2017, 3, 12, 1, 0, 0, 0, time.Local),
				},
				{
					s: "2017-03-13 04:00:00",
					t: time.Date(2017, 3, 13, 4, 0, 0, 0, time.Local),
				},
				{
					s: "2017-03-13 04:00:00.123456789",
					t: time.Date(2017, 3, 13, 4, 0, 0, 123456789, time.Local),
				},
			},
		},
		{
			dbtype:  "TIMESTAMP_LTZ(8)",
			tlayout: format,
			tests: []timeTest{
				{
					s: "2017-03-13 04:00:00.123456789",
					t: time.Date(2017, 3, 13, 4, 0, 0, 123456780, time.Local),
				},
			},
		},
	}
	runTests(t, dsn, func(dbt *DBTest) {
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

func TestTimestampTZ(t *testing.T) {
	sflo := func(offsets string) (loc *time.Location) {
		r, err := LocationWithOffsetString(offsets)
		if err != nil {
			return time.UTC
		}
		return r
	}
	format := "2006-01-02 15:04:05.999999999"
	testcases := []timeTests{
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
	runTests(t, dsn, func(dbt *DBTest) {
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
		} else if nb.Bool != true {
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
			dbt.Error("non-nil []byte wich should be nil")
		}
		// Read non-nil
		if err = nonNullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil []byte wich should be non-nil")
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

func TestLargeSetResult(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		numrows := 10000
		rows := dbt.mustQuery(fmt.Sprintf("SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))", numrows))
		defer rows.Close()
		cnt := 0
		var idx int
		var v string
		for rows.Next() {
			err := rows.Scan(&idx, &v)
			if err != nil {
				t.Fatal(err)
			}
			// fmt.Printf("%v, %v\n", idx, v)
			cnt++
		}
		if cnt != numrows {
			dbt.Errorf("number of rows didn't match. expected: %v, got: %v", cnt, numrows)
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
			dbt.Errorf("number of rows didn't match. expected: %v, got: %v", cnt, numrows)
		}
	})
}

func TestDML(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE TABLE test(c1 int, c2 string)")
		err := insertData(dbt, false)
		results, err := queryTest(dbt)
		if err != nil {
			dbt.Fatalf("failed to query test table: %v", err)
		}
		if len(*results) != 0 {
			dbt.Fatalf("number of returned data didn't match. expected 0, got: %v", len(*results))
		}
		err = insertData(dbt, true)
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
		err = tx.Commit()
		if err != nil {
			return err
		}
	} else {
		err = tx.Rollback()
		if err != nil {
			return err
		}
	}
	return err
}

func queryTestTx(tx *sql.Tx) (*map[int]string, error) {
	var c1 int
	var c2 string
	rows, err := tx.Query("SELECT c1, c2 FROM test")
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	results := make(map[int]string, 2)
	for rows.Next() {
		err := rows.Scan(&c1, &c2)
		if err != nil {
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
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	results := make(map[int]string, 2)
	for rows.Next() {
		err := rows.Scan(&c1, &c2)
		if err != nil {
			return nil, err
		}
		results[c1] = c2
	}
	return &results, nil
}

// Special cases where rows are already closed
func TestRowsClose(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows, err := dbt.db.Query("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if rows.Next() {
			dbt.Fatal("unexpected row after rows.Close()")
		}

		err = rows.Err()
		if err != nil {
			dbt.Fatal(err)
		}
	})
}

func TestCancelQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		_, err := dbt.db.QueryContext(ctx, "SELECT DISTINCT 1 FROM TABLE(GENERATOR(TIMELIMIT=> 10))")

		if err == nil {
			dbt.Fatal("No timeout error returned")
		}

		if err.Error() != "context deadline exceeded" {
			dbt.Fatal("Timeout failed")
		}
	})
}
