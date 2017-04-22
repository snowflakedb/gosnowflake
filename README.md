# Go Snowflake Driver

A Go Snowflake Driver for Go's [database/sql](https://golang.org/pkg/database/sql/) package

## Requirements
  * Go 1.8? or higher
  * [Snowflake](https://www.snowflake.net/) Database account

## Installation
Install the package to your [$GOPATH](https://github.com/golang/go/wiki/GOPATH "GOPATH") with the [go tool](https://golang.org/cmd/go/ "go command") from shell:
```bash
$ go get github.com/snowflakedb/gosnowflake
```

## Usage
A Go Snowflake Driver is an implementation of Go's `database/sql/driver` interface.

Use `snowflake` as `driverName` and valid [DSN](#dsn-data-source-name)  as `dataSourceName`:
```go
import "database/sql"
import _ "github.com/snowflakedb/gosnowflake"

db, err := sql.Open("snowflake", "user:password@accoutname/dbname")
```

### DSN (Data Source Name)

The Data Source Name has a common format widely used by other databases.
```
[username[:password]@][accountname]/dbname/schemaname[?param1=value&...&paramN=valueN]
```

### How to run Tests
Set Snowflake connection info in `parameters.json`:
```{
    "testconnection": {
        "SNOWFLAKE_TEST_USER":      "<your_user>",
        "SNOWFLAKE_TEST_PASSWORD":  "<your_password>",
        "SNOWFLAKE_TEST_ACCOUNT":   "<your_account>",
        "SNOWFLAKE_TEST_WAREHOUSE": "<your_warehouse>",
        "SNOWFLAKE_TEST_DATABASE":  "<your_database>",
        "SNOWFLAKE_TEST_SCHEMA":    "<your_schema>",
        "SNOWFLAKE_TEST_ROLE":      "<your_role>"
    }
}```

Run `make test` in Go development environment:
```make test
```
