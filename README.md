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

WIP