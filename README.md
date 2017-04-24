# Go Snowflake Driver

Go Snowflake Driver for Go's [database/sql](https://golang.org/pkg/database/sql/) package

**Warning: No production use is recommended as the current version of Go Snowflake driver is being 
actively developed and doesn't meet all of the security requirements for Snowflake clients. See 
[Limitations](#Limitations) section for details.**

## Requirements
  * Go 1.8(TBD) or higher
  * [Snowflake](https://www.snowflake.net/) Database account

## Installation
Install the package to your [$GOPATH](https://github.com/golang/go/wiki/GOPATH "GOPATH") with the 
[go tool](https://golang.org/cmd/go/ "go command") from shell:
```bash
$ go get github.com/snowflakedb/gosnowflake
```

## Usage
Go Snowflake Driver is an implementation of Go's `database/sql/driver` interface.

Use `snowflake` as `driverName` and valid [DSN](#dsn-data-source-name)  as `dataSourceName`:
```golang
import "database/sql"
import _ "github.com/snowflakedb/gosnowflake"

db, err := sql.Open("snowflake", "user:password@accoutname/dbname")
```

### DSN (Data Source Name)
The Data Source Name (DSN) has a common format widely used by other databases.
```
username[:password]@accountname/dbname/schemaname[?param1=value&...&paramN=valueN
username[:password]@accountname/dbname[?param1=value&...&paramN=valueN
username[:password]@hostname:port/dbname/schemaname[?param1=value&...&paramN=valueN
```

For example, if your account is `testaccount`, user name is `testuser` password is `testpass`, database 
is `testdb` schema is `testschema` and warehouse is `testwarehouse` the DSN will be as follows:
```golang
db, err := sql.Open("snowflake",
    "testaccount:testpass@testaccount/testdb/testschema?warehouse=testwarehouse")
```

### Logging
Go Snowflake Driver uses [glog](https://github.com/golang/glog) as a logging framework. In order to get the detail logs,
specify ``glog`` parameters in the command line. For example, if you want to get logs for all activity, set the 
command line parameters:
```bash
$ your_go_program -vmodule=*=2
```
If you want to log specific module, use ``-vmodule`` option, for example, for ``driver.go`` and ``connection.go``:
```bash
$ your_go_program -vmodule=driver=2,connection=2
```

## Limitations
### Security Requirements
Snowflake takes security as one of the top priority feature in products. Snowflake clients must 
communicate with Snowflake database. Typically HTTPS (HTTP over TLS/SSL) is used for communication layer,
if TLS/SSL layer is used, they must meet the following requirements:
  - [x] TLS/SSL must validate all of the chained certificates towards the root CA certificate.
  - [x] TLS/SSL must match hostname with the certificate hostname.
  - [ ] TLS/SSL must validate certificate revocation status.

Since Go 1.8.1 has not implemented the certification revocation check yet, we plan to implement it in Go 
Snowflake driver by production version unless Go does. By production veresion, you might want to use the 
driver but consider a risk of missing [certificate revocation check](https://en.wikipedia.org/wiki/Certificate_revocation_list).


## Development
### Build
(WIP)

### Test
Set Snowflake connection info in `parameters.json`:
```json
{
    "testconnection": {
        "SNOWFLAKE_TEST_USER":      "<your_user>",
        "SNOWFLAKE_TEST_PASSWORD":  "<your_password>",
        "SNOWFLAKE_TEST_ACCOUNT":   "<your_account>",
        "SNOWFLAKE_TEST_WAREHOUSE": "<your_warehouse>",
        "SNOWFLAKE_TEST_DATABASE":  "<your_database>",
        "SNOWFLAKE_TEST_SCHEMA":    "<your_schema>",
        "SNOWFLAKE_TEST_ROLE":      "<your_role>"
    }
}
```

Run `make test` in Go development environment:
```
make test
```
