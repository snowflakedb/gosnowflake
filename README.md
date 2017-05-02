# Go Snowflake Driver

Snowflake provides a driver for Go's [database/sql](https://golang.org/pkg/database/sql/) SQL package

**Warning: No production use is recommended as the current version of the Go Snowflake driver is being 
actively developed and doesn't meet all of the security requirements for Snowflake clients. See 
[Limitations](#limitations) section for details.**

## Requirements
  * Go 1.8 or higher
  * [Snowflake](https://www.snowflake.net/) account

## Installation
From a terminal window, install the package to your [$GOPATH](https://github.com/golang/go/wiki/GOPATH "GOPATH") path using the 
[go tool](https://golang.org/cmd/go/ "go command"):
```bash
$ go get github.com/snowflakedb/gosnowflake
```

## Usage
Go Snowflake Driver is an implementation of Go's `database/sql/driver` interface.

Use `snowflake` as `driverName` and a valid [DSN](#dsn-data-source-name) as `dataSourceName`:
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

For example, if your account is `testaccount`, username is `testuser` password is `testpass`, database 
is `testdb` schema is `testschema` and warehouse is `testwarehouse` the DSN will be as follows:
```golang
db, err := sql.Open("snowflake",
    "testuser:testpass@testaccount/testdb/testschema?warehouse=testwarehouse")
```

### Logging
Go Snowflake Driver uses [glog](https://github.com/golang/glog) as a logging framework. In order to get the detail logs,
specify ``glog`` parameters in the command line. For example, if you want to get logs for all activity, set the following 
command line parameters:
```bash
$ your_go_program -vmodule=*=2 -stderrthreshold=INFO
```
If you want to get the logs for a specific module, use the ``-vmodule`` option, for example, for ``driver.go`` and 
``connection.go``:
```bash
$ your_go_program -vmodule=driver=2,connection=2 -stderrthreshold=INFO
```

### Binding time.Time for DATE, TIME, TIMESTAMP_NTZ, TIMESTAMP_LTZ
_This behavior is subject to change by the production._

Go's [database/sql](https://golang.org/pkg/database/sql/) limits Go's data types to the following for binding and fetching.
```
int64
float64
bool
[]byte
string
time.Time
```
https://golang.org/pkg/database/sql/driver/#Value

Fetching data doesn't have a problem as the database data type is provided along with data so that Go Snowflake Driver can translate them to Golang native data types.

Binding data, however, has a challenge, because Go Snowflake Driver doesn't know the data type but binding parameter requires the database data type as well. For example:
```go
dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, ntz, timestamp_ntz, ltz timestamp_ltz)")
// ...
stmt, err :=dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz) VALUES(1, ?, ?)")
// ...
tmValue time.Now()
// ... How can this tell tmValue is for TIMESTAMP_NTZ or TIMESTAMP_LTZ?
_, err = stmt.Exec(tmValue, tmValue)
```

Go Snowflake Driver introduces a concept of binding parameter flag that indicates subsequent data types for `DATE`, `TIME`, `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ` and `BINARY`. In the previous example, you may rewrite to the following.
```go
import (
    sf "github.com/snowflakedb/gosnowflake"
)
dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, ntz, timestamp_ntz, ltz timestamp_ltz)")
// ...
stmt, err :=dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz) VALUES(1, ?, ?)")
// ...
tmValue time.Now()
// ... 
_, err = stmt.Exec(sf.DataTypeTimestampNtz, tmValue, sf.DataTypeTimestampLtz, tmValue)
```

Internally this feature leverages `[]byte` data type. As a result, `BINARY` data cannot be bound without the flag. Suppose `sf` is an alias of `gosnowflake` package, here is an example:
```
var b = []byte{0x01, 0x02, 0x03}
_, err = stmt.Exec(sf.DataTypeBinary, b)
```

### Offset based Location / Timezone type
Go Snowflake Driver fetches ``TIMESTAMP_TZ`` data along with the offset based ``Location`` types, which represent timezones by offset to UTC. The offset based ``Location`` are generated and cached when Go Snowflake Driver application starts, and if the given offset is not in the cache, it will be dynamically generated.

At the moment, Snowflake doesn't support the name based ``Location`` types, e.g., ``America/Los_Angeles``. See [Data Types](https://docs.snowflake.net/manuals/sql-reference/data-types.html) for the Snowflake data type specification.

## Limitations
### Security Requirements
Security is the highest-priority consideration for any aspect of the Snowflake service. Snowflake clients must 
communicate with a Snowflake database. Typically, HTTPS (HTTP over TLS/SSL) is used for the communication layer;
if the TLS/SSL layer is used, the client must meet the following requirements:
  - [x] TLS/SSL must validate all of the chained certificates toward the root CA certificate.
  - [x] TLS/SSL must match the hostname with the certificate hostname.
  - [ ] TLS/SSL must validate the certificate revocation status.

Since Go 1.8.1 has not implemented the certification revocation check yet, we plan to implement it ourselves in the 
production version of the Go Snowflake driver unless Go provides this security feature first. Before the production 
version is ready, consider the risk of the missing 
[certificate revocation check](https://en.wikipedia.org/wiki/Certificate_revocation_list) if you want to use the driver.

### Binding TIMESTAMP_TZ
At the moment, binding ``TIMESTAMP_TZ`` data type is not supported.

## Sample Programs
Set the environment variable ``$GOPATH`` to the top directory of your workspace, e.g., ``~/godev`` and ensure to 
include ``$GOPATH/bin`` in the environment variable ``$PATH``. Run make command to build all sample programs.
```bash
make install
```
For example, ``select1.go`` program is built and installed in ``$GOPATH/bin`` so that you can run it in commandline:
```bash
$ SNOWFLAKE_TEST_ACCOUNT=<your_account> \
  SNOWFLAKE_TEST_USER=<your_user> \
  SNOWFLAKE_TEST_PASSWORD=<your_password> \
  sample1
Congrats! You have successfully run SELECT 1 with Snowflake DB!
```

## Development
### Change Codes
You may use your favorite editor to edit codes. But ensure running ``make fmt lint`` before submitting PR.

### Test Codes
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

Run `make test` in the Go development environment:
```
make test
```
