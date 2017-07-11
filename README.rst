********************************************************************************
Go Snowflake Driver
********************************************************************************

.. image:: https://travis-ci.org/snowflakedb/gosnowflake.svg?branch=master
    :target: https://travis-ci.org/snowflakedb/gosnowflake

.. image:: https://codecov.io/gh/snowflakedb/gosnowflake/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/snowflakedb/gosnowflake

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt

This topic provides instructions for installing, running, and modifying the Go Snowflake Driver. The driver supports Go's `database/sql <https://golang.org/pkg/database/sql/>`_ package.

Prerequisites
================================================================================

The following software packages are required to use the Go Snowflake Driver.

Go
----------------------------------------------------------------------

The driver requires the `Go language <https://golang.org/>`_ 1.8 or higher. The supported operating systems are Linux, Mac OS, and Windows, but you may run the driver on other platforms if the Go language works correctly on those platforms.

Installation
================================================================================

Import the Go Snowflake Driver package ``https://github.com/snowflakedb/gosnowflake`` along with ``database/sql`` package. Use ``snowflake`` as ``driverName`` and a valid data source name as ``dataSourceName``:

.. code-block:: go

    import (
        "database/sql"
        _ "github.com/snowflakedb/gosnowflake"
    )

    func main() {
        db, err := sql.Open("snowflake", "user:password@myaccount/mydb")
        defer db.Close()
        ...
    }

Usage
================================================================================

Connection String
----------------------------------------------------------------------

Use Open to create a database handle with connection parameters:

.. code-block:: go

    db, err := sql.Open("snowflake",
        "<connection string>")

The Go Snowflake Driver supports the following connection syntaxes (or data source name formats):

.. code-block:: none

    username[:password]@accountname/dbname/schemaname[?param1=value&...&paramN=valueN
    username[:password]@accountname/dbname[?param1=value&...&paramN=valueN
    username[:password]@hostname:port/dbname/schemaname?account=<your_account>[&param1=value&...&paramN=valueN]

The following example opens a database handle with the Snowflake account ``myaccount`` where the username is ``jsmith``, 
password is ``mypassword``, database is ``mydb``, schema is ``testschema``, and warehouse is ``mywh``:

.. code-block:: go

    db, err := sql.Open("snowflake",
        "jsmith:mypassword@myaccount/mydb/testschema?warehouse=mywh")

Connection Parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``region <string>``
  Specifies the Snowflake region. By default, the US West region is used. 

  - US East region, specify ``us-east-1``.
  - EU (Frankfurt) region, specify ``eu-central-1``.

``account <string>``
  :emph:`Required`

  Specifies the name of your Snowflake account, where *string* is the name assigned to your account by Snowflake. In the URL you received from Snowflake, your account name is the first segment in the domain (e.g. ``abc123`` in ``https://abc123.snowflakecomputing.com``).

  This parameter is optional if your account is specified after the ``@`` character.

``database``
  Specifies the database to use by default in the client session (can be changed after login). 

``schema``
  Specifies the database schema to use by default in the client session (can be changed after login). 

``warehouse``
  Specifies the virtual warehouse to use by default for queries, loading, etc. in the client session (can be changed after login). 

``role``
  Specifies the role to use by default for accessing Snowflake objects in the client session (can be changed after login). 

``passcode``
  Specifies the passcode provided by Duo when using MFA for login.

``passcodeInPassword``
  ``false`` by default. Set to ``true`` if the MFA passcode is embeded in the login password. Appends the MFA passcode to the end of the password.

``loginTimeout``
  Specifies the timeout, in seconds, for login. The default is 60 seconds. The login request gives up after the timeout length if the HTTP response is ``success``.

``authenticator``
    Specifies the authenticator to use for authenticating user credentials:

      - To use the internal Snowflake authenticator, specify ``snowflake`` (Default).
      - To authenticate through Okta, specify ``https://<okta_account_name>.okta.com`` (URL prefix for Okta).

``application``
  Identifies your application to Snowflake Support.

``insecureMode``
  ``false`` by default. Set to ``true`` to bypass the Offensive Security Certified Professional (OSCP) certificate revocation check.

  .. important::

    Change the default value for testing or emergency situations only.

``proxyHost``
  Specifies the host name for the proxy server. The proxy must be accessible via the URL http://proxyHost:proxyPort/. The proxyUser and proxyPassword parameters are optional.

  Note that SSL proxy configuration is not supported. 

``proxyPort``
  Specifies the port number for the proxy server.

``proxyUser``
  Specifies the name of the user used to connect to the proxy server. 

``proxyPassword``
  Specifies the password for the user account used to connect to the proxy server. 

Logging
----------------------------------------------------------------------

The driver uses `glog <https://github.com/golang/glog>`_ as the logging framework. To get detailed logs,
specify glog parameters in the command line. For example, to get logs for all activity, set the following parameters:

.. code-block:: bash

    your_go_program -vmodule=*=2 -stderrthreshold=INFO

To get the logs for a specific module, use the ``-vmodule`` option. For example, to retrieve the ``driver.go`` and 
``connection.go`` module logs:

.. code-block:: bash

    your_go_program -vmodule=driver=2,connection=2 -stderrthreshold=INFO

.. note::

    If your request retrieves no logs, call ``db.Close()`` or ``glog.flush()`` to flush the glog buffer.

Supported Data Types
================================================================================

Queries return SQL column type information in the `ColumnType <https://golang.org/pkg/database/sql/#ColumnType>`_ type. The `DatabaseTypeName <https://golang.org/pkg/database/sql/#ColumnType.DatabaseTypeName>`_ method returns the following strings representing Snowflake data types:

======================  ===================
String Representation   Snowflake Data Type
======================  ===================
FIXED                   NUMBER/INT
REAL                    REAL
TEXT                    VARCHAR/STRING
DATE                    DATE
TIME                    TIME
TIMESTAMP_LTZ           TIMESTAMP_LTZ
TIMESTAMP_NTZ           TIMESTAMP_NTZ
TIMESTAMP_TZ            TIMESTAMP_TZ
VARIANT                 VARIANT
OBJECT                  OBJECT
ARRAY                   ARRAY
BINARY                  BINARY
BOOLEAN                 BOOLEAN
======================  ===================

Binding the ``time.Time`` Type
----------------------------------------------------------------------

Go's `database/sql <https://golang.org/pkg/database/sql/>`_ package limits Go's data types to the following for binding and fetching:

.. code-block:: none

    int64
    float64
    bool
    []byte
    string
    time.Time

Fetching data isn't an issue since the database data type is provided along with the data so the Go Snowflake Driver can translate Snowflake data types to Go native data types.

When the client binds data to send to the server, however, the driver cannot determine the date/timestamp data types to associate with binding parameters. For example:

.. code-block:: go

    dbt.mustExec("CREATE OR REPLACE TABLE tztest (id int, ntz, timestamp_ntz, ltz timestamp_ltz)")
    // ...
    stmt, err :=dbt.db.Prepare("INSERT INTO tztest(id,ntz,ltz) VALUES(1, ?, ?)")
    // ...
    tmValue time.Now()
    // ... Is tmValue a TIMESTAMP_NTZ or TIMESTAMP_LTZ?
    _, err = stmt.Exec(tmValue, tmValue)

To resolve this issue, a binding parameter flag is introduced that associates any subsequent ``time.Time`` type to the ``DATE``, ``TIME``, ``TIMESTAMP_LTZ``, ``TIMESTAMP_NTZ`` or ``BINARY`` data type. The above example could be rewritten as follows:

.. code-block:: go

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

Timestamps with Time Zones
----------------------------------------------------------------------

The driver fetches ``TIMESTAMP_TZ`` (timestamp with time zone) data using the offset-based ``Location`` types, which represent a collection of time offsets in use in a geographical area, such as CET (Central European Time) or UTC (Coordinated Universal Time). The offset-based ``Location`` data is generated and cached when a Go Snowflake Driver application starts, and if the given offset is not in the cache, it is generated dynamically.

Currently, Snowflake doesn't support the name-based ``Location`` types, e.g., ``America/Los_Angeles``. 

For more information about ``Location`` types, see the `Go documentation for Location <https://golang.org/pkg/time/#Location>`_. 

Binary Data
----------------------------------------------------------------------

Internally, this feature leverages the ``[]byte`` data type. As a result, ``BINARY`` data cannot be bound without the binding parameter flag. In the following example, ``sf`` is an alias for the ``gosnowflake`` package:

.. code-block:: go

    var b = []byte{0x01, 0x02, 0x03}
    _, err = stmt.Exec(sf.DataTypeBinary, b)

Limitations
================================================================================

This section describes the current limitations of the Go Snowflake Driver.

PUT and GET Support
----------------------------------------------------------------------

Currently, ``GET`` and ``PUT`` operations are unsupported.

Sample Programs
================================================================================

Snowflake provides a set of sample programs to test with. Set the environment variable ``$GOPATH`` to the top directory of your workspace, e.g., ``~/go`` and make certain to 
include ``$GOPATH/bin`` in the environment variable ``$PATH``. Run the ``make`` command to build all sample programs.

.. code-block:: go

    make install

In the following example, the program ``select1.go`` is built and installed in ``$GOPATH/bin`` and can be run from the command line:

.. code-block:: bash

    SNOWFLAKE_TEST_ACCOUNT=<your_account> \
    SNOWFLAKE_TEST_USER=<your_user> \
    SNOWFLAKE_TEST_PASSWORD=<your_password> \
    select1
    Congrats! You have successfully run SELECT 1 with Snowflake DB!

Development
================================================================================

The developer notes are hosted with the source code on `GitHub <https://github.com/snowflakedb/gosnowflake>`_.

Testing Code
----------------------------------------------------------------------

Set the Snowflake connection info in ``parameters.json``:

.. code-block:: json

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

Run ``make test`` in your Go development environment:

.. code-block:: bash

    make test

Submitting Pull Requests
----------------------------------------------------------------------

You may use your preferred editor to edit the driver code. Make certain to run ``make fmt lint`` before submitting any pull request to Snowflake. This command formats your source code according to the standard Go style and detects any coding style issues.
