# Changelog

## Upcoming Release

New features:

- 
- 
- 
- 
- 
- 
- 
- Added support for identity impersonation when using workload identity federation (snowflakedb/gosnowflake#1652, snowflakedb/gosnowflake#1660).
- 

Bug fixes:

-
-
-
- Fixed minicore file name gathering in client environment (snowflakedb/gosnowflake#1661).
-
-
-
-

Internal changes:

-
-
-
-
-
-
-
-
-

## 1.18.2

Bug fixes:

- Handle HTTP307 & 308 in drivers to achieve better resiliency to backend errors (snowflakedb/gosnowflake#1616).
- Create temp directory only if needed during file transfer (snowflakedb/gosnowflake#1647)
- Fix unnecessary user expansion for file paths (snowflakedb/gosnowflake#1646).

Internal changes:
- Remove spammy "telemetry disabled" log messages (snowflakedb/gosnowflake#1638).
- Introduced shared library for extended telemetry to identify and prepare testing platform for native rust extensions (snowflakedb/gosnowflake#1629)

## 1.18.0

New features:

- Added validation of CRL `NextUpdate` for freshly downloaded CRLs (snowflakedb/gosnowflake#1617)
- Exposed function to send arbitrary telemetry data (snowflakedb/gosnowflake#1627)
- Added logging of query text and parameters (snowflakedb/gosnowflake#1625)

Bug fixes:

- Fixed a data race error in tests caused by platform_detection init() function (snowflakedb/gosnowflake#1618)
- Make secrets detector initialization thread safe and more maintainable (snowflakedb/gosnowflake#1621)

Internal changes:

- Added ISA to login request telemetry (snowflakedb/gosnowflake#1620)

## 1.17.1

- Fix unsafe reflection of nil pointer on DECFLOAT func in bind uploader (snowflakedb/gosnowflake#1604).
- Added temporary download files cleanup (snowflakedb/gosnowflake#1577)
- Marked fields as deprecated (snowflakedb/gosnowflake#1556)
- Exposed `QueryStatus` from `SnowflakeResult` and `SnowflakeRows` in `GetStatus()` function (snowflakedb/gosnowflake#1556)
- Split timeout settings into separate groups based on target service types (snowflakedb/gosnowflake#1531)
- Added small clarification in oauth.go example on token escaping (snowflakedb/gosnowflake#1574)
- Ensured proper permissions for CRL cache directory (snowflakedb/gosnowflake#1588)
- Added `CrlDownloadMaxSize` to limit the size of CRL downloads (snowflakedb/gosnowflake#1588)
- Added platform telemetry to login requests. Can be disabled with `SNOWFLAKE_DISABLE_PLATFORM_DETECTION` environment variable (snowflakedb/gosnowflake#1601)
- Bypassed proxy settings for WIF metadata requests (snowflakedb/gosnowflake#1593)
- Fixed a bug where GCP PUT/GET operations would fail when the connection context was cancelled (snowflakedb/gosnowflake#1584)
- Fixed nil pointer dereference while calling long-running queries (snowflakedb/gosnowflake#1592) (snowflakedb/gosnowflake#1596)
- Moved keyring-based secure storage manager into separate file to avoid the need to initialize keyring on Linux. (snowflakedb/gosnowflake#1595)
- Enabling official support for RHEL9 by testing and enabling CI/CD checks for Rocky Linux in CICD, (snowflakedb/gosnowflake#1597)
- Improve logging (snowflakedb/gosnowflake#1570)

## 1.17.0

- Added ability to configure OCSP per connection (snowflakedb/gosnowflake#1528)
- Added `DECFLOAT` support, see details in `doc.go` (snowflakedb/gosnowflake#1504, snowflakedb/gosnowflake#1506)
- Added support for Go 1.25, dropped support for Go 1.22 (snowflakedb/gosnowflake#1544)
- Added proxy options to connection parameters (snowflakedb/gosnowflake#1511)
- Added `client_session_keep_alive_heartbeat_frequency` connection param (snowflakedb/gosnowflake#1576)
- Added support for multi-part downloads for S3, Azure and GCP (snowflakedb/gosnowflake#1549)
- Added `singleAuthenticationPrompt` to control whether only one authentication should be performed at the same time for authentications that need human interactions (like MFA or OAuth authorization code). Default is true. (snowflakedb/gosnowflake#1561)
- Fixed missing `DisableTelemetry` option in connection parameters (snowflakedb/gosnowflake#1520)
- Fixed multistatements in large result sets (snowflakedb/gosnowflake#1539, snowflakedb/gosnowflake#1543, snowflakedb/gosnowflake#1547)
- Fixed unnecessary retries when context is cancelled (snowflakedb/gosnowflake#1540)
- Fixed regression in TOML connection file (snowflakedb/gosnowflake#1530)

## Prior Releases

Release notes available at https://docs.snowflake.com/en/release-notes/clients-drivers/golang
