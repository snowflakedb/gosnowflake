# Changelog

## Upcoming Release

- Added temporary download files cleanup (snowflakedb/gosnowflake#1577)
- Marked fields as deprecated (snowflakedb/gosnowflake#1556)
- Exposed `QueryStatus` from `SnowflakeResult` and `SnowflakeRows` in `GetStatus()` function (snowflakedb/gosnowflake#1556)
- Split timeout settings into separate groups based on target service types (snowflakedb/gosnowflake#1531)
- Added small clarification in oauth.go example on token escaping (snowflakedb/gosnowflake#1574)
- Ensured proper permissions for CRL cache directory (snowflakedb/gosnowflake#1588)
- Added `CrlDownloadMaxSize` to limit the size of CRL downloads (snowflakedb/gosnowflake#1588)
- 
- 
- 
- Fixed nil pointer derereference while calling long running queries (snowflakedb/gosnowflake#1592)
- 
- 
- 
- 
- 
- 

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
