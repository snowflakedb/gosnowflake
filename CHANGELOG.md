# Changelog

## Upcoming release

New features:

Bug fixes:

Internal changes:

## 2.0.2

New features:

- Added `QueryResultFormatProvider` interface to expose the server-reported query result format ("arrow" or "json") from `QueryArrowStream`, enabling callers to distinguish Arrow IPC from JSON responses before interpreting batch streams (snowflakedb/gosnowflake#1773).

Bug fixes:

- Fixed empty `Account` when connecting with programmatic `Config` and `database/sql.Connector` by deriving `Account` from the first DNS label of `Host` in `FillMissingConfigParameters` when `Host` matches the Snowflake hostname pattern (snowflakedb/gosnowflake#1772).
- Fixed PAT (Programmatic Access Token) `authenticator` to actually require the Token or TokenFilePath field, instead of silently accepting Password which was never forwarded (snowflakedb/gosnowflake#1772).
- Fix logger reporting incorrect source location when called without `WithContext` (snowflakedb/gosnowflake#1768).
- GCP WIF attestation now uses hostname `metadata.google.internal` instead of the IPv4 link-local address, so it works on IPv6-only GCP VMs (snowflakedb/gosnowflake#1775).
- Fixed query failures on large inline results (e.g. 64MB LOB) caused by truncated HTTP response bodies. The driver now retries the query when `json.Decoder` returns `io.ErrUnexpectedEOF`, reusing the same request ID so Snowflake returns the cached result (snowflakedb/gosnowflake#1777).

## 2.0.1

Bug fixes:

- Fixed default `CrlDownloadMaxSize` to be 20MB instead of 200MB, as the previous value was set too high and could cause out-of-memory issues (snowflakedb/gosnowflake#1735).
- Replaced global `paramsMutex` with per-connection `syncParams` to encapsulate parameter synchronization and avoid cross-connection contention (snowflakedb/gosnoflake#1747).
- `Config.Params` map is not modified anymore, to avoid changing parameter values across connections of the same connection pool (snowflakedb/gosnowflake#1747).
- Set `BlobContentMD5` on Azure uploads so that multi-part uploads have the blob content-MD5 property populated (snowflakedb/gosnowflake#1757).
- Fixed 403 errors from Google/GCP/GCS PUT queries on versioned stages (snowflakedb/gosnowflake#1760).
- Fixed not updating query context cache for failed queries (snowflakedb/gosnowflake#1763).

Internal changes:

- Moved configuration to a dedicated internal package (snowflakedb/gosnowflake#1720).
- Modernized Go syntax idioms throughout the codebase.
- Added libc family, version and dynamic linking marker to client environment telemetry (snowflakedb/gosnowflake#1750).
- Bumped a few libraries to fix vulnerabilities (snowflakedb/gosnowflake#1751, snowflakedb/gosnowflake#1756).
- Depointerised query context cache in `snowflakeConn` (snowflakedb/gosnowflake#1763).

## 2.0.0

Breaking changes:

- Removed `RaisePutGetError` from `SnowflakeFileTransferOptions` - current behaviour is aligned to always raise errors for PUT/GET operations (snowflakedb/gosnowflake#1690).
- Removed `GetFileToStream` from `SnowflakeFileTransferOptions` - using `WithFileGetStream` automatically enables file streaming for GETs (snowflakedb/gosnowflake#1690).
- Renamed `WithFileStream` to `WithFilePutStream` for consistency (snowflakedb/gosnowflake#1690).
- `Array` function now returns error for unsupported types (snowflakedb/gosnowflake#1693).
- `WithMultiStatement` does not return error anymore (snowflakedb/gosnowflake#1693).
- `WithOriginalTimestamp` is removed, use `WithArrowBatchesTimestampOption(UseOriginalTimestamp)` instead (snowflakedb/gosnowflake#1693).
- `WithMapValuesNullable` and `WithArrayValuesNullable` combined into one option `WithEmbeddedValuesNullable` (snowflakedb/gosnowflake#1693).
- Hid streaming chunk downloader. It will be removed completely in the future (snowflakedb/gosnowflake#1696).
- Maximum number of chunk download goroutines is now configured with `CLIENT_PREFETCH_THREADS` session parameter (snowflakedb/gosnowflake#1696)
  and default to 4.
- Fixed typo in `GOSNOWFLAKE_SKIP_REGISTRATION` env variable (snowflakedb/gosnowflake#1696).
- Removed `ClientIP` field from `Config` struct. This field was never used and is not needed for any functionality (snowflakedb/gosnowflake#1692).
- Unexported MfaToken and IdToken (snowflakedb/gosnowflake#1692).
- Removed `InsecureMode` field from `Config` struct. Use `DisableOCSPChecks` instead (snowflakedb/gosnowflake#1692).
- Renamed `KeepSessionAlive` field in `Config` struct to `ServerSessionKeepAlive` to adjust with the remaining drivers (snowflakedb/gosnowflake#1692).
- Removed `DisableTelemetry` field from `Config` struct. Use `CLIENT_TELEMETRY_ENABLED` session parameter instead (snowflakedb/gosnowflake#1692).
- Removed stream chunk downloader. Use a regular, default downloader instead. (snowflakedb/gosnowflake#1702).
- Removed `SnowflakeTransport`. Use `Config.Transporter` or simply register your own TLS config with `RegisterTLSConfig` if you just need a custom root certificates set (snowflakedb/gosnowflake#1703).
- Arrow batches changes (snowflakedb/gosnowflake#1706):
  - Arrow batches have been extracted to a separate package. It should significantly drop the compilation size for those who don't need arrow batches (~34MB -> ~18MB).
  - Removed `GetArrowBatches` from `SnowflakeRows` and `SnowflakeResult`. Use `arrowbatches.GetArrowBatches(rows.(SnowflakeRows))` instead.
  - Migrated functions:
    - `sf.WithArrowBatchesTimestampOption` -> `arrowbatches.WithTimstampOption`
    - `sf.WithArrowBatchesUtf8Validation` -> `arrowbatches.WithUtf8Validation`
    - `sf.ArrowSnowflakeTimestampToTime` -> `arrowbatches.ArrowSnowflakeTimestampToTime`
- Logging changes (snowflakedb/gosnowflake#1710):
  - Removed Logrus logger and migrated to slog.
  - Simplified `SFLogger` interface.
  - Added `SFSlogLogger` interface for setting custom slog handler.

Bug fixes:

- The query `context.Context` is now propagated to cloud storage operations for PUT and GET queries, allowing for better cancellation handling (snowflakedb/gosnowflake#1690).

New features:

- Added support for Go 1.26, dropped support for Go 1.23 (snowflakedb/gosnowflake#1707).
- Added support for FIPS-only mode (snowflakedb/gosnowflake#1496).

Bug fixes:

- Added panic recovery block for stage file uploads and downloads operation (snowflakedb/gosnowflake#1687).
- Fixed WIF metadata request from Azure container, manifested with HTTP 400 error (snowflakedb/gosnowflake#1701).
- Fixed SAML authentication port validation bypass in `isPrefixEqual` where the second URL's port was never checked (snowflakedb/gosnowflake#1712).
- Fixed a race condition in OCSP cache clearer (snowflakedb/gosnowflake#1704).
- The query `context.Context` is now propagated to cloud storage operations for PUT and GET queries, allowing for better cancellation handling (snowflakedb/gosnowflake#1690).
- Fixed `tokenFilePath` DSN parameter triggering false validation error claiming both `token` and `tokenFilePath` were specified when only `tokenFilePath` was provided in the DSN string (snowflakedb/gosnowflake#1715).
- Fixed minicore crash (SIGFPE) on fully statically linked Linux binaries by detecting static linking via ELF PT_INTERP inspection and skipping `dlopen` gracefully (snowflakedb/gosnowflake#1721).

Internal changes:

- Moved configuration to a dedicated internal package (snowflakedb/gosnowflake#1720).

## 1.19.0

New features:

- Added ability to disable minicore loading at compile time (snowflakedb/gosnowflake#1679).
- Exposed `tokenFilePath` in `Config` (snowflakedb/gosnowflake#1666).
- `tokenFilePath` is now read for every new connection (snowflakedb/gosnowflake#1666).
- Added support for identity impersonation when using workload identity federation (snowflakedb/gosnowflake#1652, snowflakedb/gosnowflake#1660).

Bug fixes:

- Fixed getting file from an unencrypted stage (snowflakedb/gosnowflake#1672).
- Fixed minicore file name gathering in client environment (snowflakedb/gosnowflake#1661).
- Fixed file descriptor leaks in cloud storage calls (snowflakedb/gosnowflake#1682)
- Fixed path escaping for GCS urls (snowflakedb/gosnowflake#1678).

Internal changes:

- Improved Linux telemetry gathering (snowflakedb/gosnowflake#1677).
- Improved some logs returned from cloud storage clients (snowflakedb/gosnowflake#1665).

## 1.18.1

Bug fixes:

- Handle HTTP307 & 308 in drivers to achieve better resiliency to backend errors (snowflakedb/gosnowflake#1616).
- Create temp directory only if needed during file transfer (snowflakedb/gosnowflake#1647)
- Fix unnecessary user expansion for file paths (snowflakedb/gosnowflake#1646).

Internal changes:
- Remove spammy "telemetry disabled" log messages (snowflakedb/gosnowflake#1638).
- Introduced shared library ([source code](https://github.com/snowflakedb/universal-driver/tree/main/sf_mini_core)) for extended telemetry to identify and prepare testing platform for native rust extensions (snowflakedb/gosnowflake#1629)

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
