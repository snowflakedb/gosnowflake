## Version 1.1.6

- Fixed memory leak in the large result set. The chunk of memory is freed as soon as the cursor moved forward.
- Removed glide dependency in favor of dep #149 (@tjj5036)
- Fixed username and password URL escape issue #151
- Added Go 1.10 test.

## Version 1.1.5

- Added externalbrowser authenticator support PR #141, #142 (@tjj5036)

## Version 1.1.4

- Raise HTTP 403 errors immediately after the authentication failure instead of retry until the timeout. Issue #138 (@dominicbarnes)
- Fixed vararg error message.

## Version 1.1.3

- Removed hardcoded `public` schema name in case not specified.
- Fixed `requestId` value

## Version 1.1.2

- `nil` should set to the target value instead of the pointer to the target

## Version 1.1.1

- Fixed HTTP 403 errors when getting result sets from AWS S3. The change in the server release 2.23.0 will enforce a signature of key for result set.

## Version 1.1.0

- Fixed #125. Dropped proxy parameters. HTTP_PROXY, HTTPS_PROXY and NO_PROXY should be used.
- Improved logging based on security code review. No sensitive information is logged.
- Added no connection pool example
- Fixed #110. Raise error if the specified db, schema or warehouse doesn't exist. role was already supported.
- Added go 1.9 config in TravisCI
- Added session parameter support in DSN.

## Vesrion 1.0.0

- Added [dep](https://github.com/golang/dep) manifest (@CrimsonVoid)
- Bumped up the version to 1.0.0
