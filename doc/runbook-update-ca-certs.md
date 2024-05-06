### Updating CA cert store (currently manual process)
1. As with other Snowflake drivers, here we use the cacerts.pem as well, which is curated by Mozilla and available at https://curl.se/docs/caextract.html
2. The driver reads the certs from `caRootPEM` which is found in [cacert.go](https://github.com/snowflakedb/gosnowflake/blob/master/cacert.go)
3. So to update the cert store, one needs to update it in `cacert.go`. Download the latest `cacert.pem` from https://curl.se/ca/cacert.pem.
4. Edit `cacert.go`, and locate the `caRootPEM` const:
```go
const caRootPEM = `
##
## Bundle of CA Root Certificates
##
## Certificate data from Mozilla as of: Wed Jul 22 03:12:14 2020 GMT
##
## This is a bundle of X.509 certificates of public Certificate Authorities
## (CA). These were automatically extracted from Mozilla's root certificates
## file (certdata.txt).  This file can be found in the mozilla source tree:
## https://hg.mozilla.org/releases/mozilla-release/raw-file/default/security/nss/lib/ckfw/builtins/certdata.txt
##
## It contains the certificates in PEM format and therefore
## can be directly used with curl / libcurl / php_curl, or with
## an Apache+mod_ssl webserver for SSL client authentication.
## Just configure this file as the SSLCACertificateFile.
##
## Conversion done with mk-ca-bundle.pl version 1.28.
## SHA256: cc6408bd4be7fbfb8699bdb40ccb7f6de5780d681d87785ea362646e4dad5e8e
##


GlobalSign Root CA
==================
-----BEGIN CERTIFICATE-----
..here's the first CA cert, followed by tons of other CA certs
..last CA cert in the bundle ends here
-----END CERTIFICATE-----
`
```
5. replace the whole bundle of CA certs which is enclosed by
```go
const caRootPEM = `
..certs
`
```

replace the part represented by `..certs` above, with the whole content of the `cacerts.pem` which you downloaded.

6. Save the edited file and create a PR.

#### Things to watch out for:
* Make sure you retain the enclosing opening and closing backticks around the actual CA cert bundle
