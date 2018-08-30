// +build !go1.10

package gosnowflake

import (
	"crypto/rsa"
	"runtime"
)

func parsePKC8PrivateKey(block []byte) (*rsa.PrivateKey, *SnowflakeError) {
	return nil, &SnowflakeError{
		Number: ErrCodePrivateKeyParseError,
		Message: "PKCS8 decoding is not supported for go lang version under 1.10" +
			"Current version is " + runtime.Version() +
			"Please consider update to 1.10 or higher"}
}

func marshalPKC8PrivateKey(key *rsa.PrivateKey) ([]byte, *SnowflakeError) {
	return nil, &SnowflakeError{
		Number: ErrCodePrivateKeyParseError,
		Message: "PKCS8 encoding is not supported for go lang version under 1.10" +
			"Current version is " + runtime.Version() +
			"Please consider update to 1.10 or higher"}
}
