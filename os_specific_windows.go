// go:build windows

package gosnowflake

import "errors"

func provideFileOwner(filepath string) (uint32, error) {
	return 0, errors.New("provideFileOwner is unsupported on windows")
}
