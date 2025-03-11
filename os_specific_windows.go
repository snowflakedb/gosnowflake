// go:build windows

package gosnowflake

import (
	"errors"
	"os"
)

func providePathOwner(filepath string) (uint32, error) {
	return 0, errors.New("providePathOwner is unsupported on windows")
}

func provideFileOwner(file *os.File) (uint32, error) {
	return 0, errors.New("provideFileOwner is unsupported on windows")
}
