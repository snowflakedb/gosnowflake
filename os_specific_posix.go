//go:build darwin || linux

package gosnowflake

import (
	"fmt"
	"os"
	"syscall"
)

func provideFileOwner(filepath string) (uint32, error) {
	info, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}
	nativeStat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("cannot cast file info for %v to *syscall.Stat_t", filepath)
	}
	return nativeStat.Uid, nil
}
