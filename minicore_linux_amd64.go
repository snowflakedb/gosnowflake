//go:build !windows && !darwin && amd64

package gosnowflake

import _ "embed"

//go:embed libsf_mini_core_linux_amd64.so
var coreLib []byte

var _ = initFirst()

func initFirst() any {
	corePlatformConfig.initialized = true
	corePlatformConfig.coreLib = coreLib
	corePlatformConfig.extension = "so"
	return nil
}
