//go:build !windows && !darwin && arm64

package gosnowflake

import _ "embed"

//go:embed libsf_mini_core_linux_arm64.so
var coreLib []byte

var _ = initFirst()

func initFirst() any {
	corePlatformConfig.initialized = true
	corePlatformConfig.coreLib = coreLib
	corePlatformConfig.extension = "so"
	return nil
}
