package gosnowflake

import _ "embed"

//go:embed libsf_mini_core_darwin_arm64.dylib
var coreLib []byte

var _ = initFirst()

func initFirst() any {
	corePlatformConfig.initialized = true
	corePlatformConfig.coreLib = coreLib
	corePlatformConfig.extension = "dylib"
	return nil
}
