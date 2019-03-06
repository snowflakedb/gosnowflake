// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.
// +build !go1.10

package gosnowflake

// This files contains variable or function of test cases that we want to run for go version <= 1.10
// See header comments on optional_go1_10_test.go

func addParseDSNTest(parseDSNTests []tcParseDSN) []tcParseDSN {
	return nil
}

func setupPrivateKey() {
}
