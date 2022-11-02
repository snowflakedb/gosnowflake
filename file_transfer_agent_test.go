// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/smithy-go"
)

func TestGetBucketAccelerateConfiguration(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("Should be run against an account in AWS EU North1 region.")
	}
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations: make([]string, 0),
		},
	}
	if err = sfa.transferAccelerateConfig(); err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "MethodNotAllowed" {
				t.Fatalf("should have ignored 405 error: %v", err)
			}
		}
	}
}
