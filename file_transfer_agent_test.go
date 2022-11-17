// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestDownloadWithInvalidLocalPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "data")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	testData := filepath.Join(tmpDir, "data.txt")
	f, err := os.OpenFile(testData, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1,test2\ntest3,test4\n")
	f.Close()

	runTests(t, dsn, func(dbt *DBTest) {
		if _, err = dbt.db.Exec("use role sysadmin"); err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		dbt.mustExec("rm @~/test_get")
		sqlText := fmt.Sprintf("put file://%v @~/test_get", testData)
		sqlText = strings.ReplaceAll(sqlText, "\\", "\\\\")
		dbt.mustExec(sqlText)

		sqlText = fmt.Sprintf("get @~/test_get/data.txt file://%v\\get", tmpDir)
		if _, err = dbt.db.Query(sqlText); err == nil {
			t.Fatalf("should return local path not directory error.")
		}
		dbt.mustExec("rm @~/test_get")
	})
}
