// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"testing"
)

func TestTransactionOptions(t *testing.T) {
	var tx *sql.Tx
	var err error

	db := openDB(t)
	defer db.Close()

	tx, err = db.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatal("failed to start transaction.")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal("failed to rollback")
	}
	if _, err = db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true}); err == nil {
		t.Fatal("should have failed.")
	}
	if driverErr, ok := err.(*SnowflakeError); !ok || driverErr.Number != ErrNoReadOnlyTransaction {
		t.Fatalf("should have returned Snowflake Error: %v", errMsgNoReadOnlyTransaction)
	}
	if _, err = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: 100}); err == nil {
		t.Fatal("should have failed.")
	}
	if driverErr, ok := err.(*SnowflakeError); !ok || driverErr.Number != ErrNoDefaultTransactionIsolationLevel {
		t.Fatalf("should have returned Snowflake Error: %v", errMsgNoDefaultTransactionIsolationLevel)
	}
}
