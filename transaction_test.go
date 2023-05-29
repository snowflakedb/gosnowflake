// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
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

// SNOW-823072: Test that transaction uses the context object supplied by BeginTx(), not from the parent connection
func TestTransactionContext(t *testing.T) {
	var tx *sql.Tx
	var err error

	db := openDB(t)
	defer db.Close()

	ctx2 := context.Background()

	pingWithRetry := withRetry(PingFunc, 5, 3*time.Second)

	err = pingWithRetry(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginTx(ctx2, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx2, "SELECT SYSTEM$WAIT(10, 'SECONDS')")
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
}

func PingFunc(ctx context.Context, db *sql.DB) error {
	return db.PingContext(ctx)
}

// Helper function for SNOW-823072 repro
func withRetry(fn func(context.Context, *sql.DB) error, numAttempts int, timeout time.Duration) func(context.Context, *sql.DB) error {
	return func(ctx context.Context, db *sql.DB) error {
		for currAttempt := 1; currAttempt <= numAttempts; currAttempt++ {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			err := fn(ctx, db)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("context deadline exceeded, failed after [%d] attempts", numAttempts)
	}
}
