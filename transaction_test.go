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

	conn := openConn(t)
	defer conn.Close()

	tx, err = conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatal("failed to start transaction.")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal("failed to rollback")
	}
	if _, err = conn.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true}); err == nil {
		t.Fatal("should have failed.")
	}
	if driverErr, ok := err.(*SnowflakeError); !ok || driverErr.Number != ErrNoReadOnlyTransaction {
		t.Fatalf("should have returned Snowflake Error: %v", errMsgNoReadOnlyTransaction)
	}
	if _, err = conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: 100}); err == nil {
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

	conn := openConn(t)
	defer conn.Close()

	ctx := context.Background()

	pingWithRetry := withRetry(PingFunc, 5, 3*time.Second)

	err = pingWithRetry(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}

	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "SELECT SYSTEM$WAIT(10, 'SECONDS')")
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func PingFunc(ctx context.Context, conn *sql.Conn) error {
	return conn.PingContext(ctx)
}

// Helper function for SNOW-823072 repro
func withRetry(fn func(context.Context, *sql.Conn) error, numAttempts int, timeout time.Duration) func(context.Context, *sql.Conn) error {
	return func(ctx context.Context, db *sql.Conn) error {
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
