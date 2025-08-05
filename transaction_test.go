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

	runDBTest(t, func(dbt *DBTest) {
		tx, err = dbt.conn.BeginTx(context.Background(), &sql.TxOptions{})
		if err != nil {
			t.Fatal("failed to start transaction.")
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal("failed to rollback")
		}
		if _, err = dbt.conn.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true}); err == nil {
			t.Fatal("should have failed.")
		}
		if driverErr, ok := err.(*SnowflakeError); !ok || driverErr.Number != ErrNoReadOnlyTransaction {
			t.Fatalf("should have returned Snowflake Error: %v", errMsgNoReadOnlyTransaction)
		}
		if _, err = dbt.conn.BeginTx(context.Background(), &sql.TxOptions{Isolation: 100}); err == nil {
			t.Fatal("should have failed.")
		}
		if driverErr, ok := err.(*SnowflakeError); !ok || driverErr.Number != ErrNoDefaultTransactionIsolationLevel {
			t.Fatalf("should have returned Snowflake Error: %v", errMsgNoDefaultTransactionIsolationLevel)
		}
	})
}

// SNOW-823072: Test that transaction uses the context object supplied by BeginTx(), not from the parent connection
func TestTransactionContext(t *testing.T) {
	var tx *sql.Tx
	var err error

	ctx := context.Background()

	runDBTest(t, func(dbt *DBTest) {
		pingWithRetry := withRetry(PingFunc, 5, 3*time.Second)

		err = pingWithRetry(context.Background(), dbt.conn)
		if err != nil {
			t.Fatal(err)
		}

		tx, err = dbt.conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = tx.ExecContext(ctx, "SELECT SYSTEM$WAIT(10, 'SECONDS')")
		if err != nil {
			t.Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	})
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

func TestTransactionError(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPostQuery: postQueryFail,
	}

	tx := snowflakeTx{
		sc: &snowflakeConn{
			cfg:  &Config{Params: map[string]*string{}},
			rest: sr,
		},
		ctx: context.Background(),
	}

	// test for post query error when executing the txCommand
	err := tx.execTxCommand(rollback)
	assertNotNilF(t, err, "")
	assertEqualE(t, err.Error(), "failed to get query response")

	// test for invalid txCommand
	err = tx.execTxCommand(2)
	assertNotNilF(t, err, "")
	assertEqualE(t, err.Error(), "unsupported transaction command")

	// test for bad connection error when snowflakeConn is nil
	tx.sc = nil
	err = tx.execTxCommand(rollback)
	assertNotNilF(t, err, "")
	assertEqualE(t, err.Error(), "driver: bad connection")
}
