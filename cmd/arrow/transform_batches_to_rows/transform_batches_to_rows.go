package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	sf "github.com/snowflakedb/gosnowflake"
	"io"
	"log"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
	})
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}

	connector := sf.NewConnector(sf.SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatalf("cannot create a connection. %v", err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = json")
	if err != nil {
		log.Fatalf("cannot force JSON as result format. %v", err)
	}

	var rows driver.Rows
	err = conn.Raw(func(x any) error {
		rows, err = x.(driver.QueryerContext).QueryContext(sf.WithArrowBatches(context.Background()), "SELECT 1, 'hello' UNION SELECT 2, 'hi' UNION SELECT 3, 'howdy'", nil)
		return err
	})
	if err != nil {
		log.Fatalf("cannot run a query. %v", err)
	}
	defer rows.Close()

	_, err = rows.(sf.SnowflakeRows).GetArrowBatches()
	var se *sf.SnowflakeError
	if !errors.As(err, &se) || se.Number != sf.ErrNonArrowResponseInArrowBatches {
		log.Fatalf("expected to fail while retrieving arrow batches")
	}

	res := make([]driver.Value, 2)
	for {
		err = rows.Next(res)
		if err == io.EOF {
			break
		}
		println(res[0].(string), res[1].(string))
	}
}
