package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
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
		{Name: "Database", EnvName: "SNOWFLAKE_TEST_DATABASE", FailOnMissing: true},
		{Name: "Schema", EnvName: "SNOWFLAKE_TEST_SCHEMA", FailOnMissing: true},
	})
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("Failed to create DSN from config: %v, error: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("Failed to connect, dsn: %v, error: %v", dsn, err)
	}

	defer db.Close()

	printSelectDemo(db)
	printModifyingDemo(db)
}

func printSelectDemo(db *sql.DB) {
	fmt.Println("SELECTs only")
	numberOfQueries := 2
	query := `
		WITH table1 AS (SELECT 'table 1, row 1', 11 UNION SELECT 'table 1, row 2', 12)
		SELECT * FROM table1;

		WITH table2 AS (SELECT 'table 2, row 1', 21 UNION SELECT 'table 2, row 2', 22)
		SELECT * FROM table2;
	`

	fmt.Println(query)

	context := createMultistatementContext(numberOfQueries)

	result, err := db.QueryContext(context, query)
	if err != nil {
		log.Fatalf("Error while querying snowflake: %v", err)
	}

	defer result.Close()

	for hasNextResultSet := true; hasNextResultSet; hasNextResultSet = result.NextResultSet() {
		for result.Next() {
			var str string
			var int int
			err := result.Scan(&str, &int)
			if err != nil {
				log.Fatalf("Error while scanning row: %v", err)
			}
			fmt.Println(str, " | ", int)
		}
	}
	if result.Err() != nil {
		log.Fatalf("Error while reading results: %v", err)
	}
}

func printModifyingDemo(db *sql.DB) {
	fmt.Println("Modifications only")
	numberOfQueries := 4
	tableSuffix := time.Now().UnixMilli()
	query := fmt.Sprintf(`
		CREATE TABLE multistatement_test_%d (id integer);
		INSERT INTO multistatement_test_%d VALUES (1);
		INSERT INTO multistatement_test_%d VALUES (2), (3);
		DROP TABLE multistatement_test_%d
	`, tableSuffix, tableSuffix, tableSuffix, tableSuffix)

	fmt.Println(query)

	context := createMultistatementContext(numberOfQueries)

	result, err := db.ExecContext(context, query)
	if err != nil {
		log.Fatalf("Error while querying snowflake: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Fatalf("Error while reading rows affected: %v", rowsAffected)
	}

	fmt.Printf("Rows affected: %d, expected: %d\n", rowsAffected, 3)
}

func createMultistatementContext(numberOfQueries int) context.Context {
	context, err := sf.WithMultiStatement(context.Background(), numberOfQueries)
	if err != nil {
		log.Fatalf("Error while creating multi statement context: %v", err)
	}
	return context
}
