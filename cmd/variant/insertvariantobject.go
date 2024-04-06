package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
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
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: true},
		{Name: "Database", EnvName: "SNOWFLAKE_TEST_DATABASE", FailOnMissing: true},
		{Name: "Schema", EnvName: "SNOWFLAKE_TEST_SCHEMA", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
	})
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("Failed to acquire connection. err: %v", err)
	}
	defer conn.Close()

	tablename := "insert_variant_object_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	param := map[string]string{"key": "value"}
	jsonStr, err := json.Marshal(param)
	if err != nil {
		log.Fatalf("failed to marshal json. err: %v", err)
	}

	createTableQuery := "CREATE TABLE " + tablename + " (c1 VARIANT, c2 OBJECT)"

	// https://docs.snowflake.com/en/sql-reference/functions/parse_json
	// can do with TO_VARIANT(PARSE_JSON(..)) as well, but PARSE_JSON already produces VARIANT
	insertQuery := "INSERT INTO " + tablename + " (c1, c2) SELECT PARSE_JSON(?), TO_OBJECT(PARSE_JSON(?))"
	// https://docs.snowflake.com/en/sql-reference/data-types-semistructured#object
	insertOnlyObject := "INSERT INTO " + tablename + " (c2) SELECT OBJECT_CONSTRUCT('name', 'Jones'::VARIANT, 'age',  42::VARIANT)"

	selectQuery := "SELECT c1, c2 FROM " + tablename

	dropQuery := "DROP TABLE " + tablename

	fmt.Printf("Creating table: %v\n", createTableQuery)
	_, err = conn.ExecContext(ctx, createTableQuery)
	if err != nil {
		log.Fatalf("failed to run the query. %v, err: %v", createTableQuery, err)
	}
	defer func() {
		fmt.Printf("Dropping the table: %v\n", dropQuery)
		_, err = conn.ExecContext(ctx, dropQuery)
		if err != nil {
			log.Fatalf("failed to run the query. %v, err: %v", dropQuery, err)
		}
	}()
	fmt.Printf("Inserting VARIANT and OBJECT data into table: %v\n", insertQuery)
	_, err = conn.ExecContext(ctx, insertQuery,
		string(jsonStr),
		string(jsonStr),
	)
	if err != nil {
		log.Fatalf("failed to run the query. %v, err: %v", insertQuery, err)
	}
	fmt.Printf("Now for another approach: %v\n", insertOnlyObject)
	_, err = conn.ExecContext(ctx, insertOnlyObject)
	if err != nil {
		log.Fatalf("failed to run the query. %v, err: %v", insertOnlyObject, err)
	}

	fmt.Printf("Querying the table into which we just inserted the data: %v\n", selectQuery)
	rows, err := conn.QueryContext(ctx, selectQuery)
	if err != nil {
		log.Fatalf("failed to run the query. %v, err: %v", selectQuery, err)
	}
	defer rows.Close()
	var c1, c2 any
	for rows.Next() {
		err := rows.Scan(&c1, &c2)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		fmt.Printf("%v (type: %T), %v (type: %T)\n", c1, c1, c2, c2)
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}

}
