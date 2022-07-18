// Example: Fetch one row. Now with (unencrypted) private key authentication.
// using 
// https://github.com/snowflakedb/gosnowflake/blob/master/cmd/select1/select1.go
// +
// https://github.com/snowflakedb/gosnowflake/blob/master/priv_key_test.go
//
// No cancel is allowed as no context is specified in the method call Query(). If you want to capture Ctrl+C to cancel
// the query, specify the context and use QueryContext() instead. See selectmany for example.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"

	sf "github.com/snowflakedb/gosnowflake"
)

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (string, *sf.Config, error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT", true) // e.g. "xy12345.eu-central-1" , don't use the .snowflakecomputing.com suffix
	user := env("SNOWFLAKE_TEST_USER", true)

	// private key stuff
	// prior using this, please generate an unencrypted private key + add its corresponding public key to your Snowflake user
	// documentation: https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication
	privKeyPath := env("SNOWFLAKE_TEST_UNENCRYPTED_PRIVATE_KEY_PATH", false) // /path/to/your/rsa_unencrypted_key.p8
	data, _ := ioutil.ReadFile(privKeyPath)
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		panic(fmt.Sprintf("%v is not a public key in PEM format.", privKeyPath))
	}
	privKey, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
	testPrivKey := privKey.(*rsa.PrivateKey)

	cfg := &sf.Config{
		Account:  account,
		User:     user,
		Authenticator: sf.AuthTypeJwt,
		PrivateKey: testPrivKey,
	}

	dsn, err := sf.DSN(cfg)
	return dsn, cfg, err
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()
	query := "SELECT 1"
	rows, err := db.Query(query) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	for rows.Next() {
		err := rows.Scan(&v)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if v != 1 {
			log.Fatalf("failed to get 1. got: %v", v)
		}
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB, using keypair authentication!\n", query)
}
