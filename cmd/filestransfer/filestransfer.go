// Example: Files transfer using PUT/GET commands
//
// This example shows how to transfer files to staging area, from which data can be loaded into snowflake
// database tables. Apart from sending files to staging area using PUT command, files can also be downloaded
// using GET command.
package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

func getDSN() (string, *sf.Config, error) {
	env := func(key string, failOnMissing bool) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable not set", key)
		}
		return ""
	}
	account := env("SNOWFLAKE_TEST_ACCOUNT", true)
	user := env("SNOWFLAKE_TEST_USER", true)
	password := env("SNOWFLAKE_TEST_PASSWORD", true)
	database := env("SNOWFLAKE_TEST_DATABASE", true)
	warehouse := env("SNOWFLAKE_TEST_WAREHOUSE", true)
	host := env("SNOWFLAKE_TEST_HOST", false)
	schema := env("SNOWFLAKE_TEST_SCHEMA", true)
	portStr := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	port := 443
	var err error
	if len(portStr) > 0 {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return "", nil, err
		}
	}

	cfg := &sf.Config{
		Account:   account,
		User:      user,
		Password:  password,
		Database:  database,
		Warehouse: warehouse,
		Schema:    schema,
		Host:      host,
		Port:      port,
		Protocol:  protocol,
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return "", nil, err
	}

	return dsn, cfg, nil
}

func decompressAndRead(file *os.File) (string, error) {
	gzipReader, err := gzip.NewReader(file)
	defer gzipReader.Close()
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	_, err = b.ReadFrom(gzipReader)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func printRows(rows *sql.Rows) {
	for i := 1; rows.Next(); i++ {
		var col1, col2 string
		if err := rows.Scan(&col1, &col2); err != nil {
			log.Fatalf("error while scaning rows; err: %v", err)
		}
		fmt.Printf("Row %v: %v, %v\n", i, col1, col2)
	}
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	//Opening connection
	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("error while creating DSN from config: %v, error: %v", cfg, err)
	}
	db, err := sql.Open("snowflake", dsn)
	defer db.Close()

	//Creating table to which the data from CSV file will be copied
	_, err = db.Exec("CREATE OR REPLACE TABLE GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE(num integer, text varchar);")
	if err != nil {
		log.Fatalf("error while creating table; err: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE;")

	//Uploading data_to_upload.csv to internal stage for table GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE
	currentDir, _ := os.Getwd()
	fmt.Printf("CurrentDir: %v\n", currentDir)
	filePath := currentDir + "/cmd/filestransfer/data_to_upload.csv"
	_, err = db.Exec("PUT file://" + filePath + " @%GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE;")
	if err != nil {
		log.Fatalf("error while uploading file; err: %v", err)
	}
	fmt.Println("data_do_upload.csv successfully uploaded to internal stage.")

	//Creating custom file format that describes our data
	_, err = db.Exec("CREATE OR REPLACE TEMPORARY FILE FORMAT CUSTOM_CSV_FORMAT" +
		" TYPE = CSV COMPRESSION = GZIP FIELD_DELIMITER = ';' FILE_EXTENSION = 'csv' SKIP_HEADER = 1;")
	if err != nil {
		log.Fatalf("error while creating file format; err: %v", err)
	}
	fmt.Println("Custom CSV format successfully created.")

	//Loading data from files in stage area into table
	_, err = db.Exec("COPY INTO GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE FILE_FORMAT = CUSTOM_CSV_FORMAT;")
	if err != nil {
		log.Fatalf("error while copying data into table; err: %v", err)
	}
	fmt.Println("Data successfully loaded into table.")

	//Querying loaded data from table
	rows, err := db.Query("SELECT * FROM GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE;")
	if err != nil {
		log.Fatalf("error while querying data from table; err: %v", err)
	}
	defer rows.Close()
	printRows(rows)

	//Downloading file from stage area
	_, err = db.Exec("GET @%GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE/data_to_upload.csv file:///tmp/;")
	if err != nil {
		log.Fatalf("error while downloading data from internal stage area; err: %v", err)
	}
	fmt.Println("File successfully downloaded from internal stage area.")

	//Reading from downloaded file
	file, err := os.Open("/tmp/data_to_upload.csv.gz")
	if err != nil {
		log.Fatalf("error while opening downloaded file; err: %v", err)
	}
	content, err := decompressAndRead(file)
	if err != nil {
		log.Fatalf("error while reading file; err: %v", err)
	}
	fmt.Printf("Downloaded file content: \n%v\n", content)
}
