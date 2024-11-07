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
	"path/filepath"

	sf "github.com/snowflakedb/gosnowflake"
)

const customFormatCsvDataToUpload = "NUM; TEXT\n1; foo\n2; bar\n3; baz"

func createTmpFile(content string) string {
	tempFile, err := os.CreateTemp("", "data_to_upload.csv")
	if err != nil {
		log.Fatalf("error during creating temp file; err: %v", err)
	}
	_, err = tempFile.Write([]byte(content))
	if err != nil {
		log.Fatalf("error during writing data to temp file; err: %v", err)
	}
	absolutePath := tempFile.Name()
	fmt.Printf("Tmp file with data to upload created at %v with content %#v\n", absolutePath, customFormatCsvDataToUpload)
	return absolutePath
}

func decompressAndRead(file *os.File) (string, error) {
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		return "", err
	}
	defer func(gzipReader *gzip.Reader) {
		err := gzipReader.Close()
		if err != nil {
			log.Fatalf("cannot close file. %v", err)
		}
	}(gzipReader)
	var b bytes.Buffer
	_, err = b.ReadFrom(gzipReader)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func printRows(rows *sql.Rows) {
	for i := 1; rows.Next(); i++ {
		var col1 int
		var col2 string
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

	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Database", EnvName: "SNOWFLAKE_TEST_DATABASE", FailOnMissing: true},
		{Name: "Schema", EnvName: "SNOWFLAKE_TEST_SCHEMA", FailOnMissing: true},
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: true},
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
		log.Fatalf("cannot connect to snowflake. %v", err)
	}
	defer db.Close()

	//Creating table to which the data from CSV file will be copied
	_, err = db.Exec("CREATE OR REPLACE TABLE GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE(num integer, text varchar);")
	if err != nil {
		log.Fatalf("error while creating table; err: %v", err)
	}
	defer func() {
		_, err := db.Exec("DROP TABLE IF EXISTS GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE;")
		if err != nil {
			log.Fatalf("cannot drop table. %v", err)
		}
	}()

	//Uploading data_to_upload.csv to internal stage for table GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE
	tmpFilePath := createTmpFile(customFormatCsvDataToUpload)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Fatalf("cannot remove temp file. %v", err)
		}
	}(tmpFilePath)
	_, err = db.Exec(fmt.Sprintf("PUT file://%v @%%GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE;", tmpFilePath))
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
	fmt.Println("Data successfully loaded into table. Querying...")

	//Querying loaded data from table
	rows, err := db.Query("SELECT * FROM GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE;")
	if err != nil {
		log.Fatalf("error while querying data from table; err: %v", err)
	}
	defer rows.Close()
	printRows(rows)

	//Downloading file from stage area to system's TMP directory
	tmpDir := os.TempDir()
	_, err = db.Exec(fmt.Sprintf("GET @%%GOSNOWFLAKE_FILES_TRANSFER_EXAMPLE/data_to_upload.csv file://%v/;", tmpDir))
	if err != nil {
		log.Fatalf("error while downloading data from internal stage area; err: %v", err)
	}
	fmt.Printf("File successfully downloaded from internal stage area to %v\n", tmpDir)

	//Reading from downloaded file
	file, err := os.Open(fmt.Sprintf("%v.gz", filepath.Join(tmpDir, filepath.Base(tmpFilePath))))
	if err != nil {
		log.Fatalf("error while opening downloaded file; err: %v", err)
	}
	content, err := decompressAndRead(file)
	if err != nil {
		log.Fatalf("error while reading file; err: %v", err)
	}
	fmt.Printf("Downloaded file content: %#v\n", content)
}
