// Example: Download a stage file as a stream.
//
// DownloadStream returns one stage object as an io.ReadCloser so the caller can
// read the body directly. DownloadStreamWithConfig with Decompress reads the same
// object through gzip.
//
// Obtain SnowflakeConnection from database/sql Conn.Raw. Read and close the
// reader before the Raw callback returns so the connection stays checked out
// for the whole download.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

const fileContents = "hello from DownloadStream\n"

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
		log.Fatalf("failed to acquire connection. err: %v", err)
	}
	defer conn.Close()

	localPath := writeTempFile(fileContents)
	defer func() {
		if err := os.RemoveAll(filepath.Dir(localPath)); err != nil {
			log.Fatalf("failed to remove temp file. err: %v", err)
		}
	}()

	// PUT auto_compress=true stores the object as data.txt.gz.
	const stage = "@~/download_stream_example"
	const stagedFile = "data.txt.gz"
	putSQL := fmt.Sprintf("PUT 'file://%s' %s AUTO_COMPRESS=TRUE OVERWRITE=TRUE",
		strings.ReplaceAll(localPath, "\\", "\\\\"), stage)
	if _, err = conn.ExecContext(ctx, putSQL); err != nil {
		log.Fatalf("failed to upload file. err: %v", err)
	}
	defer func() {
		if _, err := conn.ExecContext(context.Background(), "RM "+stage); err != nil {
			log.Fatalf("failed to remove stage files. err: %v", err)
		}
	}()
	fmt.Printf("Uploaded %s to %s/%s\n", localPath, stage, stagedFile)

	stored, err := downloadStored(ctx, conn, stage, stagedFile)
	if err != nil {
		log.Fatalf("failed to download stored object. err: %v", err)
	}
	if len(stored) < 2 || stored[0] != 0x1f || stored[1] != 0x8b {
		log.Fatalf("stored object is not gzip (%d bytes)", len(stored))
	}
	fmt.Printf("Stored object is %d gzip bytes\n", len(stored))

	plain, err := downloadDecompressed(ctx, conn, stage, stagedFile)
	if err != nil {
		log.Fatalf("failed to download decompressed object. err: %v", err)
	}
	if string(plain) != fileContents {
		log.Fatalf("decompressed content = %q, want %q", string(plain), fileContents)
	}
	fmt.Printf("Decompressed content: %s", string(plain))
}

// downloadStored reads the stage object as it is stored (gzip bytes here).
func downloadStored(ctx context.Context, conn *sql.Conn, stage, file string) ([]byte, error) {
	var body []byte
	err := conn.Raw(func(x any) error {
		r, err := x.(sf.SnowflakeConnection).DownloadStream(ctx, stage, file)
		if err != nil {
			return err
		}
		defer r.Close()
		body, err = io.ReadAll(r)
		return err
	})
	return body, err
}

// downloadDecompressed gunzips the stage object while reading it.
func downloadDecompressed(ctx context.Context, conn *sql.Conn, stage, file string) ([]byte, error) {
	var body []byte
	err := conn.Raw(func(x any) error {
		r, err := x.(sf.SnowflakeConnection).DownloadStreamWithConfig(ctx, stage, file, sf.DownloadStreamConfig{
			Decompress: true,
		})
		if err != nil {
			return err
		}
		defer r.Close()
		body, err = io.ReadAll(r)
		return err
	})
	return body, err
}

func writeTempFile(content string) string {
	dir, err := os.MkdirTemp("", "downloadstream")
	if err != nil {
		log.Fatalf("failed to create temp dir. err: %v", err)
	}
	path := filepath.Join(dir, "data.txt")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		log.Fatalf("failed to write temp file. err: %v", err)
	}
	return path
}
