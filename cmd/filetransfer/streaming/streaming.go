// Example: Upload and download one stage object as a stream.
//
// UploadStream gzips by default, so hello.txt is stored as hello.txt.gz.
// UploadStreamWithConfig with DestPrefix places the object in a stage
// directory. destFileName is only the basename. The driver reads src and
// does not close it. Close src after UploadStream returns.
//
// DownloadStream returns the stored object. DownloadStreamWithConfig with
// Decompress reads the same object through gzip. Read and close that reader
// before the Conn.Raw callback returns, so the connection stays checked out
// for the whole transfer.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

const (
	stage           = "@~/streaming_example"
	gzippedContents = "hello from streaming\n"
	plainContents   = "plain from streaming\n"
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

	if err = uploadGzip(ctx, conn); err != nil {
		log.Fatalf("failed to upload gzip object. err: %v", err)
	}
	defer func() {
		if _, err := conn.ExecContext(context.Background(), "RM "+stage); err != nil {
			log.Fatalf("failed to remove stage files. err: %v", err)
		}
	}()
	fmt.Printf("Uploaded hello.txt.gz to %s\n", stage)

	stored, err := downloadStored(ctx, conn, stage, "hello.txt.gz")
	if err != nil {
		log.Fatalf("failed to download stored object. err: %v", err)
	}
	if len(stored) < 2 || stored[0] != 0x1f || stored[1] != 0x8b {
		log.Fatalf("stored object is not gzip (%d bytes)", len(stored))
	}
	fmt.Printf("Stored object is %d gzip bytes\n", len(stored))

	plain, err := downloadDecompressed(ctx, conn, stage, "hello.txt.gz")
	if err != nil {
		log.Fatalf("failed to download decompressed object. err: %v", err)
	}
	if string(plain) != gzippedContents {
		log.Fatalf("decompressed content = %q, want %q", string(plain), gzippedContents)
	}
	fmt.Printf("Decompressed content: %s", string(plain))

	if err = uploadPlain(ctx, conn); err != nil {
		log.Fatalf("failed to upload plain object. err: %v", err)
	}
	fmt.Printf("Uploaded nested/plain.txt to %s\n", stage)

	raw, err := downloadStored(ctx, conn, stage, "nested/plain.txt")
	if err != nil {
		log.Fatalf("failed to download plain object. err: %v", err)
	}
	if string(raw) != plainContents {
		log.Fatalf("plain content = %q, want %q", string(raw), plainContents)
	}
	fmt.Printf("Plain content: %s", string(raw))
}

// uploadGzip stores hello.txt as a gzip object at the stage root.
func uploadGzip(ctx context.Context, conn *sql.Conn) error {
	return conn.Raw(func(x any) error {
		src, err := payloadReader(gzippedContents)
		if err != nil {
			return err
		}
		defer os.Remove(src.Name())
		defer src.Close()
		return x.(sf.SnowflakeConnection).UploadStream(ctx, stage, "hello.txt", src)
	})
}

// uploadPlain stores plain.txt uncompressed under the nested prefix.
func uploadPlain(ctx context.Context, conn *sql.Conn) error {
	return conn.Raw(func(x any) error {
		src, err := payloadReader(plainContents)
		if err != nil {
			return err
		}
		defer os.Remove(src.Name())
		defer src.Close()
		return x.(sf.SnowflakeConnection).UploadStreamWithConfig(ctx, stage, "plain.txt", src, sf.UploadStreamConfig{
			DestPrefix:  "nested",
			Compression: sf.CompressionNone,
		})
	})
}

// payloadReader writes payload to a temp file and returns it at offset zero.
func payloadReader(payload string) (*os.File, error) {
	f, err := os.CreateTemp("", "streaming")
	if err != nil {
		return nil, err
	}
	if _, err = f.WriteString(payload); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return f, nil
}

// downloadStored reads the stage object as it is stored.
func downloadStored(ctx context.Context, conn *sql.Conn, stageName, file string) ([]byte, error) {
	var body []byte
	err := conn.Raw(func(x any) error {
		r, err := x.(sf.SnowflakeConnection).DownloadStream(ctx, stageName, file)
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
func downloadDecompressed(ctx context.Context, conn *sql.Conn, stageName, file string) ([]byte, error) {
	var body []byte
	err := conn.Raw(func(x any) error {
		r, err := x.(sf.SnowflakeConnection).DownloadStreamWithConfig(ctx, stageName, file, sf.DownloadStreamConfig{
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
