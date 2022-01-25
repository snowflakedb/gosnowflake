package gosnowflake

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

// benchmark a variety of input sizes for each test
var benchmarkInputSizes = []int64{
	100 * KB,
	1 * MB,
	10 * MB,
	100 * MB,
}

func BenchmarkCompressFileWithGzipFromStream(b *testing.B) {
	var (
		buf  = bytes.NewBuffer(make([]byte, 0))
		cr   cheapReader
		util = new(snowflakeFileUtil)
	)

	for _, size := range benchmarkInputSizes {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				_, _ = buf.ReadFrom(io.LimitReader(cr, size))
				_, _, _ = util.compressFileWithGzipFromStream(&buf)
			}
		})
	}
}

func BenchmarkCompressFileWithGzip(b *testing.B) {
	var (
		cr   cheapReader
		util = new(snowflakeFileUtil)
	)

	for _, size := range benchmarkInputSizes {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ReportAllocs()

			// populate a temporary file
			tf, _ := ioutil.TempFile(os.TempDir(), "")
			defer os.Remove(tf.Name())
			_, _ = io.Copy(tf, io.LimitReader(cr, size))
			tf.Close()

			for i := 0; i < b.N; i++ {
				_, _, _ = util.compressFileWithGzip(tf.Name(), os.TempDir())
			}
		})
	}
}

func BenchmarkGetDigestAndSizeForStream(b *testing.B) {
	var (
		buf  = bytes.NewBuffer(make([]byte, 0))
		cr   cheapReader
		util = new(snowflakeFileUtil)
	)

	for _, size := range benchmarkInputSizes {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				_, _ = buf.ReadFrom(io.LimitReader(cr, size))
				_, _ = util.getDigestAndSizeForStream(&buf)
			}
		})
	}
}

func BenchmarkGetDigestAndSizeForFile(b *testing.B) {
	var (
		cr   cheapReader
		util = new(snowflakeFileUtil)
	)

	for _, size := range benchmarkInputSizes {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.ReportAllocs()

			// populate a temporary file
			tf, _ := ioutil.TempFile(os.TempDir(), "")
			defer os.Remove(tf.Name())
			_, _ = io.Copy(tf, io.LimitReader(cr, size))
			tf.Close()

			for i := 0; i < b.N; i++ {
				_, _, _ = util.getDigestAndSizeForFile(tf.Name())
			}
		})
	}
}

// cheapReader is an io.Reader for testing that is cheap to call. Must be used in conjunction with
// io.LimitReader, since cheapReader.Read will otherwise never return an io.EOF.
type cheapReader struct{}

// Read implements io.Reader, but always returns the same test data on each call.
func (r cheapReader) Read(p []byte) (int, error) { return copy(p, "12345678"), nil }

// prove that cheapReader contributes insignificantly to other benchmarks.
func BenchmarkCheapReader(b *testing.B) {
	var cr cheapReader
	var buf = make([]byte, 8)
	for i := 0; i < b.N; i++ {
		_, _ = cr.Read(buf)
		buf = buf[0:]
	}
}

const (
	B  = iota
	KB = 1 << (10 * iota)
	MB
)
