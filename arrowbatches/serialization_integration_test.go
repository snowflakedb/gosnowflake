package arrowbatches

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	sf "github.com/snowflakedb/gosnowflake/v2"
)

// fetchValueStrings fetches all records of a batch and returns their values rendered
// as strings (rows x cols), releasing the records afterwards.
func fetchValueStrings(t *testing.T, batch *ArrowBatch) [][]string {
	t.Helper()
	records, err := batch.Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	var out [][]string
	for _, rec := range *records {
		nCols := int(rec.NumCols())
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]string, nCols)
			for c := 0; c < nCols; c++ {
				row[c] = rec.Column(c).ValueStr(r)
			}
			out = append(out, row)
		}
		rec.Release()
	}
	return out
}

// roundTrip serializes a batch, JSON-encodes and decodes it (simulating shipping to
// another machine), and reconstructs it with a fresh HTTP client that has no Snowflake
// connection — mimicking a distributed-fetch worker.
func roundTrip(t *testing.T, s SerializableArrowBatch, pool memory.Allocator) *ArrowBatch {
	t.Helper()
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded SerializableArrowBatch
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	batch, err := decoded.ToArrowBatch(WithHTTPClient(&http.Client{}), WithAllocator(pool))
	if err != nil {
		t.Fatalf("ToArrowBatch: %v", err)
	}
	return batch
}

func assertEqualValues(t *testing.T, want, got [][]string, batchIdx int) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("batch %d: row count mismatch: direct=%d reconstructed=%d", batchIdx, len(want), len(got))
	}
	for r := range want {
		if len(want[r]) != len(got[r]) {
			t.Fatalf("batch %d row %d: col count mismatch: %d vs %d", batchIdx, r, len(want[r]), len(got[r]))
		}
		for c := range want[r] {
			if want[r][c] != got[r][c] {
				t.Fatalf("batch %d row %d col %d: %q vs %q", batchIdx, r, c, want[r][c], got[r][c])
			}
		}
	}
}

// TestSerializableArrowBatchesDistributedRoundTrip exercises the full producer->worker
// path against a live account: a result large enough to yield an inline first batch and
// at least one downloaded chunk is serialized, JSON round-tripped, and re-fetched with a
// connection-less HTTP client; the reconstructed values must match a direct Fetch.
func TestSerializableArrowBatchesDistributedRoundTrip(t *testing.T) {
	numRows := 100000
	directPool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer directPool.AssertSize(t, 0)
	workerPool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer workerPool.AssertSize(t, 0)

	ctx := sf.WithArrowAllocator(WithArrowBatches(context.Background()), directPool)
	query := fmt.Sprintf(
		"SELECT SEQ8() AS n, RANDSTR(200, RANDOM()) AS s FROM TABLE(GENERATOR(ROWCOUNT=>%d))", numRows)
	sfRows, cleanup := queryRawRows(ctx, t, query)
	defer cleanup()

	batches, err := GetArrowBatches(sfRows)
	if err != nil {
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}
	t.Logf("result produced %d batches", len(batches))

	// Serialize every batch before fetching any (descriptor is captured up front).
	serialized := make([]SerializableArrowBatch, len(batches))
	for i, b := range batches {
		s, err := b.ToSerializable()
		if err != nil {
			t.Fatalf("ToSerializable batch %d: %v", i, err)
		}
		serialized[i] = s
	}

	total := 0
	var sum int64
	for i, b := range batches {
		direct := fetchValueStrings(t, b)
		recon := fetchValueStrings(t, roundTrip(t, serialized[i], workerPool))
		assertEqualValues(t, direct, recon, i)

		for _, row := range recon {
			total++
			var n int64
			if _, err := fmt.Sscan(row[0], &n); err != nil {
				t.Fatalf("parsing column n %q: %v", row[0], err)
			}
			sum += n
		}
	}

	if total != numRows {
		t.Fatalf("row count mismatch: expected %d, got %d", numRows, total)
	}
	// SEQ8() yields 0..numRows-1, so the sum is deterministic.
	wantSum := int64(numRows) * int64(numRows-1) / 2
	if sum != wantSum {
		t.Fatalf("value integrity check failed: sum=%d, want %d", sum, wantSum)
	}
}

// TestSerializableArrowBatchStructuredType confirms structured/nested types survive the
// serialize -> reconstruct -> fetch path (the row-type Fields drive the transform).
func TestSerializableArrowBatchStructuredType(t *testing.T) {
	directPool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer directPool.AssertSize(t, 0)
	workerPool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer workerPool.AssertSize(t, 0)

	ctx := sf.WithArrowAllocator(WithArrowBatches(context.Background()), directPool)
	query := "SELECT 1 AS n, {'s': 'some string', 'i': 42}::OBJECT(s VARCHAR, i INTEGER) AS o"
	sfRows, cleanup := queryRawRows(ctx, t, query)
	defer cleanup()

	batches, err := GetArrowBatches(sfRows)
	if err != nil {
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	for i, b := range batches {
		s, err := b.ToSerializable()
		if err != nil {
			t.Fatalf("ToSerializable batch %d: %v", i, err)
		}
		direct := fetchValueStrings(t, b)
		recon := fetchValueStrings(t, roundTrip(t, s, workerPool))
		assertEqualValues(t, direct, recon, i)
	}
}
