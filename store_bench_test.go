package store

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/tidwall/gjson"
)

// benchRecord is a representative ~250-byte JSON document: a mix of strings,
// numbers, and a small nested object, so marshal/unmarshal and value sizes
// resemble real stored objects (waypoints, users, media items) rather than a
// trivial one-field struct.
type benchRecord struct {
	UUID      string  `json:"uuid"`
	Kind      string  `json:"kind"`
	Name      string  `json:"name"`
	Notes     string  `json:"notes"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	CreatedAt int64   `json:"created_at"`
	UpdatedAt int64   `json:"updated_at"`
	Tags      []string `json:"tags"`
	Meta      struct {
		Source  string `json:"source"`
		Version int    `json:"version"`
		Active  bool   `json:"active"`
	} `json:"meta"`
}

func newBenchRecord(i int) benchRecord {
	r := benchRecord{
		UUID:      fmt.Sprintf("rec-%08d", i),
		Kind:      "waypoint",
		Name:      fmt.Sprintf("Feature number %d in the survey set", i),
		Notes:     "Recorded during the afternoon pass; conditions nominal, revisit recommended.",
		Lat:       31.0 + rand.Float64(),
		Lon:       -96.0 - rand.Float64(),
		CreatedAt: int64(1_700_000_000 + i),
		UpdatedAt: int64(1_700_000_500 + i),
		Tags:      []string{"survey", "afternoon", "priority"},
	}
	r.Meta.Source = "field-unit-7"
	r.Meta.Version = 3
	r.Meta.Active = true
	return r
}

const (
	benchAccount = "benchacct"
	benchPrefix  = "rec"
)

// seedScoped writes n records under a single account/prefix using a WriteBatch
// so setup does not dominate benchmark wall-clock. Returns the keys written.
func seedScoped(b *testing.B, store *Store, n int) []string {
	b.Helper()
	wb := store.DB.NewWriteBatch()
	defer wb.Cancel()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		rec := newBenchRecord(i)
		key := ScopedKey(benchAccount, benchPrefix, rec.UUID)
		keys[i] = key
		val := mustJSON(b, rec)
		if err := wb.Set([]byte(key), val); err != nil {
			b.Fatalf("seed set: %v", err)
		}
	}
	if err := wb.Flush(); err != nil {
		b.Fatalf("seed flush: %v", err)
	}
	return keys
}

func mustJSON(b *testing.B, v interface{}) []byte {
	b.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	return out
}

// ---- Single-key write / read ----

// BenchmarkPutScoped measures single-key writes (one Badger transaction each).
func BenchmarkPutScoped(b *testing.B) {
	store := testOpen(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := newBenchRecord(i)
		if err := store.PutScoped(benchAccount, benchPrefix, rec.UUID, &rec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutScopedParallel measures write throughput under concurrency.
func BenchmarkPutScopedParallel(b *testing.B) {
	store := testOpen(b)
	b.ReportAllocs()
	b.ResetTimer()
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		var local int
		for pb.Next() {
			local++
			rec := newBenchRecord(int(counter) + local*1_000_003)
			if err := store.PutScoped(benchAccount, benchPrefix, rec.UUID+randSuffix(), &rec); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGetScoped measures single-key reads against a pre-populated store.
func BenchmarkGetScoped(b *testing.B) {
	store := testOpen(b)
	const n = 10_000
	keys := seedScoped(b, store, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var rec benchRecord
		if err := store.GetOne(keys[i%n], &rec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetScopedParallel measures read throughput under concurrency.
func BenchmarkGetScopedParallel(b *testing.B) {
	store := testOpen(b)
	const n = 10_000
	keys := seedScoped(b, store, n)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Intn(n)
		for pb.Next() {
			var rec benchRecord
			if err := store.GetOne(keys[i%n], &rec); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// ---- Bulk write: WriteBatch vs per-key transactions ----

// BenchmarkBulkWrite compares writing 1000 records with individual PutScoped
// transactions against a single WriteBatch (via PutSlice). This quantifies the
// benefit of batching bulk ingest.
func BenchmarkBulkWrite(b *testing.B) {
	const n = 1000
	recs := make([]benchRecord, n)
	for i := range recs {
		recs[i] = newBenchRecord(i)
	}

	// Open the store once so the measurement reflects the write path, not
	// Badger's per-open cost; each iteration writes under a unique prefix.
	b.Run("PerKeyTxn", func(b *testing.B) {
		store := testOpen(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := range recs {
				if err := store.PutScoped(benchAccount, fmt.Sprintf("pk-%d", i), recs[j].UUID, &recs[j]); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("WriteBatch", func(b *testing.B) {
		store := testOpen(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := store.PutSlice(fmt.Sprintf("wb-%d", i), "uuid", recs); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ---- Scans: full list vs paginated vs stream vs count ----

// BenchmarkListScopedFullScan measures decoding an entire prefix into a slice.
// The sub-benchmarks vary dataset size so the cost curve is visible.
func BenchmarkListScopedFullScan(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := testOpen(b)
			seedScoped(b, store, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out []benchRecord
				if err := store.ListScoped(benchAccount, benchPrefix, &out); err != nil {
					b.Fatal(err)
				}
				if len(out) != n {
					b.Fatalf("got %d want %d", len(out), n)
				}
			}
		})
	}
}

// BenchmarkGetWithQueryPaginated measures a single page (limit 50) out of a
// large set, which is the pattern list endpoints should use.
func BenchmarkGetWithQueryPaginated(b *testing.B) {
	store := testOpen(b)
	const n = 10_000
	seedScoped(b, store, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out []benchRecord
		q := &Query{Prefix: benchPrefix, Limit: 50, Offset: (i * 50) % (n - 50)}
		if err := store.ListScopedWithQuery(benchAccount, q, &out); err != nil {
			b.Fatal(err)
		}
		if len(out) != 50 {
			b.Fatalf("got %d want 50", len(out))
		}
	}
}

// BenchmarkGetScopedStream measures streaming an entire prefix to an io.Writer
// (constant server memory), discarding the bytes.
func BenchmarkGetScopedStream(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := testOpen(b)
			seedScoped(b, store, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				store.GetScopedStream(io.Discard, &wg, benchAccount, &Query{Prefix: benchPrefix})
				wg.Wait()
			}
		})
	}
}

// BenchmarkGetScopedStreamProjected measures the single-iterator projection
// stream (the map-points pattern: emit only id/lat/lon per record). Compare
// against BenchmarkGetScopedStream at the same n to see the payload/CPU effect
// of projecting to a few fields.
func BenchmarkGetScopedStreamProjected(b *testing.B) {
	project := func(value []byte) ([]byte, bool) {
		res := gjson.GetManyBytes(value, "uuid", "lat", "lon")
		return []byte(fmt.Sprintf(`{"id":%q,"lat":%s,"lon":%s}`, res[0].String(), res[1].Raw, res[2].Raw)), true
	}
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := testOpen(b)
			seedScoped(b, store, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				store.GetScopedStreamProjected(io.Discard, &wg, benchAccount, &Query{Prefix: benchPrefix}, project)
				wg.Wait()
			}
		})
	}
}

// BenchmarkPutStream measures bulk ingest of a JSON array through PutStream
// (WriteBatch under the hood).
func BenchmarkPutStream(b *testing.B) {
	const n = 1_000
	var sb strings.Builder
	sb.WriteByte('[')
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.Write(mustJSON(b, newBenchRecord(i)))
	}
	sb.WriteByte(']')
	payload := sb.String()

	// Open once; each iteration ingests the array under a unique prefix so we
	// measure ingest, not repeated Badger open cost.
	store := testOpen(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.PutStream(fmt.Sprintf("ps-%d", i), strings.NewReader(payload)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetScopedCount measures a key-only count scan (PrefetchValues off).
func BenchmarkGetScopedCount(b *testing.B) {
	store := testOpen(b)
	const n = 10_000
	seedScoped(b, store, n)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := store.GetScopedCount(benchAccount, &Query{Prefix: benchPrefix})
		if err != nil {
			b.Fatal(err)
		}
		if c != n {
			b.Fatalf("got %d want %d", c, n)
		}
	}
}

func randSuffix() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
