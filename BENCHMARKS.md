# Store benchmarks

Benchmarks for the BadgerDB-backed store live in `store_bench_test.go`. They
give a repeatable baseline so any change to the store (or a BadgerDB upgrade)
can be compared for regressions or improvements.

## Running

```
# Run all benchmarks with per-op memory stats, no unit tests.
go test -run='^$' -bench=. -benchmem ./...

# Longer, more stable numbers (recommended before recording a baseline):
go test -run='^$' -bench=. -benchmem -benchtime=3s -count=6 ./... | tee new.txt
```

Use `-mod=readonly` if the workspace vendor tree is out of sync.

## Comparing against a baseline

A committed baseline lives in `benchmarks/baseline.txt`. To compare a change:

```
# 1. Capture the current baseline (main), if not already recorded:
go test -run='^$' -bench=. -benchmem -benchtime=3s -count=6 ./... > benchmarks/baseline.txt

# 2. Make your change, then capture new numbers the same way:
go test -run='^$' -bench=. -benchmem -benchtime=3s -count=6 ./... > new.txt

# 3. Compare with benchstat (install once: go install golang.org/x/perf/cmd/benchstat@latest):
benchstat benchmarks/baseline.txt new.txt
```

benchstat reports the delta and whether it is statistically significant. Use
`-count=6` (or more) so it has enough samples; a single run is too noisy to
trust small deltas.

## What each benchmark measures

| Benchmark | Measures |
|---|---|
| `PutScoped` / `PutScopedParallel` | single-key write throughput, serial and concurrent |
| `GetScoped` / `GetScopedParallel` | single-key read throughput, serial and concurrent |
| `BulkWrite/PerKeyTxn` vs `BulkWrite/WriteBatch` | 1000-record ingest: one txn per key vs a single WriteBatch |
| `ListScopedFullScan/n=100,1000,10000` | decoding an entire prefix into a slice (cost curve by size) |
| `GetWithQueryPaginated` | one page (limit 50) out of 10k — the pattern list endpoints should use |
| `GetScopedStream/n=1000,10000` | streaming a prefix to an io.Writer (constant server memory) |
| `GetScopedStreamProjected/n=10000,100000` | streaming a prefix with a per-value projection (map-points pattern: emit only a few fields) |
| `PutStream` | bulk JSON-array ingest via WriteBatch |
| `GetScopedCount` | key-only count scan (value prefetch disabled) |

## Note: BadgerDB Stream framework was evaluated and rejected

We tried backing large scans with Badger's parallel `Stream` framework
(`db.NewStream`). Benchmarked head-to-head against the existing single-iterator
`GetScopedStream`, it was **slower and far heavier** at our scan sizes: at
n=10,000 it took ~8ms vs ~3.3ms and allocated ~351MB vs ~4.2MB (about 19x); at
n=100,000 it only matched on time while allocating ~783MB vs ~40MB. Badger's
Stream targets full-database backup/restore across many SSTables, and its
z.Buffer + protobuf round-trip overhead dominates prefix scans of our size. The
lean single-iterator scan wins, so `GetStreamProjected` uses a single iterator.
If a genuinely huge (millions of keys) export path ever appears, re-benchmark
Stream for that specific case.

## Baseline (reference only; hardware-dependent)

Recorded on an Apple M4 Max, BadgerDB v4.9.6, `-benchtime=1s`. Your hardware
will differ; always re-record a baseline on the machine you compare on. Key
takeaways from this baseline:

- Single-key ops: ~150k writes/s and ~290k reads/s serial; ~325k writes/s and
  ~775k reads/s with concurrency.
- WriteBatch ingest is ~4.7x faster than per-key transactions for 1000 records
  (1.3ms vs 6.1ms) and roughly halves allocations — prefer batched writes for
  bulk ingest.
- A paginated 50-item query costs ~1.4ms against a 10k set vs ~31ms to scan and
  decode the whole 10k — prefer pagination for large lists.
