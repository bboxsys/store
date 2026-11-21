# Store Package Developer Guide

The `store` package provides a key-value data store backed by BadgerDB with built-in multi-tenancy, streaming, and pagination. It is the standard persistence layer for all Black Box Systems components.

## Table of Contents

- [Getting Started](#getting-started)
- [Basic CRUD Operations](#basic-crud-operations)
- [Account-Scoped Operations (Multi-Tenancy)](#account-scoped-operations-multi-tenancy)
- [Querying and Filtering](#querying-and-filtering)
- [Streaming Results](#streaming-results)
- [Counting](#counting)
- [Batch and Bulk Operations](#batch-and-bulk-operations)
- [TTL and Metadata](#ttl-and-metadata)
- [Metrics](#metrics)
- [Key Format Reference](#key-format-reference)

---

## Getting Started

### Creating a Store

```go
import "github.com/bboxsys/store"

s, err := store.NewStore(&store.Options{
    Location:    "/path/to/data.db",
    Application: "myapp",
    Component:   "server",
})
if err != nil {
    log.Fatal(err)
}
defer s.Close()
```

`Options` fields:

| Field | Description |
|-------|-------------|
| `Location` | Path to the BadgerDB directory |
| `Application` | Application name (used for logging) |
| `Component` | Component name (used for logging) |
| `IndexCacheSize` | BadgerDB index cache size in bytes (optional) |
| `BlockCacheSize` | BadgerDB block cache size in bytes (optional) |

---

## Basic CRUD Operations

All basic operations use string keys. Objects are JSON-marshaled automatically.

### Store an Object

```go
type Sensor struct {
    ID   string  `json:"id"`
    Name string  `json:"name"`
    Lat  float64 `json:"lat"`
    Lon  float64 `json:"lon"`
}

sensor := Sensor{ID: "s1", Name: "Dock Temp", Lat: 33.05, Lon: -96.70}

// Store with explicit key
err := s.PutOne("sensor:s1", &sensor)

// Store raw string (no JSON marshaling)
err = s.PutOneRaw("config:version", "1.2.3")
```

### Retrieve an Object

```go
var sensor Sensor
err := s.GetOne("sensor:s1", &sensor)

// Raw string retrieval
version, err := s.GetOneRaw("config:version")

// Check if a key exists (without reading the value)
exists, err := s.GetKeyExists("sensor:s1")
```

### Delete an Object

```go
err := s.DeleteOneByKey("sensor:s1")
```

### Get All by Prefix

```go
var sensors []Sensor
err := s.Get("sensor:", &sensors)
```

---

## Account-Scoped Operations (Multi-Tenancy)

All user-facing data should be stored with account-scoped keys. This ensures data isolation between accounts. Scoped operations build keys in the format `{accountID}:{prefix}:{key}`.

### Scoped CRUD

```go
accountID := "acct_abc123"

// Store
err := s.PutScoped(accountID, "sensor", "s1", &sensor)
// Key created: "acct_abc123:sensor:s1"

// Retrieve
var sensor Sensor
err = s.GetScoped(accountID, "sensor", "s1", &sensor)

// Delete
err = s.DeleteScoped(accountID, "sensor", "s1")

// List all sensors for an account
var sensors []Sensor
err = s.ListScoped(accountID, "sensor", &sensors)

// Delete all sensors for an account
err = s.DeleteScopedByPrefix(accountID, "sensor")
```

### Helper Functions

```go
// Build keys manually when needed
key := store.ScopedKey("acct_abc123", "sensor", "s1")
// => "acct_abc123:sensor:s1"

prefix := store.ScopedPrefix("acct_abc123", "sensor")
// => "acct_abc123:sensor:"
```

### Nested Prefixes

You can use composite prefixes to add structure:

```go
// Store media items scoped by source
s.PutScoped(accountID, "media:camera-roll", itemID, &item)
// Key: "acct_abc123:media:camera-roll:item-xyz"

// List all items from one source
s.ListScoped(accountID, "media:camera-roll", &items)

// List all media items across all sources
s.ListScoped(accountID, "media", &items)
```

---

## Querying and Filtering

The `Query` struct supports prefix scanning, JSON path filtering, pagination, and time ranges.

### Query Struct

```go
type Query struct {
    Prefix    string      // Key prefix to scan
    Filters   []*Filter   // JSON path filters (AND logic)
    Limit     int         // Max items to return (0 = unlimited)
    Offset    int         // Items to skip
    TimeRange TimeRange   // Optional time range filter
}

type Filter struct {
    Path  string  // gjson path (e.g., "name", "location.state")
    Value string  // Value to match (exact string match)
}
```

### Filtered Queries

```go
// Find all sensors in Texas
var sensors []Sensor
err := s.GetWithQuery(&store.Query{
    Prefix: "sensor:",
    Filters: []*store.Filter{
        {Path: "state", Value: "TX"},
    },
}, &sensors)
```

### Paginated Queries

`Limit` and `Offset` are honored ONLY by the streaming readers
(`GetStream` / `GetScopedStream`). `GetWithQuery` and `ListScopedWithQuery`
ignore them and return every match, so paginated endpoints must stream:

```go
// Page 2 (offset 50, 50 items) written as a JSON array to w.
var wg sync.WaitGroup
s.GetScopedStream(w, &wg, accountID, &store.Query{
    Prefix: "sensor",
    Limit:  50,
    Offset: 50,
})
wg.Wait()

// Total for the paginator.
n, err := s.GetScopedCount(accountID, &store.Query{Prefix: "sensor"})
```

### Account-Scoped Queries

```go
var sensors []Sensor
err := s.ListScopedWithQuery(accountID, &store.Query{
    Prefix: "sensor",
    Filters: []*store.Filter{
        {Path: "type", Value: "temperature"},
    },
    Limit:  25,
    Offset: 0,
}, &sensors)
```

### Find a Single Object by Filter

```go
var sensor Sensor
filters := []store.Filter{
    {Path: "name", Value: "Dock Temp"},
}
err := s.GetOneByFilter("sensor:", &filters, &sensor)

// Account-scoped variant
err = s.GetScopedByFilter(accountID, "sensor", &filters, &sensor)
```

---

## Streaming Results

For large datasets, stream results as a JSON array directly to an `io.Writer` (typically an HTTP response). This avoids loading everything into memory.

```go
func (srv *Server) GetSensors(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    var wg sync.WaitGroup
    wg.Add(1)

    s.GetStream(w, &wg, &store.Query{
        Prefix: "sensor:",
        Limit:  100,
    })

    wg.Wait()
}

// Account-scoped streaming
var wg sync.WaitGroup
wg.Add(1)
s.GetScopedStream(w, &wg, accountID, &store.Query{
    Prefix: "sensor",
    Limit:  100,
    Offset: 0,
})
wg.Wait()
```

The stream writes a valid JSON array (`[{...},{...}]`) directly to the writer. Limit and offset are applied during iteration so only the requested page is serialized.

---

## Counting

Count items without loading them into memory.

```go
// Count all sensors
count, err := s.GetCount(&store.Query{
    Prefix: "sensor:",
})

// Count with filter
count, err = s.GetCount(&store.Query{
    Prefix: "sensor:",
    Filters: []*store.Filter{
        {Path: "type", Value: "temperature"},
    },
})

// Account-scoped count
count, err = s.GetScopedCount(accountID, &store.Query{
    Prefix: "sensor",
})
```

When filters are present, values are fetched and checked. When no filters are present, only keys are iterated (faster).

---

## Batch and Bulk Operations

### Store a Slice

```go
sensors := []Sensor{
    {ID: "s1", Name: "Dock"},
    {ID: "s2", Name: "Cove"},
}
// Stores each item with key "sensor:{id}"
err := s.PutSlice("sensor", "id", sensors)
```

The second argument is the gjson path used to extract the ID from each object.

### Delete Multiple Keys

```go
keys := [][]byte{
    []byte("sensor:s1"),
    []byte("sensor:s2"),
}
err := s.DeleteKeys(keys)
```

### Delete by Prefix

```go
// Delete all sensors
err := s.DeleteByPrefix("sensor:")
```

---

## TTL and Metadata

### Time-to-Live

Store objects that automatically expire:

```go
// Expires in 24 hours
err := s.PutOneWithTTL("session:abc", &session, 24*time.Hour)

// Raw string with TTL
err = s.PutOneRawWithTTL("token:xyz", tokenValue, 1*time.Hour)
```

### User Metadata Byte

Attach a single byte of metadata to entries for fast filtering without deserializing values:

```go
const (
    MetaPending  byte = 0x01
    MetaApproved byte = 0x02
)

// Store with metadata
err := s.PutOneWithTTLAndMeta("request:r1", &req, 72*time.Hour, MetaPending)

// Query by metadata byte
var pending []Request
err = s.GetByMeta(MetaPending, &pending)

// Delete by metadata byte
err = s.DeleteByMeta(MetaPending)
```

---

## Metrics

The store tracks operation counts:

```go
metrics := s.GetMetrics()
// metrics.PutOps, metrics.GetOps, metrics.DeleteOps
```

---

## Key Format Reference

| Scope | Format | Example |
|-------|--------|---------|
| Global | `{prefix}:{key}` | `config:version` |
| Account-scoped | `{accountID}:{prefix}:{key}` | `acct123:sensor:s1` |
| Nested prefix | `{accountID}:{prefix}:{subprefix}:{key}` | `acct123:media:camera:item1` |

Guidelines:
- Use account-scoped keys for all user-facing data
- Use global keys only for system configuration and cross-account indexes
- Keep prefixes short and consistent (e.g., `wpt`, `rte`, `trk`, `sensor`)
- For secondary indexes that look up across accounts, use a dedicated global prefix (e.g., `user_email_index:{email}`)
