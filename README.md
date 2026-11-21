# store

JSON document store on top of [BadgerDB](https://github.com/dgraph-io/badger)
with account-scoped keys for multi-tenant data. This is the persistence layer
of the services built on [bboxsys/platform](https://github.com/bboxsys/platform).

```
go get github.com/bboxsys/store
```

Licensed under the Apache License 2.0 (`LICENSE`). The source is published
for the projects that build on it; it is maintained by Black Box Systems
and is not seeking outside contributions.

## Usage

```go
import "github.com/bboxsys/store"

st, err := store.NewStore(&store.Options{
    Location: "/var/lib/myapp/data.db",
    Logger:   myLogger, // optional; anything with Debug/Info/Warn/Error(string)
})
if err != nil {
    return err
}
defer st.Close()

// Unscoped
err = st.PutOne("job:123", job)
err = st.GetOne("job:123", &job)

// Account-scoped: keys become "{accountID}:wpt:{key}"
err = st.PutScoped(accountID, "wpt", key, waypoint)
err = st.ListScoped(accountID, "wpt", &waypoints)
n, err := st.GetScopedCount(accountID, &store.Query{Prefix: "wpt"})
```

Keys are `prefix:id`. Account-scoped keys are `{accountID}:{prefix}:{key}`;
that layout is a contract shared by every consumer, so do not change it.

See [docs/store-guide.md](docs/store-guide.md) for the full guide: CRUD,
queries and filters, streaming, pagination, TTL and meta bytes, metrics.

## Logging

`Options.Logger` accepts any value implementing the four-method `store.Logger`
interface. When nil, the store creates a zerolog console logger tagged with
`Options.Application` and `Options.Component`. `store.NewZerologLogger`
adapts an existing `zerolog.Logger`.

## Development

```
go test ./...
go test -short ./...   # skips the TTL expiry test
```

Tests use `t.TempDir()` and can run in parallel.
