# store

[![Go Reference](https://pkg.go.dev/badge/github.com/bboxsys/store.svg)](https://pkg.go.dev/github.com/bboxsys/store)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

A JSON document store on [BadgerDB](https://github.com/dgraph-io/badger)
with account-scoped keys for multi-tenant data. It is the persistence layer
of services built on [bboxsys/platform](https://github.com/bboxsys/platform),
and works on its own in any Go program.

```sh
go get github.com/bboxsys/store
```

## Features

| | |
|---|---|
| **Documents** | Store, fetch and delete any JSON-serializable value by key |
| **Multi-tenancy** | Account-scoped operations keep each tenant's records under its own key prefix |
| **Queries** | Prefix scans with filters, pagination, counting and find-one |
| **Streaming** | Walk large result sets without loading them into memory |
| **Bulk** | Store a slice, delete many keys, or delete by prefix |
| **Expiry** | Per-record time-to-live and a user metadata byte |
| **Metrics** | Size and operation metrics for the open database |

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

Keys are `prefix:id`, and account-scoped keys are
`{accountID}:{prefix}:{key}`. That layout is a contract shared by every
consumer.

## Logging

`Options.Logger` accepts any value that implements the four-method
`store.Logger` interface. Left nil, the store logs to the console through
zerolog, tagged with `Options.Application` and `Options.Component`.
`store.NewZerologLogger` adapts an existing `zerolog.Logger`.

## Documentation

- [Store guide](docs/store-guide.md): CRUD, scoped operations, queries and
  filters, streaming, pagination, TTL and metadata, metrics.
- [Benchmarks](BENCHMARKS.md): running them and comparing against a
  baseline.

## Development

```sh
go test ./...
go test -short ./...   # skips the TTL expiry test
```

Requires Go 1.24.

## License

[Apache-2.0](LICENSE). The source is published for the projects that build
on it. It is maintained by Black Box Systems and is not seeking outside
contributions.
