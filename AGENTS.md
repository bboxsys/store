# store Developer Guide

BadgerDB JSON document store shared by the services built on the bboxsys platform.
Full API reference: `docs/store-guide.md`. Read it before changing behavior.

## Rules

- No emojis in code, comments, or docs. No AI-tool attribution in commits.
- The key layout is a cross-consumer CONTRACT -- do not change it:
  global `{prefix}:{key}`, account-scoped `{accountID}:{prefix}:{key}`
  (built by `ScopedKey`/`ScopedPrefix`). Time-range queries additionally
  assume keys shaped `prefix:startMillis:endMillis`.
- Values are raw JSON. Multi-reads concatenate stored bytes into one
  `[...]` buffer and unmarshal once into the caller's slice pointer;
  filters are gjson paths with exact-string AND matching against the raw
  bytes. Keep both properties -- consumers rely on them for performance.
- Prefix-handling asymmetry (easy to break, hard to notice):
  `Get`/`GetWithQuery` take the prefix VERBATIM (caller includes the
  trailing `:`); `GetStream`/`GetCount`/`GetOneByFilter*` append `:`
  themselves (caller must NOT include it). All `*Scoped*` methods take
  bare prefixes with no colons.
- `Limit`/`Offset` are honored ONLY by `GetStream`/`GetScopedStream`.
  `GetWithQuery`/`ListScopedWithQuery` return every match. Paginated API
  endpoints stream + `GetCount`.
- There is no exported ErrNotFound: callers compare
  `badger.ErrKeyNotFound`. `Store.DB` is public; consumers may run raw
  `DB.View/Update` transactions. Avoid TOCTOU: collect
  keys inside one View, then `DeleteKeys` (which already handles
  `badger.ErrTxnTooBig` by committing and retrying).
- Logging goes through the 4-method `store.Logger` interface
  (Debug/Info/Warn/Error, string arg). Nil Logger gets a zerolog console
  logger tagged with Options.Application/Component; `NewZerologLogger`
  adapts an existing zerolog.Logger. Do not add methods to the interface
  -- every consumer implements it.
- No geo code here: haversine/radius helpers belong to the consumer.

## Build and test

```
go vet ./... && go test -race ./...   # CI runs exactly this
go test -short ./...                  # skips the TTL expiry test
```
Tests use t.TempDir() and run in parallel.

## Release

Apache-2.0, public module. Tag `v0.x.y` on main; consumers (including
`github.com/bboxsys/platform`) update with
`GOWORK=off go get github.com/bboxsys/store@v0.x.y`.
