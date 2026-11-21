package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// putNUsers stores n users under "users:<uuid>" and returns them.
func putNUsers(t *testing.T, store *Store, n int) []User {
	t.Helper()
	users := make([]User, 0, n)
	for i := 0; i < n; i++ {
		u, err := NewUser(UserConfig{
			Kind:      "user",
			FirstName: fmt.Sprintf("First%02d", i),
			LastName:  fmt.Sprintf("Last%02d", i),
			Email:     fmt.Sprintf("user%02d@test.com", i),
			Password:  "password123",
		})
		require.NoError(t, err)
		require.NoError(t, store.PutOne(fmt.Sprintf("users:%s", u.UUID), &u))
		users = append(users, u)
	}
	return users
}

func putNScopedUsers(t *testing.T, store *Store, accountID string, n int) []User {
	t.Helper()
	users := make([]User, 0, n)
	for i := 0; i < n; i++ {
		u, err := NewUser(UserConfig{
			Kind:      "user",
			FirstName: fmt.Sprintf("First%02d", i),
			LastName:  fmt.Sprintf("Last%02d", i),
			Email:     fmt.Sprintf("user%02d@test.com", i),
			Password:  "password123",
		})
		require.NoError(t, err)
		require.NoError(t, store.PutScoped(accountID, "user", u.UUID.String(), &u))
		users = append(users, u)
	}
	return users
}

// uuidsOf collects the UUIDs of a page of users, for disjointness assertions.
func uuidsOf(users []User) map[uuid.UUID]bool {
	m := make(map[uuid.UUID]bool, len(users))
	for _, u := range users {
		m[u.UUID] = true
	}
	return m
}

// TestGetWithQueryPagination is the key regression test: the bug just fixed was
// that Limit/Offset were ignored by GetWithQuery.
func TestGetWithQueryPagination(t *testing.T) {
	store := testOpen(t)
	putNUsers(t, store, 10)

	// Canonical ordering is the store's key (UUID) iteration order.
	var all []User
	require.NoError(t, store.GetWithQuery(&Query{Prefix: "users"}, &all))
	require.Len(t, all, 10, "Limit:0 must return all rows")

	t.Run("limit returns exactly N", func(t *testing.T) {
		var page []User
		require.NoError(t, store.GetWithQuery(&Query{Prefix: "users", Limit: 3}, &page))
		require.Len(t, page, 3)
		assert.Equal(t, all[:3], page, "first page must be the first 3 in iteration order")
	})

	t.Run("offset returns the next page and differs from the first", func(t *testing.T) {
		var first, second []User
		require.NoError(t, store.GetWithQuery(&Query{Prefix: "users", Limit: 3}, &first))
		require.NoError(t, store.GetWithQuery(&Query{Prefix: "users", Limit: 3, Offset: 3}, &second))
		require.Len(t, second, 3)
		assert.Equal(t, all[3:6], second)

		firstIDs := uuidsOf(first)
		for _, u := range second {
			assert.False(t, firstIDs[u.UUID], "second page must not overlap the first page")
		}
	})

	t.Run("offset beyond the set returns empty", func(t *testing.T) {
		var page []User
		require.NoError(t, store.GetWithQuery(&Query{Prefix: "users", Offset: 100}, &page))
		assert.Empty(t, page)
	})

	t.Run("limit zero returns everything", func(t *testing.T) {
		var page []User
		require.NoError(t, store.GetWithQuery(&Query{Prefix: "users", Limit: 0}, &page))
		assert.Len(t, page, 10)
	})
}

// TestListScopedWithQueryPagination exercises the same pagination through the
// scoped wrapper for a single account.
func TestListScopedWithQueryPagination(t *testing.T) {
	store := testOpen(t)
	putNScopedUsers(t, store, "acctA", 10)
	// A second account must not leak into the first account's pages.
	putNScopedUsers(t, store, "acctB", 10)

	var all []User
	require.NoError(t, store.ListScopedWithQuery("acctA", &Query{Prefix: "user"}, &all))
	require.Len(t, all, 10)

	t.Run("limit returns exactly N", func(t *testing.T) {
		var page []User
		require.NoError(t, store.ListScopedWithQuery("acctA", &Query{Prefix: "user", Limit: 3}, &page))
		require.Len(t, page, 3)
		assert.Equal(t, all[:3], page)
	})

	t.Run("offset returns the next page and differs", func(t *testing.T) {
		var first, second []User
		require.NoError(t, store.ListScopedWithQuery("acctA", &Query{Prefix: "user", Limit: 3}, &first))
		require.NoError(t, store.ListScopedWithQuery("acctA", &Query{Prefix: "user", Limit: 3, Offset: 3}, &second))
		require.Len(t, second, 3)
		assert.Equal(t, all[3:6], second)
		firstIDs := uuidsOf(first)
		for _, u := range second {
			assert.False(t, firstIDs[u.UUID], "second page must not overlap the first")
		}
	})

	t.Run("offset beyond the set returns empty", func(t *testing.T) {
		var page []User
		require.NoError(t, store.ListScopedWithQuery("acctA", &Query{Prefix: "user", Offset: 100}, &page))
		assert.Empty(t, page)
	})

	t.Run("limit zero returns everything", func(t *testing.T) {
		var page []User
		require.NoError(t, store.ListScopedWithQuery("acctA", &Query{Prefix: "user", Limit: 0}, &page))
		assert.Len(t, page, 10)
	})
}

// TestScopeInjectionGuard asserts every scoped method rejects an accountID or
// prefix that is empty or contains the ':' delimiter.
func TestScopeInjectionGuard(t *testing.T) {
	store := testOpen(t)

	var obj User
	var slice []User
	filters := []Filter{{"lastname", "x"}}

	// Each closure invokes one scoped method with the given account/prefix and
	// returns its error.
	methods := map[string]func(acct, prefix string) error{
		"PutScoped": func(acct, prefix string) error {
			return store.PutScoped(acct, prefix, "key", &obj)
		},
		"GetScoped": func(acct, prefix string) error {
			return store.GetScoped(acct, prefix, "key", &obj)
		},
		"DeleteScoped": func(acct, prefix string) error {
			return store.DeleteScoped(acct, prefix, "key")
		},
		"ListScoped": func(acct, prefix string) error {
			return store.ListScoped(acct, prefix, &slice)
		},
		"ListScopedWithQuery": func(acct, prefix string) error {
			return store.ListScopedWithQuery(acct, &Query{Prefix: prefix}, &slice)
		},
		"GetScopedCount": func(acct, prefix string) error {
			_, err := store.GetScopedCount(acct, &Query{Prefix: prefix})
			return err
		},
		"DeleteScopedByPrefix": func(acct, prefix string) error {
			return store.DeleteScopedByPrefix(acct, prefix)
		},
		"GetScopedByFilter": func(acct, prefix string) error {
			return store.GetScopedByFilter(acct, prefix, &filters, &obj)
		},
	}

	bad := []struct {
		name         string
		acct, prefix string
	}{
		{"accountID with colon", "acctA:evil", "user"},
		{"empty accountID", "", "user"},
		{"prefix with colon", "acctA", "us:er"},
		{"empty prefix", "acctA", ""},
	}

	for name, fn := range methods {
		for _, b := range bad {
			t.Run(fmt.Sprintf("%s/%s", name, b.name), func(t *testing.T) {
				err := fn(b.acct, b.prefix)
				require.Error(t, err)
				assert.ErrorIs(t, err, errInvalidScope)
			})
		}
	}
}

// TestScopedKeyWithColon confirms the trailing KEY segment may contain colons
// (time-series keys use "lat:lon:ts") even though accountID/prefix may not.
func TestScopedKeyWithColon(t *testing.T) {
	store := testOpen(t)
	want := User{Kind: "wpt", FirstName: "Buoy", LastName: "One", Email: "wpt@test.com"}
	require.NoError(t, store.PutScoped("acct", "wpt", "12.34:56.78:1699999999", &want))

	var got User
	require.NoError(t, store.GetScoped("acct", "wpt", "12.34:56.78:1699999999", &got))
	assert.Equal(t, want, got)
}

// TestDeleteScopedRoundTrip deletes one scoped key and confirms a sibling in the
// same account survives.
func TestDeleteScopedRoundTrip(t *testing.T) {
	store := testOpen(t)
	keep := User{FirstName: "Keep"}
	drop := User{FirstName: "Drop"}
	require.NoError(t, store.PutScoped("acct", "user", "keep", &keep))
	require.NoError(t, store.PutScoped("acct", "user", "drop", &drop))

	require.NoError(t, store.DeleteScoped("acct", "user", "drop"))

	var got User
	err := store.GetScoped("acct", "user", "drop", &got)
	require.Error(t, err, "deleted key must be gone")

	var stillThere User
	require.NoError(t, store.GetScoped("acct", "user", "keep", &stillThere))
	assert.Equal(t, "Keep", stillThere.FirstName)
}

// TestPutScopedWithTTL confirms values are present immediately regardless of TTL
// length; actual expiry is only exercised outside -short (mirrors TestTTL).
func TestPutScopedWithTTL(t *testing.T) {
	store := testOpen(t)
	shortLived := User{FirstName: "Short"}
	longLived := User{FirstName: "Long"}
	require.NoError(t, store.PutScopedWithTTL("acct", "user", "short", &shortLived, 1*time.Second))
	require.NoError(t, store.PutScopedWithTTL("acct", "user", "long", &longLived, 1*time.Hour))

	var got User
	require.NoError(t, store.GetScoped("acct", "user", "short", &got))
	assert.Equal(t, "Short", got.FirstName)
	require.NoError(t, store.GetScoped("acct", "user", "long", &got))
	assert.Equal(t, "Long", got.FirstName)

	if !testing.Short() {
		time.Sleep(2 * time.Second)
		err := store.GetScoped("acct", "user", "short", &got)
		assert.Error(t, err, "short-TTL value should have expired")
		require.NoError(t, store.GetScoped("acct", "user", "long", &got))
		assert.Equal(t, "Long", got.FirstName)
	}
}

func TestGetKeyExists(t *testing.T) {
	store := testOpen(t)

	t.Run("missing key is false with no error", func(t *testing.T) {
		ok, err := store.GetKeyExists("does-not-exist")
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("true after storing true", func(t *testing.T) {
		require.NoError(t, store.PutOneRaw("flag", "true"))
		ok, err := store.GetKeyExists("flag")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("false after storing false", func(t *testing.T) {
		require.NoError(t, store.PutOneRaw("flag", "false"))
		ok, err := store.GetKeyExists("flag")
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

// TestPutStreamSkips confirms the fixed PutStream skips objects with no uuid (and
// non-object entries) while still storing the valid ones.
func TestPutStreamSkips(t *testing.T) {
	store := testOpen(t)

	// One valid uuid object, one object without a uuid, and one non-object entry.
	stream := `[{"uuid":"keep-1","firstname":"Valid"},{"firstname":"NoUUID"},"just-a-string"]`
	require.NoError(t, store.PutStream("items", strings.NewReader(stream)))

	var got []map[string]interface{}
	require.NoError(t, store.Get("items", &got))
	require.Len(t, got, 1, "only the object with a uuid should be stored")
	assert.Equal(t, "Valid", got[0]["firstname"])

	t.Run("malformed truncated stream errors", func(t *testing.T) {
		err := store.PutStream("bad", strings.NewReader(`[{"uuid":"x"},{"uuid":`))
		assert.Error(t, err)
	})
}

// TestPutSliceMissingID confirms PutSlice errors when an element lacks a value at
// the id path (otherwise elements would collide on "prefix:").
func TestPutSliceMissingID(t *testing.T) {
	store := testOpen(t)

	type TestObject struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}

	t.Run("empty id field errors", func(t *testing.T) {
		objs := []TestObject{{"David", "123"}, {"Salee", ""}}
		err := store.PutSlice("things", "id", objs)
		require.Error(t, err)
	})

	t.Run("missing id path errors", func(t *testing.T) {
		objs := []TestObject{{Name: "David", ID: "1"}}
		err := store.PutSlice("things", "nonexistent", objs)
		require.Error(t, err)
	})
}

// TestRawVariants covers the raw string round-trip, TTL put, single-key delete,
// and raw filter lookup.
func TestRawVariants(t *testing.T) {
	store := testOpen(t)

	t.Run("PutOneRaw/GetOneRaw round-trip", func(t *testing.T) {
		require.NoError(t, store.PutOneRaw("raw:1", "hello"))
		got, err := store.GetOneRaw("raw:1")
		require.NoError(t, err)
		assert.Equal(t, "hello", got)
	})

	t.Run("PutOneWithTTL present immediately", func(t *testing.T) {
		u := User{FirstName: "TTL"}
		require.NoError(t, store.PutOneWithTTL("ttl:1", &u, 1*time.Hour))
		var got User
		require.NoError(t, store.GetOne("ttl:1", &got))
		assert.Equal(t, "TTL", got.FirstName)
	})

	t.Run("DeleteOneByKey removes the key", func(t *testing.T) {
		require.NoError(t, store.PutOneRaw("del:1", "x"))
		require.NoError(t, store.DeleteOneByKey("del:1"))
		_, err := store.GetOneRaw("del:1")
		assert.Error(t, err)
	})

	t.Run("GetOneByFilterRaw finds the match", func(t *testing.T) {
		u := User{FirstName: "Filtered", LastName: "Match", Email: "fm@test.com"}
		require.NoError(t, store.PutOne("filt:1", &u))
		got, err := store.GetOneByFilterRaw("filt", Filter{Path: "lastname", Value: "Match"})
		require.NoError(t, err)
		require.NotEmpty(t, got)
		assert.Contains(t, got, "Filtered")
	})

	t.Run("GetOneByFilterRaw returns empty when nothing matches", func(t *testing.T) {
		got, err := store.GetOneByFilterRaw("filt", Filter{Path: "lastname", Value: "Nope"})
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

// TestStartGC is a smoke test: GC runs on its own goroutine on a short interval;
// the store must keep serving reads and writes and must not panic.
func TestStartGC(t *testing.T) {
	store := testOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store.StartGC(ctx, 20*time.Millisecond)

	// Write and read some data while GC ticks in the background.
	putNUsers(t, store, 5)
	var got []User
	require.NoError(t, store.Get("users", &got))
	assert.Len(t, got, 5)

	// Let a couple of GC ticks fire, then confirm the store is still usable.
	if !testing.Short() {
		time.Sleep(60 * time.Millisecond)
	}
	require.NoError(t, store.PutOneRaw("after-gc", "true"))
	ok, err := store.GetKeyExists("after-gc")
	require.NoError(t, err)
	assert.True(t, ok)
}
