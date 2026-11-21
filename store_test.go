package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type UserConfig struct {
	Kind      string `json:"kind"`
	FirstName string `json:"firstname"`
	LastName  string `json:"lastname"`
	Email     string `json:"email"`
	Password  string `json:"password"`
}

type User struct {
	Kind       string    `json:"kind"`
	FirstName  string    `json:"firstname"`
	LastName   string    `json:"lastname"`
	Email      string    `json:"email"`
	Active     bool      `json:"active"`
	LoginCount int       `json:"login_count"`
	LoggedIn   bool      `json:"loggedin"`
	UUID       uuid.UUID `json:"uuid"`
}

func NewUser(u UserConfig) (User, error) {
	if u.Password == "" {
		return User{}, errors.New("need password")
	}
	return User{
		Kind:      u.Kind,
		FirstName: u.FirstName,
		LastName:  u.LastName,
		Email:     u.Email,
		UUID:      uuid.New(),
	}, nil
}

var (
	tcs = []UserConfig{
		{Kind: "user", FirstName: "John", LastName: "Doe", Email: "jdoe@test.com", Password: "password123"},
		{Kind: "user", FirstName: "Joe", LastName: "Buck", Email: "jbuck@test.com", Password: "password123"},
		{Kind: "user", FirstName: "Troy", LastName: "Aikman", Email: "taikman@test.com", Password: "password123"},
		{Kind: "user", FirstName: "Dak", LastName: "Prescott", Email: "tprescott@test.com", Password: "password123"},
	}
	usersStream = `[{"kind":"user", "firstname":"Troy","lastname":"Aikman","email":"taikman@test.com","active":false,"login_count":0,"loggedin":false,"uuid":"d82e0bf4-fd90-4ee6-8ae8-cacc56d5005d"},{"active":false,"email":"jbuck@test.com","kind":"user","firstname":"Joe","lastname":"Buck","loggedin":false,"login_count":0,"uuid":"d8e95c66-021e-41a0-9065-c19f820755bc"},{"active":false,"email":"tprescott@test.com","kind":"user","firstname":"Dak","lastname":"Prescott","loggedin":false,"login_count":0,"uuid":"d95808e3-f142-49fd-9e16-8f07d5d21077"}]`
)

type quietLogger struct{}

func (quietLogger) Debug(string) {}
func (quietLogger) Info(string)  {}
func (quietLogger) Warn(string)  {}
func (quietLogger) Error(string) {}

func testOpen(t testing.TB) *Store {
	t.Helper()
	store, err := NewStore(&Options{
		Location: t.TempDir(),
		Logger:   quietLogger{},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func putUsers(t *testing.T, store *Store) []User {
	t.Helper()
	users := make([]User, 0, len(tcs))
	for _, c := range tcs {
		u, err := NewUser(c)
		require.NoError(t, err)
		require.NoError(t, store.PutOne(fmt.Sprintf("users:%s", u.UUID), &u))
		users = append(users, u)
	}
	return users
}

func TestPutGetDeleteSlice(t *testing.T) {
	store := testOpen(t)

	type TestObject struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}
	tos := []TestObject{{"David", "123"}, {"Salee", "321"}, {"Rachel", "333"}}
	require.NoError(t, store.PutSlice("testing", "id", tos))

	var got []TestObject
	require.NoError(t, store.Get("testing", &got))
	assert.Len(t, got, len(tos))

	require.NoError(t, store.DeleteByPrefix("testing"))
	got = nil
	require.NoError(t, store.Get("testing", &got))
	assert.Empty(t, got)
}

func TestGetEmptyPrefix(t *testing.T) {
	store := testOpen(t)
	var users []User
	require.NoError(t, store.Get("users", &users))
	assert.Empty(t, users)
}

func TestPutAndGetOne(t *testing.T) {
	store := testOpen(t)
	users := putUsers(t, store)

	for _, u := range users {
		var byKey User
		require.NoError(t, store.GetOne(fmt.Sprintf("users:%s", u.UUID), &byKey))
		assert.Equal(t, u, byKey)

		var byFilter User
		filters := []Filter{{"lastname", u.LastName}}
		require.NoError(t, store.GetOneByFilter("users", &filters, &byFilter))
		assert.Equal(t, u, byFilter)
	}

	var all []User
	require.NoError(t, store.Get("users", &all))
	assert.Len(t, all, len(tcs))
}

func TestGetWithQuery(t *testing.T) {
	store := testOpen(t)
	users := putUsers(t, store)

	t.Run("single filter", func(t *testing.T) {
		for _, u := range users {
			var got []User
			q := Query{Prefix: "users", Filters: []*Filter{{"lastname", u.LastName}}}
			require.NoError(t, store.GetWithQuery(&q, &got))
			require.Len(t, got, 1)
			assert.Equal(t, u, got[0])
		}
	})

	t.Run("no filters returns everything", func(t *testing.T) {
		var got []User
		require.NoError(t, store.GetWithQuery(&Query{Prefix: "users"}, &got))
		assert.Len(t, got, len(users))
	})

	t.Run("no match returns empty slice", func(t *testing.T) {
		var got []User
		q := Query{Prefix: "users", Filters: []*Filter{{"lastname", "Nobody"}}}
		require.NoError(t, store.GetWithQuery(&q, &got))
		assert.Empty(t, got)
	})
}

func TestGetCount(t *testing.T) {
	store := testOpen(t)
	putUsers(t, store)

	n, err := store.GetCount(&Query{Prefix: "users"})
	require.NoError(t, err)
	assert.Equal(t, len(tcs), n)

	n, err = store.GetCount(&Query{Prefix: "users", Filters: []*Filter{{"firstname", "Dak"}}})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestStream(t *testing.T) {
	store := testOpen(t)
	require.NoError(t, store.PutStream("users", strings.NewReader(usersStream)))

	var wg sync.WaitGroup
	buf := new(bytes.Buffer)
	store.GetStream(buf, &wg, &Query{Prefix: "users"})
	wg.Wait()
	var got []User
	require.NoError(t, jsonUnmarshal(buf.Bytes(), &got))
	assert.Len(t, got, 3)

	t.Run("limit and offset", func(t *testing.T) {
		buf := new(bytes.Buffer)
		store.GetStream(buf, &wg, &Query{Prefix: "users", Limit: 1, Offset: 1})
		wg.Wait()
		var page []User
		require.NoError(t, jsonUnmarshal(buf.Bytes(), &page))
		assert.Len(t, page, 1)
	})

	t.Run("malformed stream returns error", func(t *testing.T) {
		err := store.PutStream("bad", strings.NewReader(`[{"uuid":"x"},{"uuid":`))
		assert.Error(t, err)
	})
}

func TestMeta(t *testing.T) {
	store := testOpen(t)
	ttl := 10 * time.Second
	for _, u := range tcs {
		require.NoError(t, store.PutOneWithTTLAndMeta(u.Email+"-1", &u, ttl, 1))
		require.NoError(t, store.PutOneWithTTLAndMeta(u.Email+"-2", &u, ttl, 2))
	}

	var users1, users2 []User
	require.NoError(t, store.GetByMeta(1, &users1))
	require.NoError(t, store.GetByMeta(2, &users2))
	assert.Len(t, users1, len(tcs))
	assert.Len(t, users2, len(tcs))

	require.NoError(t, store.DeleteByMeta(1))
	users1 = nil
	require.NoError(t, store.GetByMeta(1, &users1))
	assert.Empty(t, users1)

	require.NoError(t, store.DeleteByMeta(2))
	users2 = nil
	require.NoError(t, store.GetByMeta(2, &users2))
	assert.Empty(t, users2)
}

func TestTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for badger TTL expiry")
	}
	store := testOpen(t)
	ttl := 1 * time.Second
	for _, u := range tcs {
		require.NoError(t, store.PutOneWithTTLAndMeta(u.Email+"-1", &u, ttl, 1))
	}
	time.Sleep(2 * ttl)
	var users []User
	require.NoError(t, store.GetByMeta(1, &users))
	assert.Empty(t, users)
}

func TestScoped(t *testing.T) {
	store := testOpen(t)
	a, b := "acctA", "acctB"
	users := putScopedUsers(t, store, a)
	putScopedUsers(t, store, b)

	t.Run("get by key", func(t *testing.T) {
		var got User
		require.NoError(t, store.GetScoped(a, "user", users[0].UUID.String(), &got))
		assert.Equal(t, users[0], got)
	})

	t.Run("list is isolated per account", func(t *testing.T) {
		var got []User
		require.NoError(t, store.ListScoped(a, "user", &got))
		assert.Len(t, got, len(tcs))
	})

	t.Run("count", func(t *testing.T) {
		n, err := store.GetScopedCount(a, &Query{Prefix: "user"})
		require.NoError(t, err)
		assert.Equal(t, len(tcs), n)
	})

	t.Run("stream", func(t *testing.T) {
		var wg sync.WaitGroup
		buf := new(bytes.Buffer)
		store.GetScopedStream(buf, &wg, b, &Query{Prefix: "user"})
		wg.Wait()
		var got []User
		require.NoError(t, jsonUnmarshal(buf.Bytes(), &got))
		assert.Len(t, got, len(tcs))
	})

	t.Run("filter", func(t *testing.T) {
		var got User
		filters := []Filter{{"lastname", "Aikman"}}
		require.NoError(t, store.GetScopedByFilter(a, "user", &filters, &got))
		assert.Equal(t, "Troy", got.FirstName)
	})

	t.Run("delete by prefix leaves other account intact", func(t *testing.T) {
		require.NoError(t, store.DeleteScopedByPrefix(a, "user"))
		var gotA, gotB []User
		require.NoError(t, store.ListScoped(a, "user", &gotA))
		require.NoError(t, store.ListScoped(b, "user", &gotB))
		assert.Empty(t, gotA)
		assert.Len(t, gotB, len(tcs))
	})
}

func TestCollectMetrics(t *testing.T) {
	store := testOpen(t)
	putUsers(t, store)
	m := store.CollectMetrics()
	assert.Equal(t, int64(len(tcs)), m.NumKeys)
	assert.Equal(t, int64(len(tcs)), m.PutOps)
	assert.GreaterOrEqual(t, m.SizeTotal, int64(0))
}

func TestInTimeRange(t *testing.T) {
	base := time.UnixMilli(1_000_000)
	key := fmt.Sprintf("flow:%d:%d", base.UnixMilli(), base.Add(time.Minute).UnixMilli())
	cases := []struct {
		name string
		tr   TimeRange
		want bool
	}{
		{"inside both bounds", TimeRange{base.Add(-time.Second), base.Add(2 * time.Minute)}, true},
		{"starts too early", TimeRange{base.Add(time.Second), base.Add(2 * time.Minute)}, false},
		{"ends too late", TimeRange{base.Add(-time.Second), base.Add(30 * time.Second)}, false},
		{"start only", TimeRange{Start: base}, true},
		{"end only", TimeRange{End: base.Add(time.Minute)}, true},
		{"empty range", TimeRange{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, inTimeRange(key, c.tr))
		})
	}
	assert.False(t, inTimeRange("malformed", TimeRange{Start: base}))
}

func jsonUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func putScopedUsers(t *testing.T, store *Store, accountID string) []User {
	t.Helper()
	users := make([]User, 0, len(tcs))
	for _, c := range tcs {
		u, err := NewUser(c)
		require.NoError(t, err)
		require.NoError(t, store.PutScoped(accountID, "user", u.UUID.String(), &u))
		users = append(users, u)
	}
	return users
}
