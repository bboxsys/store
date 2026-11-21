package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestGetScopedStreamProjected verifies the projection stream returns a value
// for every record in the account (order preserved by the single iterator) and
// is isolated per account.
func TestGetScopedStreamProjected(t *testing.T) {
	store := testOpen(t)
	a, b := "acctA", "acctB"
	usersA := putScopedUsers(t, store, a)
	putScopedUsers(t, store, b)

	// Identity projection (nil transform) returns full values.
	var wg sync.WaitGroup
	buf := new(bytes.Buffer)
	store.GetScopedStreamProjected(buf, &wg, a, &Query{Prefix: "user"}, nil)
	wg.Wait()

	var got []User
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	require.Len(t, got, len(usersA))

	wantNames := make([]string, 0, len(usersA))
	for _, u := range usersA {
		wantNames = append(wantNames, u.LastName)
	}
	gotNames := make([]string, 0, len(got))
	for _, u := range got {
		gotNames = append(gotNames, u.LastName)
	}
	sort.Strings(wantNames)
	sort.Strings(gotNames)
	assert.Equal(t, wantNames, gotNames)
}

// TestGetScopedStreamProjectedProjection verifies transform projects fields and
// that returning include=false skips an item.
func TestGetScopedStreamProjectedProjection(t *testing.T) {
	store := testOpen(t)
	users := putScopedUsers(t, store, "acctA")

	// Project to {last} and skip "Aikman".
	project := func(value []byte) ([]byte, bool) {
		last := gjson.GetBytes(value, "lastname").String()
		if last == "Aikman" {
			return nil, false
		}
		return []byte(fmt.Sprintf(`{"last":%q}`, last)), true
	}

	var wg sync.WaitGroup
	buf := new(bytes.Buffer)
	store.GetScopedStreamProjected(buf, &wg, "acctA", &Query{Prefix: "user"}, project)
	wg.Wait()

	var got []struct {
		Last string `json:"last"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Len(t, got, len(users)-1) // Aikman skipped
	for _, g := range got {
		assert.NotEqual(t, "Aikman", g.Last)
	}
}

// TestGetScopedStreamProjectedEmpty verifies an empty prefix yields an empty
// JSON array.
func TestGetScopedStreamProjectedEmpty(t *testing.T) {
	store := testOpen(t)
	var wg sync.WaitGroup
	buf := new(bytes.Buffer)
	store.GetScopedStreamProjected(buf, &wg, "acctA", &Query{Prefix: "user"}, nil)
	wg.Wait()
	assert.Equal(t, "[]", buf.String())
}
