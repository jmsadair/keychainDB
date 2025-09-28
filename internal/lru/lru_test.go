package lru

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	capacity := 10
	lru := NewLruCache(capacity, func(string, int) {})

	// Set some values.
	key1 := "key1"
	value1 := 1
	lru.Set(key1, value1)
	key2 := "key2"
	value2 := 2
	lru.Set(key2, value2)
	key3 := "key3"
	value3 := 3
	lru.Set(key3, value3)

	// Ensure those values can be retrieved.
	storedValue1, ok := lru.Get(key1)
	require.True(t, ok)
	require.Equal(t, value1, storedValue1)
	storedValue2, ok := lru.Get(key2)
	require.True(t, ok)
	require.Equal(t, value2, storedValue2)
	storedValue3, ok := lru.Get(key3)
	require.True(t, ok)
	require.Equal(t, value3, storedValue3)
}

func TestUpdate(t *testing.T) {
	capacity := 10
	lru := NewLruCache(capacity, func(string, int) {})

	// Set a value.
	key := "key"
	value := 1
	lru.Set(key, value)
	storedValue, ok := lru.Get(key)
	require.True(t, ok)
	require.Equal(t, value, storedValue)

	// Try updating that value.
	value = 2
	lru.Set(key, value)
	storedValue, ok = lru.Get(key)
	require.True(t, ok)
	require.Equal(t, value, storedValue)
}

func TestGetMissing(t *testing.T) {
	capacity := 10
	lru := NewLruCache(capacity, func(string, int) {})

	key := "key"
	_, ok := lru.Get(key)
	require.False(t, ok)
}

func TestEvict(t *testing.T) {
	evicted := make(map[string]int)
	callback := func(k string, v int) {
		evicted[k] = v
	}
	capacity := 2
	lru := NewLruCache(capacity, callback)

	// Fill the cache to capcity.
	key1 := "key1"
	value1 := 1
	lru.Set(key1, value1)
	_, ok := lru.Get(key1)
	require.True(t, ok)
	key2 := "key2"
	value2 := 2
	lru.Set(key2, value2)
	_, ok = lru.Get(key2)
	require.True(t, ok)

	// Try adding another value.
	// The first key added should have been evicted.
	// The second and third key added should remain.
	key3 := "key3"
	value3 := 3
	lru.Set(key3, value3)
	_, ok = lru.Get(key1)
	require.False(t, ok)
	_, ok = lru.Get(key3)
	require.True(t, ok)
	_, ok = lru.Get(key2)
	require.True(t, ok)
	require.Len(t, evicted, 1)
	require.Equal(t, evicted[key1], value1)
	delete(evicted, key1)

	// Try adding another value.
	// The third key added should have been evicted.
	// The second and fourth key added should remain.
	key4 := "key4"
	value4 := 4
	lru.Set(key4, value4)
	_, ok = lru.Get(key3)
	require.False(t, ok)
	_, ok = lru.Get(key4)
	require.True(t, ok)
	_, ok = lru.Get(key2)
	require.True(t, ok)
	require.Len(t, evicted, 1)
	require.Equal(t, evicted[key3], value3)
}
