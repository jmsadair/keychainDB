package lru

import (
	"container/list"
)

type cacheEntry[V any] struct {
	value   V
	element *list.Element
}

// LruCache is a basic implementation of a generic, least-recently-used cache. This implementation is not thread-safe.
type LruCache[K comparable, V any] struct {
	lru              *list.List
	lookup           map[K]*cacheEntry[V]
	evictionCallback func(K, V)
	maxCapacity      int
}

// NewLruCache creates an LruCache with the specified max capacity.
func NewLruCache[K comparable, V any](maxCapacity int, evictionCallback func(K, V)) *LruCache[K, V] {
	return &LruCache[K, V]{
		maxCapacity:      maxCapacity,
		lru:              list.New(),
		evictionCallback: evictionCallback,
		lookup:           make(map[K]*cacheEntry[V], maxCapacity),
	}
}

// Set sets the value of a key and updates the recentness of the key.
// If the cache is already at maximum capacity, then the least recently
// used entry will be evicted.
func (l *LruCache[K, V]) Set(key K, value V) {
	entry, ok := l.lookup[key]
	if ok {
		entry.value = value
		entry.element.Value = key
		l.lru.MoveToBack(entry.element)
		return
	}
	if l.lru.Len() == l.maxCapacity {
		toRemove := l.lru.Front()
		l.lru.Remove(toRemove)
		removeKey := toRemove.Value.(K)
		removeValue := l.lookup[removeKey].value
		delete(l.lookup, removeKey)
		l.evictionCallback(removeKey, removeValue)
	}
	entry = &cacheEntry[V]{value: value, element: l.lru.PushBack(key)}
	l.lookup[key] = entry
}

// Get retrieves the value of a key and updates the recentness of the key.
func (l *LruCache[K, V]) Get(key K) (V, bool) {
	var value V
	entry, ok := l.lookup[key]
	if !ok {
		return value, ok
	}
	l.lru.MoveToBack(entry.element)
	return entry.value, ok
}
