package cache

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type entry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
}

type LRU[K comparable, V any] struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	items      map[K]*list.Element
	order      *list.List
}

func NewLRU[K comparable, V any](maxEntries int, ttl time.Duration) (*LRU[K, V], error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("max entries must be positive")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be positive")
	}
	return &LRU[K, V]{
		maxEntries: maxEntries,
		ttl:        ttl,
		items:      make(map[K]*list.Element, maxEntries),
		order:      list.New(),
	}, nil
}

func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	element, ok := l.items[key]
	if !ok {
		var zero V
		return zero, false
	}

	entryValue := element.Value.(*entry[K, V])
	if time.Now().After(entryValue.expiresAt) {
		l.removeElement(element)
		var zero V
		return zero, false
	}

	l.order.MoveToFront(element)
	return entryValue.value, true
}

func (l *LRU[K, V]) Add(key K, value V) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if element, ok := l.items[key]; ok {
		entryValue := element.Value.(*entry[K, V])
		entryValue.value = value
		entryValue.expiresAt = time.Now().Add(l.ttl)
		l.order.MoveToFront(element)
		return
	}

	entryValue := &entry[K, V]{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(l.ttl),
	}
	item := l.order.PushFront(entryValue)
	l.items[key] = item

	if l.order.Len() > l.maxEntries {
		l.removeOldest()
	}
}

func (l *LRU[K, V]) removeOldest() {
	oldest := l.order.Back()
	if oldest == nil {
		return
	}
	l.removeElement(oldest)
}

func (l *LRU[K, V]) removeElement(element *list.Element) {
	entryValue := element.Value.(*entry[K, V])
	delete(l.items, entryValue.key)
	l.order.Remove(element)
}
