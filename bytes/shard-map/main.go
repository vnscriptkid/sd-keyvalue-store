// Idea: reduce contention on a map by sharding it into multiple maps
package main

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type Sharded[K comparable, V any] struct {
	shards []struct {
		mu sync.RWMutex
		m  map[K]V
	}
}

func NewSharded[K comparable, V any](n int) *Sharded[K, V] {
	s := &Sharded[K, V]{shards: make([]struct {
		mu sync.RWMutex
		m  map[K]V
	}, n)}
	for i := range s.shards {
		s.shards[i].m = make(map[K]V)
	}
	return s
}

// pickShard: replace with xxhash for speed; fnv is OK for demo
func (s *Sharded[K, V]) shardOf(key K) int {
	h := fnv.New32a()
	fmt.Fprint(h, key)
	return int(h.Sum32()) % len(s.shards)
}

func (s *Sharded[K, V]) Get(key K) (V, bool) {
	sh := &s.shards[s.shardOf(key)]
	sh.mu.RLock()
	v, ok := sh.m[key]
	sh.mu.RUnlock()
	return v, ok
}

func (s *Sharded[K, V]) Put(key K, v V) {
	sh := &s.shards[s.shardOf(key)]
	sh.mu.Lock()
	sh.m[key] = v
	sh.mu.Unlock()
}

func main() {
	const (
		shards     = 64
		perShard   = 1000
		totalItems = shards * perShard
	)

	m := NewSharded[string, int](shards)

	var wg sync.WaitGroup
	wg.Add(shards)
	for i := 0; i < shards; i++ {
		i := i
		go func() {
			base := i * perShard
			for j := 0; j < perShard; j++ {
				key := fmt.Sprintf("k-%d", base+j)
				m.Put(key, 1)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	sum := 0
	for i := 0; i < totalItems; i++ {
		key := fmt.Sprintf("k-%d", i)
		if v, ok := m.Get(key); ok {
			sum += v
		}
	}
	fmt.Println(sum)
}
