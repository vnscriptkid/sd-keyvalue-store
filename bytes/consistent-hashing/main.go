package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// HashRing implements basic consistent hashing without virtual nodes.
type HashRing struct {
	mu     sync.RWMutex
	keys   []uint32          // sorted hashes of nodes
	lookup map[uint32]string // hash -> nodeID
}

func NewHashRing() *HashRing {
	return &HashRing{
		lookup: make(map[uint32]string),
	}
}

func (r *HashRing) Add(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h := hash32(nodeID)

	fmt.Println("Adding nodeID:", nodeID, "with hash:", h)

	// If already exists, just overwrite nodeID (or ignore; up to you)
	if _, ok := r.lookup[h]; ok {
		r.lookup[h] = nodeID
		return
	}

	r.lookup[h] = nodeID
	r.keys = append(r.keys, h)
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *HashRing) Remove(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	h := hash32(nodeID)
	if _, ok := r.lookup[h]; !ok {
		return
	}
	delete(r.lookup, h)

	// remove from r.keys
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i < len(r.keys) && r.keys[i] == h {
		r.keys = append(r.keys[:i], r.keys[i+1:]...)
	}
}

func (r *HashRing) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return "", false
	}

	h := hash32(key)

	// Find first node hash >= key hash; if none, wrap to 0 (index 0)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}

	nodeHash := r.keys[i]
	return r.lookup[nodeHash], true
}

// hash32 returns a stable 32-bit hash for a string (FNV-1a).
func hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func main() {
	ring := NewHashRing()
	ring.Add("A")
	ring.Add("BB")
	ring.Add("CCC")

	keys := []string{"user:1", "user:2", "order:9", "image:cat", "k2"}
	for _, k := range keys {
		n, _ := ring.Get(k)
		fmt.Printf("%-10s -> %s\n", k, n)
	}

	fmt.Println("\nRemove B:")
	ring.Remove("B")
	for _, k := range keys {
		n, _ := ring.Get(k)
		fmt.Printf("%-10s -> %s\n", k, n)
	}
}
