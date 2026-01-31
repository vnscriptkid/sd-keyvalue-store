package main

import (
	"fmt"

	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/eviction"
	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/store"
)

// =====================
// Demo
// =====================

func mustSet(s *store.Store, k, v string) {
	if err := s.Set(k, []byte(v)); err != nil {
		panic(err)
	}
}

func demo(policy eviction.Evictor) {
	fmt.Printf("\n===== Policy: %s =====\n", policy.Name())

	// Small limits so eviction happens quickly
	maxKeys := 3
	maxBytes := int64(20)

	s := store.NewStore(maxKeys, maxBytes, policy)

	// Fill
	mustSet(s, "a", "1111") // ~5 bytes
	mustSet(s, "b", "2222") // ~5 => 10
	mustSet(s, "c", "3333") // ~5 => 15

	// Access patterns affect LRU/LFU
	_, _ = s.Get("a") // a touched
	_, _ = s.Get("a") // a more frequent
	_, _ = s.Get("b") // b touched

	// This will exceed maxBytes (~15 + 7 = 22) and exceed maxKeys (4 > 3)
	mustSet(s, "d", "444444") // ~7

	keys, bytes, pol := s.Stats()
	fmt.Printf("Stats: keys=%d bytes=%d policy=%s\n", keys, bytes, pol)

	// Show what remains (Get() also touches; for demo simplicity we just check existence)
	for _, k := range []string{"a", "b", "c", "d"} {
		if v, ok := s.Get(k); ok {
			fmt.Printf("  %s=%s\n", k, string(v))
		} else {
			fmt.Printf("  %s=(missing)\n", k)
		}
	}
}

func main() {
	demo(eviction.NewLRUEvictor())
	demo(eviction.NewLFUEvictor())
	demo(eviction.NewRandomEvictor())
}
