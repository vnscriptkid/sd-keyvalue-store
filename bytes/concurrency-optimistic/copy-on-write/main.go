package main

import (
	"fmt"
	"sync/atomic"
)

type Config struct {
	Routes map[string]string
	Limit  int
}

type Store struct {
	p atomic.Pointer[Config]
}

func NewStore() *Store {
	s := &Store{}
	s.p.Store(&Config{
		Routes: map[string]string{"home": "/"},
		Limit:  10,
	})
	return s
}

func (s *Store) Load() *Config {
	return s.p.Load() // readers are lock-free
}

func (s *Store) Update(fn func(c *Config)) {
	for {
		old := s.p.Load()

		// deep copy (important: maps/slices must be copied)
		next := &Config{
			Routes: make(map[string]string, len(old.Routes)),
			Limit:  old.Limit,
		}
		for k, v := range old.Routes {
			next.Routes[k] = v
		}

		fn(next)

		if s.p.CompareAndSwap(old, next) {
			return
		}
		// CAS failed => someone updated first; retry with latest
	}
}

func main() {
	s := NewStore()

	s.Update(func(c *Config) {
		c.Routes["about"] = "/about"
		c.Limit = 20
	})

	cfg := s.Load()
	fmt.Println(cfg.Limit, cfg.Routes["about"])
}
