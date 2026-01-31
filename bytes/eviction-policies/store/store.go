package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/eviction"
	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/lib"
)

type Store struct {
	mu sync.Mutex

	items map[string]*lib.Entry

	// Limits (0 means "no limit")
	maxKeys  int
	maxBytes int64

	// Current usage
	keysUsed  int
	bytesUsed int64

	evictor eviction.Evictor
}

func NewStore(maxKeys int, maxBytes int64, evictor eviction.Evictor) *Store {
	return &Store{
		items:    make(map[string]*lib.Entry),
		maxKeys:  maxKeys,
		maxBytes: maxBytes,
		evictor:  evictor,
	}
}

// Simplified memory accounting: key bytes + value bytes.
// Real overhead is higher; this is a learning/demo approximation.
func approxEntryBytes(key string, val []byte) int64 {
	return int64(len(key) + len(val))
}

func (s *Store) Stats() (keys int, bytes int64, policy string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keysUsed, s.bytesUsed, s.evictor.Name()
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil, false
	}
	s.evictor.OnGet(e)

	out := make([]byte, len(e.Value))
	copy(out, e.Value)
	return out, true
}

func (s *Store) Set(key string, val []byte) error {
	if key == "" {
		return errors.New("key must not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entryBytes := approxEntryBytes(key, val)

	// If entry itself can't fit into maxBytes, fail fast (otherwise we'd evict everything and still fail).
	if s.maxBytes > 0 && entryBytes > s.maxBytes {
		return fmt.Errorf("entry too large: entryBytes=%d > maxBytes=%d", entryBytes, s.maxBytes)
	}

	if e, ok := s.items[key]; ok {
		// Update
		oldBytes := e.Bytes
		e.Value = append([]byte(nil), val...)
		e.Bytes = entryBytes
		s.bytesUsed += (e.Bytes - oldBytes)

		s.evictor.OnUpdate(e)
	} else {
		// Insert
		e := &lib.Entry{
			Key:   key,
			Value: append([]byte(nil), val...),
			Bytes: entryBytes,
		}
		s.items[key] = e
		s.keysUsed++
		s.bytesUsed += e.Bytes

		s.evictor.OnAdd(e)
	}

	s.evictIfNeededLocked()
	return nil
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return false
	}
	s.removeEntryLocked(e)
	return true
}

func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.items))
	for k := range s.items {
		out = append(out, k)
	}
	return out
}

func (s *Store) evictIfNeededLocked() {
	for (s.maxKeys > 0 && s.keysUsed > s.maxKeys) ||
		(s.maxBytes > 0 && s.bytesUsed > s.maxBytes) {

		v := s.evictor.Victim()
		if v == nil {
			return
		}

		// Safety: ensure victim is still present.
		cur, ok := s.items[v.Key]
		if !ok {
			// Evictor might be desynced; ask it to forget.
			s.evictor.OnRemove(v)
			continue
		}
		s.removeEntryLocked(cur)
	}
}

func (s *Store) removeEntryLocked(e *lib.Entry) {
	delete(s.items, e.Key)
	s.keysUsed--
	s.bytesUsed -= e.Bytes
	s.evictor.OnRemove(e)
}
