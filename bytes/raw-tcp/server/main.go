package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type Store struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewStore() *Store {
	return &Store{m: make(map[string]string)}
}

func (s *Store) Set(k, v string) {
	s.mu.Lock()
	s.m[k] = v
	s.mu.Unlock()
}

func (s *Store) Get(k string) (string, bool) {
	s.mu.RLock()
	v, ok := s.m[k]
	s.mu.RUnlock()
	return v, ok
}

func (s *Store) Del(k string) bool {
	s.mu.Lock()
	_, ok := s.m[k]
	if ok {
		delete(s.m, k)
	}
	s.mu.Unlock()
	return ok
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	return keys
}

func writeLine(w *bufio.Writer, line string) error {
	_, err := w.WriteString(line + "\n")
	if err != nil {
		return err
	}
	return w.Flush()
}

func handleConn(conn net.Conn, st *Store) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	_ = writeLine(w, "+OK kv-server ready")

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			// client disconnected
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Very simple parser:
		// - split into tokens by spaces
		// - SET uses: SET key <rest-of-line as value>
		parts := strings.Split(line, " ")
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PING":
			_ = writeLine(w, "+PONG")

		case "QUIT":
			_ = writeLine(w, "+BYE")
			return

		case "SET":
			if len(parts) < 3 {
				_ = writeLine(w, "-ERR usage: SET key value")
				continue
			}
			key := parts[1]
			// keep spaces in value
			value := strings.TrimSpace(strings.TrimPrefix(line, parts[0]+" "+key))
			st.Set(key, value)
			_ = writeLine(w, "+OK")

		case "GET":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: GET key")
				continue
			}
			key := parts[1]
			if v, ok := st.Get(key); ok {
				// simple bulk string: $<len>\n<value>
				_ = writeLine(w, fmt.Sprintf("$%d", len(v)))
				_ = writeLine(w, v)
			} else {
				_ = writeLine(w, "$-1")
			}

		case "DEL":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: DEL key")
				continue
			}
			key := parts[1]
			if st.Del(key) {
				_ = writeLine(w, ":1")
			} else {
				_ = writeLine(w, ":0")
			}

		case "KEYS":
			keys := st.Keys()
			_ = writeLine(w, fmt.Sprintf("*%d", len(keys)))
			for _, k := range keys {
				_ = writeLine(w, "+"+k)
			}

		default:
			_ = writeLine(w, "-ERR unknown command")
		}
	}
}

func main() {
	addr := "127.0.0.1:6380"
	st := NewStore()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("kv-server listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, st)
	}

	// Using netcat: nc 127.0.0.1:6380
}
