package main

import (
	"bufio"
	"flag"
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

func handleConn(conn net.Conn, st *Store, serverName string) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	_ = writeLine(w, fmt.Sprintf("+OK %s ready", serverName))

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Split(line, " ")
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PING":
			_ = writeLine(w, "+PONG")

		case "WHOAMI":
			_ = writeLine(w, "+"+serverName)

		case "QUIT":
			_ = writeLine(w, "+BYE")
			return

		case "SET":
			if len(parts) < 3 {
				_ = writeLine(w, "-ERR usage: SET key value")
				continue
			}
			key := parts[1]
			value := strings.TrimSpace(strings.TrimPrefix(line, parts[0]+" "+key))
			st.Set(key, value)
			log.Printf("[%s] SET %s = %s", serverName, key, value)
			_ = writeLine(w, "+OK")

		case "GET":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: GET key")
				continue
			}
			key := parts[1]
			if v, ok := st.Get(key); ok {
				log.Printf("[%s] GET %s -> %s", serverName, key, v)
				_ = writeLine(w, fmt.Sprintf("$%d", len(v)))
				_ = writeLine(w, v)
			} else {
				log.Printf("[%s] GET %s -> (nil)", serverName, key)
				_ = writeLine(w, "$-1")
			}

		case "DEL":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: DEL key")
				continue
			}
			key := parts[1]
			if st.Del(key) {
				log.Printf("[%s] DEL %s -> 1", serverName, key)
				_ = writeLine(w, ":1")
			} else {
				log.Printf("[%s] DEL %s -> 0", serverName, key)
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
	port := flag.Int("port", 6381, "port to listen on")
	name := flag.String("name", "", "server name (defaults to cache-<port>)")
	flag.Parse()

	serverName := *name
	if serverName == "" {
		serverName = fmt.Sprintf("cache-%d", *port)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	st := NewStore()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[%s] listen: %v", serverName, err)
	}
	log.Printf("[%s] listening on %s", serverName, addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[%s] accept: %v", serverName, err)
			continue
		}
		go handleConn(conn, st, serverName)
	}
}
