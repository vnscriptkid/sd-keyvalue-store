package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// ──────────────────────────────────────────────────────────────────────────────
// Consistent Hashing Ring
// ──────────────────────────────────────────────────────────────────────────────

type HashRing struct {
	mu       sync.RWMutex
	keys     []uint32          // sorted hashes of nodes
	lookup   map[uint32]string // hash -> nodeAddr
	replicas int               // virtual nodes per physical node
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		lookup:   make(map[uint32]string),
		replicas: replicas,
	}
}

func (r *HashRing) Add(nodeAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		h := hash32(fmt.Sprintf("%s#%d", nodeAddr, i))
		if _, ok := r.lookup[h]; ok {
			continue
		}
		r.lookup[h] = nodeAddr
		r.keys = append(r.keys, h)
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *HashRing) Remove(nodeAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		h := hash32(fmt.Sprintf("%s#%d", nodeAddr, i))
		delete(r.lookup, h)
	}

	// rebuild keys slice
	newKeys := make([]uint32, 0, len(r.keys))
	for _, k := range r.keys {
		if _, ok := r.lookup[k]; ok {
			newKeys = append(newKeys, k)
		}
	}
	r.keys = newKeys
}

func (r *HashRing) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.keys) == 0 {
		return "", false
	}

	h := hash32(key)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}

	return r.lookup[r.keys[i]], true
}

func (r *HashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	var nodes []string
	for _, nodeAddr := range r.lookup {
		if !seen[nodeAddr] {
			seen[nodeAddr] = true
			nodes = append(nodes, nodeAddr)
		}
	}
	sort.Strings(nodes)
	return nodes
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// ──────────────────────────────────────────────────────────────────────────────
// Connection Pool
// ──────────────────────────────────────────────────────────────────────────────

type ConnPool struct {
	mu    sync.Mutex
	conns map[string]net.Conn
}

func NewConnPool() *ConnPool {
	return &ConnPool{conns: make(map[string]net.Conn)}
}

func (p *ConnPool) Get(addr string) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[addr]; ok {
		return conn, bufio.NewReader(conn), bufio.NewWriter(conn), nil
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	// read the greeting
	_, _ = r.ReadString('\n')

	p.conns[addr] = conn
	return conn, r, w, nil
}

func (p *ConnPool) Remove(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[addr]; ok {
		conn.Close()
		delete(p.conns, addr)
	}
}

func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, conn := range p.conns {
		conn.Close()
		delete(p.conns, addr)
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Proxy Server
// ──────────────────────────────────────────────────────────────────────────────

type Proxy struct {
	ring *HashRing
	pool *ConnPool
}

func NewProxy(replicas int) *Proxy {
	return &Proxy{
		ring: NewHashRing(replicas),
		pool: NewConnPool(),
	}
}

func writeLine(w *bufio.Writer, line string) error {
	_, err := w.WriteString(line + "\n")
	if err != nil {
		return err
	}
	return w.Flush()
}

func (p *Proxy) forwardToServer(addr string, cmd string) ([]string, error) {
	_, r, w, err := p.pool.Get(addr)
	if err != nil {
		p.pool.Remove(addr)
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}

	if err := writeLine(w, cmd); err != nil {
		p.pool.Remove(addr)
		return nil, fmt.Errorf("write to %s: %w", addr, err)
	}

	line, err := r.ReadString('\n')
	if err != nil {
		p.pool.Remove(addr)
		return nil, fmt.Errorf("read from %s: %w", addr, err)
	}
	line = strings.TrimSpace(line)

	responses := []string{line}

	// Handle bulk strings ($N) and arrays (*N)
	if len(line) > 0 && line[0] == '$' {
		var length int
		fmt.Sscanf(line, "$%d", &length)
		if length >= 0 {
			valueLine, _ := r.ReadString('\n')
			responses = append(responses, strings.TrimSpace(valueLine))
		}
	} else if len(line) > 0 && line[0] == '*' {
		var count int
		fmt.Sscanf(line, "*%d", &count)
		for i := 0; i < count; i++ {
			itemLine, _ := r.ReadString('\n')
			responses = append(responses, strings.TrimSpace(itemLine))
		}
	}

	return responses, nil
}

func (p *Proxy) handleConn(conn net.Conn) {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	_ = writeLine(w, "+OK proxy ready (type HELP for commands)")

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
		case "HELP":
			_ = writeLine(w, "+Commands: SET/GET/DEL/KEYS, ADD_SERVER/REMOVE_SERVER/SERVERS, ROUTE, PING, QUIT")

		case "PING":
			_ = writeLine(w, "+PONG")

		case "QUIT":
			_ = writeLine(w, "+BYE")
			return

		// ─────────────────────────────────────────────────────────────────────
		// Server management commands
		// ─────────────────────────────────────────────────────────────────────

		case "ADD_SERVER":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: ADD_SERVER host:port")
				continue
			}
			addr := parts[1]
			p.ring.Add(addr)
			log.Printf("[proxy] Added server: %s", addr)
			_ = writeLine(w, fmt.Sprintf("+OK added %s", addr))

		case "REMOVE_SERVER":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: REMOVE_SERVER host:port")
				continue
			}
			addr := parts[1]
			p.ring.Remove(addr)
			p.pool.Remove(addr)
			log.Printf("[proxy] Removed server: %s", addr)
			_ = writeLine(w, fmt.Sprintf("+OK removed %s", addr))

		case "SERVERS":
			nodes := p.ring.Nodes()
			_ = writeLine(w, fmt.Sprintf("*%d", len(nodes)))
			for _, n := range nodes {
				_ = writeLine(w, "+"+n)
			}

		case "ROUTE":
			// Show which server a key would route to
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: ROUTE key")
				continue
			}
			key := parts[1]
			if nodeAddr, ok := p.ring.Get(key); ok {
				_ = writeLine(w, "+"+nodeAddr)
			} else {
				_ = writeLine(w, "-ERR no servers available")
			}

		// ─────────────────────────────────────────────────────────────────────
		// Data commands (forwarded via consistent hashing)
		// ─────────────────────────────────────────────────────────────────────

		case "SET":
			if len(parts) < 3 {
				_ = writeLine(w, "-ERR usage: SET key value")
				continue
			}
			key := parts[1]
			nodeAddr, ok := p.ring.Get(key)
			if !ok {
				_ = writeLine(w, "-ERR no servers available")
				continue
			}
			log.Printf("[proxy] SET %s -> routing to %s", key, nodeAddr)
			responses, err := p.forwardToServer(nodeAddr, line)
			if err != nil {
				_ = writeLine(w, "-ERR "+err.Error())
				continue
			}
			for _, resp := range responses {
				_ = writeLine(w, resp)
			}

		case "GET":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: GET key")
				continue
			}
			key := parts[1]
			nodeAddr, ok := p.ring.Get(key)
			if !ok {
				_ = writeLine(w, "-ERR no servers available")
				continue
			}
			log.Printf("[proxy] GET %s -> routing to %s", key, nodeAddr)
			responses, err := p.forwardToServer(nodeAddr, line)
			if err != nil {
				_ = writeLine(w, "-ERR "+err.Error())
				continue
			}
			for _, resp := range responses {
				_ = writeLine(w, resp)
			}

		case "DEL":
			if len(parts) != 2 {
				_ = writeLine(w, "-ERR usage: DEL key")
				continue
			}
			key := parts[1]
			nodeAddr, ok := p.ring.Get(key)
			if !ok {
				_ = writeLine(w, "-ERR no servers available")
				continue
			}
			log.Printf("[proxy] DEL %s -> routing to %s", key, nodeAddr)
			responses, err := p.forwardToServer(nodeAddr, line)
			if err != nil {
				_ = writeLine(w, "-ERR "+err.Error())
				continue
			}
			for _, resp := range responses {
				_ = writeLine(w, resp)
			}

		case "KEYS":
			// Query all servers and aggregate keys
			nodes := p.ring.Nodes()
			if len(nodes) == 0 {
				_ = writeLine(w, "*0")
				continue
			}
			var allKeys []string
			for _, nodeAddr := range nodes {
				responses, err := p.forwardToServer(nodeAddr, "KEYS")
				if err != nil {
					log.Printf("[proxy] KEYS from %s: %v", nodeAddr, err)
					continue
				}
				// Skip the *N header and collect key names
				for i := 1; i < len(responses); i++ {
					if strings.HasPrefix(responses[i], "+") {
						allKeys = append(allKeys, responses[i][1:])
					}
				}
			}
			_ = writeLine(w, fmt.Sprintf("*%d", len(allKeys)))
			for _, k := range allKeys {
				_ = writeLine(w, "+"+k)
			}

		default:
			_ = writeLine(w, "-ERR unknown command (type HELP)")
		}
	}
}

func main() {
	port := flag.Int("port", 6380, "proxy port")
	replicas := flag.Int("replicas", 3, "virtual nodes per server")
	flag.Parse()

	proxy := NewProxy(*replicas)
	addr := fmt.Sprintf("127.0.0.1:%d", *port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[proxy] listen: %v", err)
	}
	log.Printf("[proxy] listening on %s (replicas=%d)", addr, *replicas)
	log.Printf("[proxy] Use 'nc %s' to connect", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[proxy] accept: %v", err)
			continue
		}
		go proxy.handleConn(conn)
	}
}
