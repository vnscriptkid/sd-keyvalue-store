package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

const proxyAddr = "127.0.0.1:6380"

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     CONSISTENT HASHING IN ACTION - DISTRIBUTED CACHE DEMO      ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("This demo shows how consistent hashing affects key routing when")
	fmt.Println("cache servers are added or removed from the cluster.")
	fmt.Println()

	// Connect to proxy
	conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		fmt.Printf("❌ Cannot connect to proxy at %s\n", proxyAddr)
		fmt.Println("   Make sure to start the proxy and servers first!")
		fmt.Println()
		fmt.Println("   Terminal 1: go run ./server -port 6381 -name cache-A")
		fmt.Println("   Terminal 2: go run ./server -port 6382 -name cache-B")
		fmt.Println("   Terminal 3: go run ./server -port 6383 -name cache-C")
		fmt.Println("   Terminal 4: go run ./proxy -port 6380")
		fmt.Println("   Terminal 5: go run ./demo")
		return
	}
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	// Read greeting
	r.ReadString('\n')

	send := func(cmd string) string {
		w.WriteString(cmd + "\n")
		w.Flush()
		line, _ := r.ReadString('\n')
		line = strings.TrimSpace(line)

		// Handle bulk strings
		if strings.HasPrefix(line, "$") && !strings.HasPrefix(line, "$-1") {
			value, _ := r.ReadString('\n')
			return strings.TrimSpace(value)
		}
		// Handle arrays
		if strings.HasPrefix(line, "*") {
			var count int
			fmt.Sscanf(line, "*%d", &count)
			var items []string
			for i := 0; i < count; i++ {
				item, _ := r.ReadString('\n')
				item = strings.TrimSpace(item)
				if strings.HasPrefix(item, "+") {
					items = append(items, item[1:])
				}
			}
			return strings.Join(items, ", ")
		}
		if strings.HasPrefix(line, "+") {
			return line[1:]
		}
		return line
	}

	testKeys := []string{
		"user:1001",
		"user:2002",
		"session:abc",
		"session:xyz",
		"product:laptop",
		"product:phone",
		"order:99",
		"cart:alice",
	}

	showRouting := func(title string) {
		fmt.Println("─────────────────────────────────────────────────────────────────")
		fmt.Println(title)
		fmt.Println("─────────────────────────────────────────────────────────────────")
		servers := send("SERVERS")
		fmt.Printf("Active servers: [%s]\n\n", servers)
		fmt.Printf("%-20s → %s\n", "KEY", "ROUTED TO")
		for _, key := range testKeys {
			server := send("ROUTE " + key)
			fmt.Printf("%-20s → %s\n", key, server)
		}
		fmt.Println()
	}

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 1: Add initial servers
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("📌 STEP 1: Adding initial servers (cache-A and cache-B)")
	send("ADD_SERVER 127.0.0.1:6381") // cache-A
	send("ADD_SERVER 127.0.0.1:6382") // cache-B

	showRouting("Initial routing with 2 servers")

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 2: Store some data
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println("📌 STEP 2: Storing data through the proxy")
	for _, key := range testKeys {
		value := fmt.Sprintf("value-for-%s", key)
		send(fmt.Sprintf("SET %s %s", key, value))
		server := send("ROUTE " + key)
		fmt.Printf("   SET %-20s → stored on %s\n", key, server)
	}
	fmt.Println()

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 3: Add a third server
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println("📌 STEP 3: Adding a new server (cache-C)")
	fmt.Println("   Watch how some keys now route to the new server!")
	send("ADD_SERVER 127.0.0.1:6383") // cache-C

	showRouting("Routing after adding cache-C (127.0.0.1:6383)")

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 4: Show the impact - some keys now miss!
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println("📌 STEP 4: Checking for cache misses after adding server")
	fmt.Println("   Keys that moved to cache-C won't find their data!")
	fmt.Println()
	hits, misses := 0, 0
	for _, key := range testKeys {
		result := send("GET " + key)
		server := send("ROUTE " + key)
		if result == "-1" || result == "" {
			fmt.Printf("   ❌ MISS: %-20s on %s (data was on another server)\n", key, server)
			misses++
		} else {
			fmt.Printf("   ✅ HIT:  %-20s on %s\n", key, server)
			hits++
		}
	}
	fmt.Printf("\n   Summary: %d hits, %d misses\n", hits, misses)
	fmt.Println()

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 5: Remove cache-B
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println("📌 STEP 5: Removing a server (cache-B)")
	fmt.Println("   Some keys that were on cache-B will now route elsewhere!")
	send("REMOVE_SERVER 127.0.0.1:6382")

	showRouting("Routing after removing cache-B (127.0.0.1:6382)")

	// ─────────────────────────────────────────────────────────────────────────
	// STEP 6: Re-store and verify
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println("📌 STEP 6: Re-storing all data with current topology")
	for _, key := range testKeys {
		value := fmt.Sprintf("value-for-%s", key)
		send(fmt.Sprintf("SET %s %s", key, value))
	}
	fmt.Println("   All keys re-stored.")
	fmt.Println()

	fmt.Println("📌 STEP 7: Verifying all keys are now accessible")
	hits, misses = 0, 0
	for _, key := range testKeys {
		result := send("GET " + key)
		server := send("ROUTE " + key)
		if result == "-1" || result == "" {
			fmt.Printf("   ❌ MISS: %-20s on %s\n", key, server)
			misses++
		} else {
			fmt.Printf("   ✅ HIT:  %-20s on %s = %s\n", key, server, result)
			hits++
		}
	}
	fmt.Printf("\n   Summary: %d hits, %d misses\n", hits, misses)
	fmt.Println()

	// ─────────────────────────────────────────────────────────────────────────
	// Summary
	// ─────────────────────────────────────────────────────────────────────────
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                           SUMMARY                              ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Key observations about consistent hashing:")
	fmt.Println()
	fmt.Println("  1. When adding a server, only ~1/N of keys get remapped")
	fmt.Println("     (where N is the new total number of servers)")
	fmt.Println()
	fmt.Println("  2. When removing a server, only keys on that server need")
	fmt.Println("     to be rehashed to other servers")
	fmt.Println()
	fmt.Println("  3. This is much better than simple modulo hashing where")
	fmt.Println("     changing server count would remap almost ALL keys!")
	fmt.Println()
	fmt.Println("  4. Virtual nodes (replicas) help distribute keys more evenly")
	fmt.Println("     across servers")
	fmt.Println()
	fmt.Println("Try the interactive demo with: nc 127.0.0.1 6380")
	fmt.Println()
}
