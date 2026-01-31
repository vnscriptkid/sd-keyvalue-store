# Consistent Hashing in Action - Distributed Cache Demo

This demo shows consistent hashing for distributing keys across multiple cache servers. It demonstrates what happens when servers are added or removed from the cluster.

## Architecture

```
                    ┌─────────────────┐
                    │     Client      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Proxy      │
                    │ (consistent     │
                    │  hashing ring)  │
                    └────────┬────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
    ┌─────────┐        ┌─────────┐        ┌─────────┐
    │ cache-A │        │ cache-B │        │ cache-C │
    │  :6381  │        │  :6382  │        │  :6383  │
    └─────────┘        └─────────┘        └─────────┘
```

## Quick Start

### 1. Start Cache Servers (3 terminals)

```bash
# Terminal 1
cd bytes/consistent-hashing-in-action
go run ./server -port 6381 -name cache-A

# Terminal 2
go run ./server -port 6382 -name cache-B

# Terminal 3
go run ./server -port 6383 -name cache-C
```

### 2. Start the Proxy (1 terminal)

```bash
# Terminal 4
go run ./proxy -port 6380
```

### 3. Run the Automated Demo

```bash
# Terminal 5
go run ./demo
```

This will show:
- How keys route to servers initially
- What happens when a new server is added (some keys miss!)
- What happens when a server is removed

## Interactive Demo

Connect directly to the proxy with netcat:

```bash
nc 127.0.0.1 6380
```

### Proxy Commands

| Command | Description |
|---------|-------------|
| `ADD_SERVER host:port` | Add a cache server to the ring |
| `REMOVE_SERVER host:port` | Remove a server from the ring |
| `SERVERS` | List all active servers |
| `ROUTE key` | Show which server handles a key |
| `SET key value` | Store a value (routed by consistent hash) |
| `GET key` | Retrieve a value |
| `DEL key` | Delete a key |
| `KEYS` | List all keys across all servers |
| `PING` | Health check |
| `HELP` | Show available commands |

### Example Session

```
$ nc 127.0.0.1 6380
+OK proxy ready (type HELP for commands)

ADD_SERVER 127.0.0.1:6381
+OK added 127.0.0.1:6381

ADD_SERVER 127.0.0.1:6382
+OK added 127.0.0.1:6382

SERVERS
*2
+127.0.0.1:6381
+127.0.0.1:6382

ROUTE user:1001
+127.0.0.1:6381

SET user:1001 Alice
+OK

GET user:1001
$5
Alice

# Now add a third server and see routing change!
ADD_SERVER 127.0.0.1:6383
+OK added 127.0.0.1:6383

ROUTE user:1001
+127.0.0.1:6383    # Key may now route to new server!

GET user:1001
$-1                 # Cache miss! Data is on the old server
```

## What This Demonstrates

### 1. Key Distribution
Keys are distributed across servers based on their hash value's position on the ring. Each key consistently maps to the same server.

### 2. Adding Servers
When a new server is added:
- Only ~1/N of keys get remapped (N = new total servers)
- Keys that move to the new server will experience cache misses
- Much better than modulo hashing where ALL keys would remap!

### 3. Removing Servers  
When a server is removed:
- Only keys on that server need to find new homes
- They get rehashed to the next server on the ring
- Other keys are unaffected

### 4. Virtual Nodes (Replicas)
The proxy uses virtual nodes (default: 3 per server) to:
- Distribute keys more evenly
- Reduce hotspots
- Make the hash space more balanced

## Configuration

### Proxy Options
```bash
go run ./proxy -port 6380 -replicas 5
```
- `-port`: Proxy listen port (default: 6380)
- `-replicas`: Virtual nodes per server (default: 3)

### Server Options
```bash
go run ./server -port 6381 -name my-cache
```
- `-port`: Server listen port (default: 6381)  
- `-name`: Server display name (default: cache-<port>)

## Real-World Considerations

In production, you would also need:

1. **Data Migration**: When adding/removing servers, migrate affected keys
2. **Replication**: Store copies on multiple servers for fault tolerance
3. **Health Checks**: Automatically detect and remove failed servers
4. **Connection Pooling**: Efficient connection reuse (basic version included)
5. **More Virtual Nodes**: Production systems often use 100-200 virtual nodes per server
