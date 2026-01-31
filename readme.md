# How to build key value store like Redis

## Requirements

## System evolution

1. Version 1: key-value store is simply a hash map in memory
- Design the interface
- Low level design approaches for `Entry`
    - Approach 1: Keeping type variable (string, set, list)
    - Approach 2: ...

2. Version 2: Communication
- Approaches:
    - raw TCP
    - HTTP
    - gRPC
- Demo with netcat

3. Version 3: Support TTL
- Why TTL?
- Approaches:
    - Random sampling
    - Priority queue (heap)

4. Version 4: Handle concurrency
- Why concurrency needs to be handled?
- Approaches:
    - Optimistic concurrency control (CAS)
    - Pessimistic concurrency control (mutex)
- Optimization: sharded hashmap

5. Version 5: Support eviction policies
- Why eviction?
- Eviction vs TTL?
- Approaches:
    - LRU
    - LFU
    - Random
- Low-level design: make it extensible

6. Version 6: Improve reliability
- Support WAL

7. Version 7: Make it distributed
- Why distributed?
- Approaches:
    - Modulo hashing
    - Range based
    - Consistent hashing
- Demo with distributed system