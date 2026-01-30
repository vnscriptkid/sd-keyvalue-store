package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Op codes
const (
	opSet byte = 1
	opDel byte = 2
)

// Record format (little endian):
// [1 byte op]
// [4 bytes keyLen]
// [4 bytes valLen] (0 for DEL)
// [key bytes]
// [val bytes]
// [4 bytes crc32]  (over op|keyLen|valLen|key|val)
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	bufw *bufio.Writer
}

func OpenWAL(path string) (*WAL, error) {
	// O_APPEND ensures writes go to the end.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		f:    f,
		bufw: bufio.NewWriterSize(f, 1<<20), // 1MB buffer
	}, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bufw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// AppendSET logs a SET operation to WAL.
// If you want strict WAL semantics: append record, flush buffer, then (optionally) fsync.
func (w *WAL) AppendSET(key, val []byte) error {
	return w.appendRecord(opSet, key, val)
}

func (w *WAL) AppendDEL(key []byte) error {
	return w.appendRecord(opDel, key, nil)
}

func (w *WAL) appendRecord(op byte, key, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var header [1 + 4 + 4]byte
	header[0] = op
	binary.LittleEndian.PutUint32(header[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[5:9], uint32(len(val)))

	// Compute CRC over header+key+val
	h := crc32.NewIEEE()
	_, _ = h.Write(header[:])
	_, _ = h.Write(key)
	if len(val) > 0 {
		_, _ = h.Write(val)
	}
	sum := h.Sum32()

	// Write record
	if _, err := w.bufw.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.bufw.Write(key); err != nil {
		return err
	}
	if len(val) > 0 {
		if _, err := w.bufw.Write(val); err != nil {
			return err
		}
	}
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], sum)
	if _, err := w.bufw.Write(crcBuf[:]); err != nil {
		return err
	}

	// Ensure record reaches OS buffers (not necessarily disk yet).
	// If you want "WAL before apply" strictly visible to crash recovery,
	// you need at least Flush() here.
	return w.bufw.Flush()
}

// Sync forces an fsync to disk. This is like Redis AOF fsync policy.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bufw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Replay reads WAL from the beginning and calls apply(op,key,val) for each valid record.
// If it hits a partial/corrupt tail record, it stops (common WAL behavior).
func (w *WAL) Replay(apply func(op byte, key, val []byte)) error {
	// NOTE: for simplicity, open a separate read handle so we don't mess with append fd offset.
	rf, err := os.Open(w.f.Name())
	if err != nil {
		return err
	}
	defer rf.Close()

	br := bufio.NewReaderSize(rf, 1<<20)

	for {
		// Read fixed header
		var header [1 + 4 + 4]byte
		if _, err := io.ReadFull(br, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil // clean end or partial tail: stop replay
			}
			return err
		}
		op := header[0]
		keyLen := binary.LittleEndian.Uint32(header[1:5])
		valLen := binary.LittleEndian.Uint32(header[5:9])

		// Basic sanity limits to avoid OOM on corrupted file
		const maxKey = 1 << 20  // 1MB
		const maxVal = 64 << 20 // 64MB
		if keyLen == 0 || keyLen > maxKey || valLen > maxVal {
			return nil // treat as corruption: stop replay
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(br, key); err != nil {
			return nil // partial tail
		}
		val := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(br, val); err != nil {
				return nil
			}
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(br, crcBuf[:]); err != nil {
			return nil
		}
		wantCRC := binary.LittleEndian.Uint32(crcBuf[:])

		// Verify CRC
		h := crc32.NewIEEE()
		_, _ = h.Write(header[:])
		_, _ = h.Write(key)
		if valLen > 0 {
			_, _ = h.Write(val)
		}
		gotCRC := h.Sum32()
		if gotCRC != wantCRC {
			return nil // corruption/torn write: stop replay
		}

		apply(op, key, val)
	}
}

// ---- KV Store ----

type KV struct {
	mu  sync.RWMutex
	mem map[string][]byte
	wal *WAL

	// fsyncEvery can simulate AOF policies:
	// 0 => never fsync automatically
	// 1 => fsync every write (slow, durable)
	fsyncEvery int
}

func OpenKV(path string, fsyncEvery int) (*KV, error) {
	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}
	kv := &KV{
		mem:        make(map[string][]byte),
		wal:        wal,
		fsyncEvery: fsyncEvery,
	}
	// Recover state by replaying WAL
	if err := wal.Replay(func(op byte, key, val []byte) {
		k := string(key)
		switch op {
		case opSet:
			// Copy because val slice is reused by replay allocations anyway; still safe.
			v := make([]byte, len(val))
			copy(v, val)
			kv.mem[k] = v
		case opDel:
			delete(kv.mem, k)
		}
	}); err != nil {
		_ = wal.Close()
		return nil, err
	}
	return kv, nil
}

func (kv *KV) Close() error {
	return kv.wal.Close()
}

func (kv *KV) Get(key string) ([]byte, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.mem[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (kv *KV) Set(key string, val []byte) error {
	// 1) WAL append first (write-ahead)
	if err := kv.wal.AppendSET([]byte(key), val); err != nil {
		return err
	}
	if kv.fsyncEvery == 1 {
		if err := kv.wal.Sync(); err != nil {
			return err
		}
	}

	// 2) Apply to mem after WAL persisted to OS buffers (and maybe disk)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v := make([]byte, len(val))
	copy(v, val)
	kv.mem[key] = v
	return nil
}

func (kv *KV) Del(key string) error {
	if err := kv.wal.AppendDEL([]byte(key)); err != nil {
		return err
	}
	if kv.fsyncEvery == 1 {
		if err := kv.wal.Sync(); err != nil {
			return err
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.mem, key)
	return nil
}

// ---- Demo ----

func main() {
	const walPath = "demo.wal"

	// Open store (replays WAL)
	kv, err := OpenKV(walPath, 0) // fsyncEvery=0 (like AOF everysec-ish / no fsync by default)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	// _ = kv.Set("name", []byte("thanh"))
	// _ = kv.Set("city", []byte("singapore"))
	// _ = kv.Del("city")

	if v, ok := kv.Get("name"); ok {
		fmt.Println("name =", string(v))
	} else {
		fmt.Println("name missing")
	}

	if v, ok := kv.Get("city"); ok {
		fmt.Println("city =", string(v))
	} else {
		fmt.Println("city missing")
	}

	fmt.Println("Restart the program to see WAL replay rebuild state.")
}
