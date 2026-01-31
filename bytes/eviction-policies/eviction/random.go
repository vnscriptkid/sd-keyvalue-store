package eviction

import (
	"math/rand"
	"time"

	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/lib"
)

type RandomEvictor struct {
	rnd  *rand.Rand
	keys []string
	idx  map[string]int        // key -> index in keys
	ptr  map[string]*lib.Entry // key -> *lib.Entry (so Victim can return pointer)
}

func NewRandomEvictor() *RandomEvictor {
	return &RandomEvictor{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
		idx: make(map[string]int),
		ptr: make(map[string]*lib.Entry),
	}
}

func (e *RandomEvictor) Name() string { return "RANDOM" }

func (e *RandomEvictor) OnAdd(en *lib.Entry) {
	if _, ok := e.idx[en.Key]; ok {
		// already exists; treat as update
		e.ptr[en.Key] = en
		return
	}
	e.idx[en.Key] = len(e.keys)
	e.keys = append(e.keys, en.Key)
	e.ptr[en.Key] = en
}

func (e *RandomEvictor) OnGet(en *lib.Entry)    { e.ptr[en.Key] = en }
func (e *RandomEvictor) OnUpdate(en *lib.Entry) { e.ptr[en.Key] = en }

func (e *RandomEvictor) OnRemove(en *lib.Entry) {
	i, ok := e.idx[en.Key]
	if !ok {
		return
	}
	last := len(e.keys) - 1
	lastKey := e.keys[last]

	// swap-delete
	e.keys[i] = lastKey
	e.idx[lastKey] = i
	e.keys = e.keys[:last]

	delete(e.idx, en.Key)
	delete(e.ptr, en.Key)
}

func (e *RandomEvictor) Victim() *lib.Entry {
	if len(e.keys) == 0 {
		return nil
	}
	i := e.rnd.Intn(len(e.keys))
	k := e.keys[i]
	return e.ptr[k] // can be nil if desynced; store should handle nil defensively
}
