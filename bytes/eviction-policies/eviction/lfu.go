package eviction

import (
	"container/list"

	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/lib"
)

type lfuItem struct {
	en   *lib.Entry
	freq uint64
	node *list.Element // node inside buckets[freq], Value=*lfuItem
}

type LFUEvictor struct {
	buckets map[uint64]*list.List // freq -> list of *lfuItem (front=MRU)
	items   map[string]*lfuItem   // key -> item
	minFreq uint64
}

func NewLFUEvictor() *LFUEvictor {
	return &LFUEvictor{
		buckets: make(map[uint64]*list.List),
		items:   make(map[string]*lfuItem),
		minFreq: 0,
	}
}

func (e *LFUEvictor) Name() string { return "LFU" }

func (e *LFUEvictor) bucket(freq uint64) *list.List {
	l, ok := e.buckets[freq]
	if !ok {
		l = list.New()
		e.buckets[freq] = l
	}
	return l
}

func (e *LFUEvictor) OnAdd(en *lib.Entry) {
	if it, ok := e.items[en.Key]; ok {
		// defensive: treat as update
		it.en = en
		e.OnUpdate(en)
		return
	}

	it := &lfuItem{en: en, freq: 1}
	l := e.bucket(1)
	it.node = l.PushFront(it)
	e.items[en.Key] = it

	if e.minFreq == 0 || e.minFreq > 1 {
		e.minFreq = 1
	}
}

func (e *LFUEvictor) OnGet(en *lib.Entry)    { e.bump(en) }
func (e *LFUEvictor) OnUpdate(en *lib.Entry) { e.bump(en) }

func (e *LFUEvictor) bump(en *lib.Entry) {
	it, ok := e.items[en.Key]
	if !ok {
		// if desynced, re-add
		e.OnAdd(en)
		return
	}
	it.en = en // keep pointer fresh

	oldFreq := it.freq
	oldBucket := e.buckets[oldFreq]
	if oldBucket != nil && it.node != nil {
		oldBucket.Remove(it.node)
	}

	// if old bucket becomes empty and it was minFreq, increase minFreq
	if oldBucket != nil && oldBucket.Len() == 0 {
		delete(e.buckets, oldFreq)
		if e.minFreq == oldFreq {
			e.minFreq = oldFreq + 1
		}
	}

	// move to new freq bucket, MRU within that freq
	it.freq++
	newBucket := e.bucket(it.freq)
	it.node = newBucket.PushFront(it)
}

func (e *LFUEvictor) OnRemove(en *lib.Entry) {
	it, ok := e.items[en.Key]
	if !ok {
		return
	}
	b := e.buckets[it.freq]
	if b != nil && it.node != nil {
		b.Remove(it.node)
		if b.Len() == 0 {
			delete(e.buckets, it.freq)
			// If we removed the last element of minFreq bucket, we must advance minFreq.
			// We can advance by searching upward until we find an existing bucket.
			if e.minFreq == it.freq {
				e.advanceMinFreq()
			}
		}
	}
	delete(e.items, en.Key)
	if len(e.items) == 0 {
		e.minFreq = 0
	}
}

func (e *LFUEvictor) advanceMinFreq() {
	if len(e.items) == 0 {
		e.minFreq = 0
		return
	}
	// advance minFreq until a bucket exists
	for f := e.minFreq; ; f++ {
		if b, ok := e.buckets[f]; ok && b.Len() > 0 {
			e.minFreq = f
			return
		}
	}
}

func (e *LFUEvictor) Victim() *lib.Entry {
	if e.minFreq == 0 {
		return nil
	}
	b := e.buckets[e.minFreq]
	if b == nil || b.Len() == 0 {
		e.advanceMinFreq()
		b = e.buckets[e.minFreq]
		if b == nil || b.Len() == 0 {
			return nil
		}
	}
	// within minFreq bucket, evict LRU among them => back
	back := b.Back()
	if back == nil {
		return nil
	}
	return back.Value.(*lfuItem).en
}
