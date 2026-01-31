package eviction

import (
	"container/list"

	"github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/lib"
)

type LRUEvictor struct {
	ll    *list.List               // front=MRU, back=LRU
	nodes map[string]*list.Element // key -> *list.Element (Value=*lib.Entry)
}

func NewLRUEvictor() *LRUEvictor {
	return &LRUEvictor{
		ll:    list.New(),
		nodes: make(map[string]*list.Element),
	}
}

func (e *LRUEvictor) Name() string { return "LRU" }

func (e *LRUEvictor) OnAdd(en *lib.Entry) {
	if node, ok := e.nodes[en.Key]; ok {
		// already exists (defensive): treat as update
		node.Value = en
		e.ll.MoveToFront(node)
		return
	}
	node := e.ll.PushFront(en)
	e.nodes[en.Key] = node
}

func (e *LRUEvictor) OnGet(en *lib.Entry)    { e.touch(en.Key, en) }
func (e *LRUEvictor) OnUpdate(en *lib.Entry) { e.touch(en.Key, en) }

func (e *LRUEvictor) touch(key string, en *lib.Entry) {
	if node, ok := e.nodes[key]; ok {
		node.Value = en // keep pointer fresh
		e.ll.MoveToFront(node)
		return
	}
	// if store calls OnGet for a missing internal node (bug/desync), re-add
	e.OnAdd(en)
}

func (e *LRUEvictor) OnRemove(en *lib.Entry) {
	node, ok := e.nodes[en.Key]
	if !ok {
		return
	}
	e.ll.Remove(node)
	delete(e.nodes, en.Key)
}

func (e *LRUEvictor) Victim() *lib.Entry {
	back := e.ll.Back()
	if back == nil {
		return nil
	}
	return back.Value.(*lib.Entry)
}
