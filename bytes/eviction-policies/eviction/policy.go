package eviction

import "github.com/vnscriptkid/sd-keyvalue-store/bytes/eviction-policies/lib"

type Evictor interface {
	Name() string
	OnAdd(e *lib.Entry)
	OnGet(e *lib.Entry)
	OnUpdate(e *lib.Entry)
	OnRemove(e *lib.Entry)
	Victim() *lib.Entry
}
