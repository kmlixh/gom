package register

import (
	"github.com/kmlixh/gom/v2/structs"
	"sync"
)

var (
	mux       sync.RWMutex
	factories = make(map[string]structs.SqlFactory)
)

func Register(name string, inter structs.SqlFactory) {
	mux.Lock()
	defer mux.Unlock()
	if inter == nil {
		panic("PreparedSql: Register driver is nil")
	}
	if _, dup := factories[name]; dup {
		panic("PreparedSql: Register called twice for factory " + name)
	}
	factories[name] = inter
}
func Get(name string) (structs.SqlFactory, bool) {
	data, ok := factories[name]
	return data, ok
}
