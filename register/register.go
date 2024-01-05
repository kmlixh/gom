package register

import (
	"github.com/kmlixh/gom/v2/defines"
	"sync"
)

var (
	mux       sync.RWMutex
	factories = make(map[string]defines.SqlFactory)
)

func Register(name string, inter defines.SqlFactory) {
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
func Get(name string) (defines.SqlFactory, bool) {
	data, ok := factories[name]
	return data, ok
}
