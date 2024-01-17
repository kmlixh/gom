package factory

import (
	"github.com/kmlixh/gom/v3/define"
	"sync"
)

var (
	mux       sync.RWMutex
	factories = make(map[string]define.SqlFactory)
)

func Register(name string, inter define.SqlFactory) {
	mux.Lock()
	defer mux.Unlock()
	if inter == nil {
		panic("PreparedSql: Register driver is nil")
	}
	if _, dup := factories[name]; dup {
		return
	}
	factories[name] = inter
}
func Get(name string) (define.SqlFactory, bool) {
	data, ok := factories[name]
	return data, ok
}
