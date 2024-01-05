package gom

import (
	"sync"
)

var (
	mux       sync.RWMutex
	factories = make(map[string]SqlFactory)
)

func Register(name string, inter SqlFactory) {
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
func Get(name string) (SqlFactory, bool) {
	data, ok := factories[name]
	return data, ok
}
