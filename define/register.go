package define

import (
	"sync"
)

var (
	mux       sync.RWMutex
	factories = make(map[string]SqlFactory)
)

func RegisterFactory(name string, inter SqlFactory) {
	mux.Lock()
	defer mux.Unlock()
	if inter == nil {
		panic("PreparedSql: RegisterFactory driver is nil")
	}
	if _, dup := factories[name]; dup {
		return
	}
	factories[name] = inter
}
func GetFactory(name string) (SqlFactory, bool) {
	data, ok := factories[name]
	return data, ok
}
