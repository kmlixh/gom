package register

import (
	"gom/structs"
	"sync"
)

var (
	factorysMux sync.RWMutex
	factorys    = make(map[string]structs.SqlFactory)
)

func Register(name string, inter structs.SqlFactory) {
	factorysMux.Lock()
	defer factorysMux.Unlock()
	if inter == nil {
		panic("Sql: Register driver is nil")
	}
	if _, dup := factorys[name]; dup {
		panic("Sql: Register called twice for factory " + name)
	}
	factorys[name] = inter
}
func Get(name string) (structs.SqlFactory, bool) {
	data, ok := factorys[name]
	return data, ok
}
