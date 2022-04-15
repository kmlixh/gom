package register

import (
	"github.com/kmlixh/gom/v2/structs"
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
		panic("PreparedSql: Register driver is nil")
	}
	if _, dup := factorys[name]; dup {
		panic("PreparedSql: Register called twice for factory " + name)
	}
	factorys[name] = inter
}
func Get(name string) (structs.SqlFactory, bool) {
	data, ok := factorys[name]
	return data, ok
}
