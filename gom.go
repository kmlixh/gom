package gom

import (
	"database/sql"
	"fmt"
	"sync"
)

var (
	factorysMux sync.RWMutex
	factorys    = make(map[string]SqlFactory)
)
var debug bool

func Register(name string, factory SqlFactory) {
	factorysMux.Lock()
	defer factorysMux.Unlock()
	if factory == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := factorys[name]; dup {
		panic("sql: Register called twice for factory " + name)
	}
	factorys[name] = factory
}

func OpenWithConfig(driverName string, dsn string, maxOpen int, maxIdle int, debugs bool) (*Db, error) {
	debug = debugs
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		return &Db{rawDb: db, executor: db, factory: factorys[driverName]}, nil
	}
}

func Open(driverName string, dsn string, debugs bool) (*Db, error) {
	debug = debugs
	db, err := sql.Open(driverName, dsn)
	db.SetMaxIdleConns(0)
	if err != nil {
		return nil, err
	} else {
		return &Db{rawDb: db, executor: db, factory: factorys[driverName]}, nil
	}
}
func debugs(vs ...interface{}) {
	if debug {
		fmt.Println(vs)
	}
}
