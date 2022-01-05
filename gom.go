package gom

import (
	"database/sql"
	"sync"
	"time"
)

var (
	factorysMux sync.RWMutex
	factorys    = make(map[string]SqlFactory)
)
var debug bool

const defaultDBId = -1000

func Register(name string, factory SqlFactory) {
	factorysMux.Lock()
	defer factorysMux.Unlock()
	if factory == nil {
		panic("Sql: Register driver is nil")
	}
	if _, dup := factorys[name]; dup {
		panic("Sql: Register called twice for factory " + name)
	}
	factorys[name] = factory
}

func OpenWithConfig(driverName string, dsn string, maxOpen int, maxIdle int, debugs bool) (*DB, error) {
	debug = debugs
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		return &DB{id: defaultDBId, db: db, factory: factorys[driverName]}, nil
	}
}

func Open(driverName string, dsn string, debugs bool) (*DB, error) {
	debug = debugs
	db, err := sql.Open(driverName, dsn)
	db.SetConnMaxLifetime(time.Minute * 1)
	if err != nil {
		return nil, err
	} else {
		return &DB{id: defaultDBId, db: db, factory: factorys[driverName]}, nil
	}
}
