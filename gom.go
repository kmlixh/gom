package gom

import (
	"sync"
	"database/sql"
)

var (
	factorysMux sync.RWMutex
	factorys = make(map[string]SqlFactory)
)

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

func OpenWithConfig(driverName string, dsn string, maxOpen int, maxIdle int) (*DB, error) {
	db,err:=sql.Open(driverName,dsn)
	if(err!=nil){
		return nil,err
	}else{
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		return &DB{factorys[driverName],db},nil
	}
}


func Open(driverName string, dsn string) (*DB, error) {
	db,err:=sql.Open(driverName,dsn)
	if(err!=nil){
		return nil,err
	}else{
		return &DB{factorys[driverName],db},nil
	}
}