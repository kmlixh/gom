package gom

import (
	"fmt"
	"github.com/kmlixh/gom/v4/define"
)

// Open creates a new DB connection with debug option
func Open(driverName, dsn string, debug bool) (*DB, error) {
	factory, ok := define.GetFactory(driverName)
	if !ok {
		return nil, fmt.Errorf("no SQL factory registered for driver: %s", driverName)
	}
	db, err := factory.Connect(dsn)
	if err != nil {
		return nil, err
	}
	define.Debug = debug
	return &DB{
		DB:      db,
		Factory: factory,
	}, nil
}

// MustOpen creates a new DB connection and panics on error
func MustOpen(driverName, dsn string) *DB {
	db, err := Open(driverName, dsn, false)
	if err != nil {
		panic(err)
	}
	return db
}
