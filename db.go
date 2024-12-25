package gom

import (
	"database/sql"
	"fmt"
	"sync/atomic"

	"github.com/kmlixh/gom/v4/define"
)

var routineIDCounter int64

// DB represents the database connection
type DB struct {
	DB        *sql.DB
	Factory   define.SQLFactory
	RoutineID int64
}

// cloneSelfIfDifferentGoRoutine ensures thread safety by cloning DB instance if needed
func (db *DB) cloneSelfIfDifferentGoRoutine() *DB {
	currentID := atomic.AddInt64(&routineIDCounter, 1)
	if db.RoutineID == 0 {
		atomic.StoreInt64(&db.RoutineID, currentID)
		return db
	}
	if db.RoutineID != currentID {
		newDB := &DB{
			DB:        db.DB,
			Factory:   db.Factory,
			RoutineID: currentID,
		}
		return newDB
	}
	return db
}

// Chain starts a new chain
func (db *DB) Chain() *Chain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &Chain{
		db:      db,
		factory: db.Factory,
	}
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}

// Open creates a new DB connection with debug option
func Open(driverName, dsn string, debug bool) (*DB, error) {
	factory, er := define.GetFactory(driverName)
	if er != nil {
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
