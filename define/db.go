package define

import (
	"database/sql"
	"sync/atomic"
)

var routineIDCounter int64

// New creates a new DB instance
func NewDB(db *sql.DB, factory SQLFactory) *DB {
	return &DB{
		DB:      db,
		Factory: factory,
	}
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

// StartQuery starts a new query chain
func (db *DB) StartQuery(table string) *QueryChain {
	db = db.cloneSelfIfDifferentGoRoutine()
	return &QueryChain{
		DB:        db,
		TableName: table,
		Factory:   db.Factory,
	}
}

// Begin starts a new transaction
func (db *DB) Begin() error {
	db = db.cloneSelfIfDifferentGoRoutine()
	tx, err := db.DB.Begin()
	if err != nil {
		return err
	}
	db.Tx = tx
	return nil
}

// Commit commits the current transaction
func (db *DB) Commit() error {
	if db.Tx == nil {
		return nil
	}
	err := db.Tx.Commit()
	db.Tx = nil
	return err
}

// Rollback rolls back the current transaction
func (db *DB) Rollback() error {
	if db.Tx == nil {
		return nil
	}
	err := db.Tx.Rollback()
	db.Tx = nil
	return err
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.DB.Close()
}

// Execute executes a query and returns the result
func (db *DB) Execute(query string, args ...interface{}) (sql.Result, error) {
	if db.Tx != nil {
		return db.Tx.Exec(query, args...)
	}
	return db.DB.Exec(query, args...)
}

// ExecuteQuery executes a query and returns the rows
func (db *DB) ExecuteQuery(query string, args ...interface{}) (*sql.Rows, error) {
	if db.Tx != nil {
		return db.Tx.Query(query, args...)
	}
	return db.DB.Query(query, args...)
}
