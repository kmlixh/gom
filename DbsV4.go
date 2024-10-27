package gom

import (
	"database/sql"
	"errors"
	"github.com/kmlixh/gom/v3/define"
)

type DBv4 struct {
	id      int64
	factory define.SqlFactory
	db      *sql.DB
	tx      *sql.Tx
}

func (db DBv4) GetCurrentSchema() (string, error) {
	return db.Factory().GetCurrentSchema(db.db)
}

func (db DBv4) GetColumns(table string) ([]define.Column, error) {
	return db.Factory().GetColumns(table, db.db)
}

func (db DBv4) GetTables() ([]string, error) {
	return db.Factory().GetTables(db.db)
}

func (db DBv4) GetTableStruct(table string) (define.ITableStruct, error) {
	return db.Factory().GetTableStruct(table, db.db)
}
func (db DBv4) GetTableStruct2(i any) (define.ITableStruct, error) {
	rawInfo := GetRawTableInfo(i)
	if rawInfo.TableName == "" {
		panic("table name was nil")
	}
	return db.GetTableStruct(rawInfo.TableName)
}

type TransactionWorkV4 func(databaseTx *DBv4) (interface{}, error)

func (db DBv4) GetRawDb() *sql.DB {
	return db.db
}
func (db DBv4) Factory() define.SqlFactory {
	return db.factory
}

func (db *DBv4) cloneSelfIfDifferentGoRoutine() {
	if db.id != getGrouteId() {
		*db = db.Clone()
	}
}
func (db DBv4) Clone() DBv4 {
	return DBv4{id: getGrouteId(), factory: db.factory, db: db.db}
}

func (db *DBv4) ExecuteStatement(statement string, data ...any) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	st, err := db.prepare(statement)
	if err != nil {
		return nil, err
	}
	rs, er := st.Exec(data...)
	if er != nil && db.IsInTransaction() {
		//如果是在事务中，则直接panic整个事务，从而使事务可以回滚尽早回滚事务，避免发生错误的Commit
		db.Rollback()
	}
	defer func() {
		panics := recover()
		if panics != nil && db.IsInTransaction() {
			db.Rollback()
		}
	}()
	return rs, er
}

func (db *DBv4) prepare(query string) (*sql.Stmt, error) {
	if db.IsInTransaction() {
		st, er := db.tx.Prepare(query)
		if er != nil {
			db.Rollback()
		}
		return st, er
	}
	return db.db.Prepare(query)
}

func (db *DBv4) Begin() error {
	if db.tx != nil {
		return errors.New(" there was a DoTransaction")
	}
	tx, err := db.db.Begin()
	db.tx = tx
	return err
}
func (db *DBv4) IsInTransaction() bool {
	return db.tx != nil
}
func (db *DBv4) Commit() {
	if db.IsInTransaction() {
		err := db.tx.Commit()
		if err != nil {
			panic(err)
		}
		db.tx = nil
	}
}
func (db *DBv4) Rollback() {
	if db.tx != nil {
		err := db.tx.Rollback()
		if err != nil {
			panic(err)
		}
		db.tx = nil
	}
}
