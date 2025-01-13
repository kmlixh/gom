package gom

import (
	"database/sql"

	"github.com/kmlixh/gom/v4/define"
)

type DB struct {
	factory define.SqlFactory
	db      *sql.DB
}

func (db DB) GetCurrentSchema() (string, error) {
	return db.Factory().GetCurrentSchema(db.db)
}

func (db DB) GetColumns(table string) ([]define.Column, error) {
	return db.Factory().GetColumns(table, db.db)
}

func (db DB) GetTables() ([]string, error) {
	return db.Factory().GetTables(db.db)
}

func (db DB) GetTableStruct(table string) (define.ITableStruct, error) {
	return db.Factory().GetTableStruct(table, db.db)
}
func (db DB) GetTableStruct2(i any) (define.ITableStruct, error) {
	rawInfo := GetRawTableInfo(i)
	if rawInfo.TableName == "" {
		panic("table name was nil")
	}
	return db.GetTableStruct(rawInfo.TableName)
}

func (db DB) GetRawDb() *sql.DB {
	return db.db
}
func (db DB) Factory() define.SqlFactory {
	return db.factory
}
func (db DB) Chain() *Chain {
	return &Chain{factory: db.factory, db: db.db}
}
