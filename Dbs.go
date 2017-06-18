package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type TransactionWork func(databaseTx *DataBase) (int, error)

type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}
type DataBase struct {
	factory  SqlFactory
	rawDb    *sql.DB
	executor sqlExecutor
}

func (db DataBase) RawDb() *sql.DB {
	return db.rawDb
}
func (Db DataBase) makeInsertSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{Db.factory.Insert, tableModel}
}
func (Db DataBase) makeUpdateSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{Db.factory.Update, tableModel}
}
func (Db DataBase) makeDeleteSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{Db.factory.Delete, tableModel}
}
func (Db DataBase) QueryByTableModel(model TableModel, vs interface{}, c Condition) (interface{}, error) {
	tps, isPtr, islice := getType(vs)
	if debug {
		fmt.Println("model:", model)
	}
	if len(model.TableName) > 0 {
		if c.State() != "" {
			model.Cnd = c
		}
		if islice {
			results := reflect.Indirect(reflect.ValueOf(vs))
			sqls, datas := Db.factory.Query(model)
			if debug {
				fmt.Println(sqls, datas)
			}
			st, err := Db.executor.Prepare(sqls)
			if err != nil {
				return nil, err
			}
			rows, err := st.Query(datas...)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				val := getValueOfTableRow(model, rows)
				if isPtr {
					results.Set(reflect.Append(results, val.Elem()))
				} else {
					results.Set(reflect.Append(results, val))
				}
			}
			return vs, nil

		} else {
			sqls, datas := Db.factory.Query(model)
			if debug {
				fmt.Println(sqls, datas)
			}
			st, err := Db.executor.Prepare(sqls)
			if err != nil {
				return nil, err
			}
			row := st.QueryRow(datas...)
			if debug {
				fmt.Println("row is", row)
			}
			val := getValueOfTableRow(model, row)
			var vt reflect.Value
			if isPtr {
				vt = reflect.ValueOf(vs).Elem()
			} else {
				vt = reflect.New(tps).Elem()

			}
			vt.Set(val.Elem())
			return vt.Interface(), nil
		}

	} else {
		return nil, errors.New("can't create a TableModel")
	}
}

func (db DataBase) Query(vs interface{}, c Condition) (interface{}, error) {
	model := getTableModel(vs)
	return db.QueryByTableModel(model, vs, c)

}

func (db DataBase) WorkInTransaction(work TransactionWork) (int, error) {
	result := 0
	tx, err := db.rawDb.Begin()
	if err != nil {
		return result, err
	}

	result, err = work(&DataBase{rawDb: db.rawDb, factory: db.factory, executor: tx})
	if err != nil {
		tx.Rollback()
		return result, err
	}
	tx.Commit()
	return result, nil
}
func (db DataBase) execute(job SqlGenerator) (int, error) {
	result := 0
	for _, table := range job.tableModels {
		sql, datas := job.createSql(table)
		st, ers := db.executor.Prepare(sql)
		if ers != nil {
			return -1, ers
		}
		rt, ers := st.Exec(datas)
		if ers != nil {
			return -1, ers
		}
		count, ers := rt.RowsAffected()
		result += int(count)
		if ers != nil {
			return result, ers
		}
	}
	return result, nil
}
func (db DataBase) Insert(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Insert, models})
}

func (db DataBase) Replace(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Replace, models})
}

func (db DataBase) Delete(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Delete, tables})
}

func (db DataBase) DeleteByConditon(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	if c.State() != "" {
		tableModel.Cnd = c
	}
	return db.execute(SqlGenerator{db.factory.Delete, []TableModel{tableModel}})
}

func (db DataBase) Update(vs ...interface{}) (int, error) {
	tms := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Update, tms})
}

func (db DataBase) UpdateByCondition(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.execute(SqlGenerator{db.factory.Update, []TableModel{tableModel}})
}
