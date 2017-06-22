package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type TransactionWork func(databaseTx *Db) (int, error)

type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}
type Db struct {
	factory  SqlFactory
	rawDb    *sql.DB
	executor sqlExecutor
}

func (db Db) RawDb() *sql.DB {
	return db.rawDb
}
func (Db Db) makeInsertSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{Db.factory.Insert, tableModel}
}
func (Db Db) makeUpdateSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{Db.factory.Update, tableModel}
}
func (Db Db) makeDeleteSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{Db.factory.Delete, tableModel}
}
func (Db Db) QueryByTableModel(model TableModel, vs interface{}, c Condition) (interface{}, error) {
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
				results.Set(reflect.Append(results, val))
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
			vt.Set(val)
			return vt.Interface(), nil
		}

	} else {
		return nil, errors.New("can't create a TableModel")
	}
}

func (db Db) Query(vs interface{}, c Condition) (interface{}, error) {
	models, err := getTableModel(vs)
	if err != nil {
		return nil, err
	}
	model := models[0]
	return db.QueryByTableModel(model, vs, c)

}
func (db Db) Counts(column string, table string, c Condition) (int64, error) {
	table := CreateSingleValueTableModel()
}

func (db Db) WorkInTransaction(work TransactionWork) (int, error) {
	result := 0
	tx, err := db.rawDb.Begin()
	if err != nil {
		return result, err
	}

	result, err = work(&Db{rawDb: db.rawDb, factory: db.factory, executor: tx})
	if err != nil {
		tx.Rollback()
		return result, err
	}
	tx.Commit()
	return result, nil
}
func (db Db) execute(job SqlGenerator) (int, error) {
	result := 0
	if debug {
		fmt.Println("tableModels were:", job.tableModels)
	}
	for _, table := range job.tableModels {
		sql, datas := job.createSql(table)
		if debug {
			fmt.Println(sql, datas)
		}
		st, ers := db.executor.Prepare(sql)
		if ers != nil {
			if debug {
				fmt.Println("error when execute sql:", sql, "error:", ers.Error())
			}
			return -1, ers
		}
		rt, ers := st.Exec(datas...)
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
func (db Db) Insert(vs ...interface{}) (int, error) {
	models, err := getTableModels(vs...)
	if err != nil {
		return -1, nil
	}
	return db.execute(SqlGenerator{db.factory.Insert, models})
}

func (db Db) Replace(vs ...interface{}) (int, error) {
	models, err := getTableModels(vs...)
	if err != nil {
		return -1, nil
	}
	return db.execute(SqlGenerator{db.factory.Replace, models})
}

func (db Db) Delete(vs ...interface{}) (int, error) {
	models, err := getTableModels(vs...)
	if err != nil {
		return -1, nil
	}
	return db.execute(SqlGenerator{db.factory.Delete, models})
}

func (db Db) DeleteByConditon(v interface{}, c Condition) (int, error) {
	models, err := getTableModel(v)
	if err != nil {
		return -1, nil
	}
	model := models[0]
	if c.State() != "" {
		model.Cnd = c
	}
	return db.execute(SqlGenerator{db.factory.Delete, []TableModel{model}})
}

func (db Db) Update(vs ...interface{}) (int, error) {
	models, err := getTableModels(vs...)
	if err != nil {
		return -1, nil
	}
	return db.execute(SqlGenerator{db.factory.Update, models})
}

func (db Db) UpdateByCondition(v interface{}, c Condition) (int, error) {
	models, err := getTableModel(v)
	if err != nil {
		return -1, nil
	}
	model := models[0]
	if c.State() != "" {
		model.Cnd = c
	}
	return db.execute(SqlGenerator{db.factory.Update, []TableModel{model}})
}
