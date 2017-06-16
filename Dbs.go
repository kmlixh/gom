package gom

import (
	"database/sql"
	"fmt"
	"reflect"
)

type DataBase interface {
	Insert(vs ...interface{}) (int, error)
	Update(vs ...interface{}) (int, error)
}

type DataBases struct {
	factory SqlFactory
	db      *sql.DB
}
type CreateSql func(TableModel) (string, []interface{})

type SqlGenerator struct {
	createSql   CreateSql
	tableModels []TableModel
}

func (db DataBases) GetRawDb() *sql.DB {
	db.db.ExecContext()
	return db.db
}
func (db DataBases) MakeInsertSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{db.factory.Insert, tableModel}
}
func (db DataBases) MakeUpdateSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{db.factory.Update, tableModel}
}
func (db DataBases) MakeDeleteSqlGenerator(tableModel []TableModel) SqlGenerator {
	return SqlGenerator{db.factory.Delete, tableModel}
}

func (db DataBases) execute(manager SqlGenerator) (int, error) {
	var results int
	for _, model := range manager.tableModels {
		sqls, datas := manager.createSql(model)
		if debug {
			fmt.Println(sqls, datas)
		}
		result, err := db.db.Exec(sqls, datas...)
		if err != nil {
			return results, err
		} else {
			rows, _ := result.RowsAffected()
			results += int(rows)
		}
	}
	return results, nil
}

type TransactionWork func(tx *sql.Tx) (int, error)

func (db DataBases) WorkInTransaction(work TransactionWork) (int, error) {
	result := 0
	tx, err := db.db.Begin()
	if err != nil {
		return result, err
	}
	result, err = work(tx)
	if err != nil {
		if debug {
			fmt.Println("rollback transaction")
		}
		tx.Rollback()
		return result, err
	}
	tx.Commit()
	return result, nil
}
func (db DataBases) ExecuteSqlGenerator(jobs ...SqlGenerator) (int, error) {
	work := func(dd DataBases) (int, error) {
		result := 0
		for _, executor := range jobs {
			rt, ers := dd.execute(executor)
			result += rt
			if ers != nil {
				return result, ers
			}
		}
		return result, nil
	}
	return db.WorkInTransaction(work)
}
func (db DataBases) Insert(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Insert, models})
}
func (db DataBases) InsertInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteSqlGenerator(SqlGenerator{db.factory.Replace, tables})
}
func (db DataBases) Replace(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Replace, models})
}
func (db DataBases) ReplaceInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteSqlGenerator(SqlGenerator{db.factory.Replace, tables})
}
func (db DataBases) Delete(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Delete, tables})
}
func (db DataBases) DeleteInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteSqlGenerator(SqlGenerator{db.factory.Delete, tables})
}
func (db DataBases) DeleteByConditon(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	if c.State() != "" {
		tableModel.Cnd = c
	}
	return db.execute(SqlGenerator{db.factory.Delete, []TableModel{tableModel}})
}
func (db DataBases) DeleteByConditonInTransaction(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.ExecuteSqlGenerator(SqlGenerator{db.factory.Delete, []TableModel{tableModel}})
}
func (db DataBases) Update(vs ...interface{}) (int, error) {
	tms := getTableModels(vs...)
	return db.execute(SqlGenerator{db.factory.Update, tms})
}
func (db DataBases) UpdateInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteSqlGenerator(SqlGenerator{db.factory.Update, tables})
}
func (db DataBases) UpdateByCondition(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.execute(SqlGenerator{db.factory.Update, []TableModel{tableModel}})
}
func (db DataBases) UpdateByConditionInTransaction(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.execute(SqlGenerator{db.factory.Update, []TableModel{tableModel}})
}
func (db DataBases) QueryByTableModel(model TableModel, vs interface{}, c Condition) interface{} {
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
			sqls, adds := db.factory.Query(model)
			if debug {
				fmt.Println(sqls, adds)
			}
			rows, err := db.db.Query(sqls, adds...)
			if err != nil {
				return nil
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
			return vs

		} else {
			sqls, adds := db.factory.Query(model)
			if debug {
				fmt.Println(sqls, adds)
			}
			row := db.db.QueryRow(sqls, adds...)
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
			return vt.Interface()
		}

	} else {
		return nil
	}
}

func (db DataBases) Query(vs interface{}, c Condition) interface{} {
	model := getTableModel(vs)
	return db.QueryByTableModel(model, vs, c)

}
