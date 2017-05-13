package gom

import (
	"database/sql"
	"fmt"
	"reflect"
)

type DB struct {
	Factory SqlFactory
	Db      *sql.DB
}
type Execute func(TableModel) (string, []interface{})

type Executor struct {
	execute Execute
	tms     []TableModel
}

type ExecutorType int

const (
	_ ExecutorType = iota
	Insert
	Delete
	Update
)

func (DB DB) GetExecutor(executorType ExecutorType, tableModel []TableModel) Executor {
	var execute Execute
	switch executorType {
	case Insert:
		execute = DB.Factory.Insert
	case Delete:
		execute = DB.Factory.Delete
	case Update:
		execute = DB.Factory.Update
	}
	return Executor{execute, tableModel}
}

func (DB DB) exec(executor Executor) (int, error) {
	var results int
	for _, model := range executor.tms {
		sqls, datas := executor.execute(model)
		if debug {
			fmt.Println(sqls, datas)
		}
		result, err := DB.Db.Exec(sqls, datas...)
		if err != nil {
			return results, err
		} else {
			rows, _ := result.RowsAffected()
			results += int(rows)
		}
	}
	return results, nil
}
func (DB DB) execTransc(executors ...Executor) (int, error) {
	result := 0
	tx, err := DB.Db.Begin()
	if err != nil {
		return result, err
	}
	for _, executor := range executors {
		result, err = DB.exec(executor)
		if err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	tx.Commit()
	return result, nil
}
func (DB DB) Insert(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return DB.exec(Executor{DB.Factory.Insert, models})
}
func (DB DB) InsertInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return DB.execTransc(Executor{DB.Factory.Insert, tables})
}
func (DB DB) Delete(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return DB.exec(Executor{DB.Factory.Delete, tables})
}
func (DB DB) DeleteInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return DB.execTransc(Executor{DB.Factory.Delete, tables})
}
func (DB DB) DeleteByConditon(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return DB.exec(Executor{DB.Factory.Delete, []TableModel{tableModel}})
}
func (DB DB) DeleteByConditonInTransaction(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return DB.execTransc(Executor{DB.Factory.Delete, []TableModel{tableModel}})
}
func (DB DB) Update(vs ...interface{}) (int, error) {
	tms := getTableModels(vs...)
	return DB.exec(Executor{DB.Factory.Update, tms})
}
func (DB DB) UpdateInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return DB.execTransc(Executor{DB.Factory.Update, tables})
}
func (DB DB) UpdateByCondition(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return DB.exec(Executor{DB.Factory.Update, []TableModel{tableModel}})
}
func (DB DB) UpdateByConditionInTransaction(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return DB.exec(Executor{DB.Factory.Update, []TableModel{tableModel}})
}

func (DB DB) Query(vs interface{}, c Condition) interface{} {
	tps, isPtr, islice := getType(vs)
	model := getTableModel(vs)
	if debug {
		fmt.Println("model:", model)
	}
	if len(model.TableName) > 0 {
		model.Cnd = c
		if islice {
			results := reflect.Indirect(reflect.ValueOf(vs))
			sqls, adds := DB.Factory.Query(model)
			if debug {
				fmt.Println(sqls, adds)
			}
			rows, err := DB.Db.Query(sqls, adds...)
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
			sqls, adds := DB.Factory.Query(model)
			if debug {
				fmt.Println(sqls, adds)
			}
			row := DB.Db.QueryRow(sqls, adds...)
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
