package gom

import (
	"database/sql"
	"fmt"
	"reflect"
)

type DB struct {
	factory SqlFactory
	db      *sql.DB
}
type Execute func(TableModel) (string, []interface{})

type TransactionJob struct {
	execute Execute
	tms     []TableModel
}

func (db DB) GetRawDb() *sql.DB {
	return db.db
}
func (db DB) MakeInsertTransactionJob(tableModel []TableModel) TransactionJob {
	return TransactionJob{db.factory.Insert, tableModel}
}
func (db DB) MakeUpdateTransactionJob(tableModel []TableModel) TransactionJob {
	return TransactionJob{db.factory.Update, tableModel}
}
func (db DB) MakeDeleteTransactionJob(tableModel []TableModel) TransactionJob {
	return TransactionJob{db.factory.Delete, tableModel}
}

func (db DB) exec(executor TransactionJob) (int, error) {
	var results int
	for _, model := range executor.tms {
		sqls, datas := executor.execute(model)
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

type TransactionWork func(db DB) (int, error)

func (db DB) WorkInTransaction(work TransactionWork) (int, error) {
	result := 0
	tx, err := db.db.Begin()
	if err != nil {
		return result, err
	}
	result, err = work(db)
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
func (db DB) ExecuteTransactionJob(jobs ...TransactionJob) (int, error) {
	work := func(dd DB) (int, error) {
		result := 0
		for _, executor := range jobs {
			rt, ers := dd.exec(executor)
			result += rt
			if ers != nil {
				return result, ers
			}
		}
		return result, nil
	}
	return db.WorkInTransaction(work)
}
func (db DB) Insert(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return db.exec(TransactionJob{db.factory.Insert, models})
}
func (db DB) InsertInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteTransactionJob(TransactionJob{db.factory.Replace, tables})
}
func (db DB) Replace(vs ...interface{}) (int, error) {
	models := getTableModels(vs...)
	return db.exec(TransactionJob{db.factory.Replace, models})
}
func (db DB) ReplaceInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteTransactionJob(TransactionJob{db.factory.Replace, tables})
}
func (db DB) Delete(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.exec(TransactionJob{db.factory.Delete, tables})
}
func (db DB) DeleteInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteTransactionJob(TransactionJob{db.factory.Delete, tables})
}
func (db DB) DeleteByConditon(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	if c.State() != "" {
		tableModel.Cnd = c
	}
	return db.exec(TransactionJob{db.factory.Delete, []TableModel{tableModel}})
}
func (db DB) DeleteByConditonInTransaction(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.ExecuteTransactionJob(TransactionJob{db.factory.Delete, []TableModel{tableModel}})
}
func (db DB) Update(vs ...interface{}) (int, error) {
	tms := getTableModels(vs...)
	return db.exec(TransactionJob{db.factory.Update, tms})
}
func (db DB) UpdateInTransaction(vs ...interface{}) (int, error) {
	tables := getTableModels(vs...)
	return db.ExecuteTransactionJob(TransactionJob{db.factory.Update, tables})
}
func (db DB) UpdateByCondition(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.exec(TransactionJob{db.factory.Update, []TableModel{tableModel}})
}
func (db DB) UpdateByConditionInTransaction(v interface{}, c Condition) (int, error) {
	tableModel := getTableModel(v)
	tableModel.Cnd = c
	return db.exec(TransactionJob{db.factory.Update, []TableModel{tableModel}})
}
func (db DB) QueryByTableModel(model TableModel, vs interface{}, c Condition) interface{} {
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

func (db DB) Query(vs interface{}, c Condition) interface{} {
	model := getTableModel(vs)
	return db.QueryByTableModel(model, vs, c)

}
