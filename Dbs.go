package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type ISqlGenerator interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Prepare(query string) (*sql.Stmt, error)
}
type Db struct {
	factory SqlFactory
	db      *sql.DB
	cnd     Condition
}

func (this Db) RawDb() *sql.DB {
	return this.db
}
func (this Db) Where(sql string, patches ...interface{}) Db {
	return this.Where2(Cnd(sql, patches...))
}
func (this Db) Where2(cnd Condition) Db {
	return Db{this.factory, this.db, cnd}
}
func (this Db) Clone() Db {
	return this.clone()
}
func (this Db) clone() Db {
	return Db{this.factory, this.db, nil}
}
func (thiz Db) Select(vs interface{}, nameFilters ...string) (interface{}, error) {

	model, err := getTableModel(vs, nameFilters...)
	if err != nil {
		return nil, err
	}
	return thiz.SelectWithModel(model, vs)
}

func (this Db) Count(columnName string, table string) (int64, error) {
	var counts int64
	columns := make(map[string]Column)
	columns["result"] = Column{ColumnName: "result", Type: reflect.TypeOf(counts), QueryField: "count(" + columnName + ") as result", IsPrimary: false, Auto: false}
	tableModel := TableModel{Columns: columns, ColumnNames: []string{"result"}, Type: reflect.TypeOf(counts), Value: reflect.ValueOf(counts), TableName: table}
	_, er := this.SelectWithModel(tableModel, &counts)
	return counts, er
}
func (this Db) SelectWithModel(model TableModel, vs interface{}) (interface{}, error) {
	tps, isPtr, islice := getType(vs)
	if debug {
		fmt.Println("model:", model)
	}
	if len(model.TableName) > 0 {
		if islice {
			results := reflect.Indirect(reflect.ValueOf(vs))
			sqls, datas := this.factory.Query(model, this.cnd)
			if debug {
				fmt.Println(sqls, datas)
			}
			st, err := this.db.Prepare(sqls)
			if err != nil {
				return nil, err
			}
			rows, err := st.Query(datas...)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.NextResultSet() {
				val := getValueOfTableRow(model, rows)
				results.Set(reflect.Append(results, val))
			}
			return vs, nil
		} else {
			sqls, datas := this.factory.Query(model, this.cnd)
			if debug {
				fmt.Println(sqls, datas)
			}
			st, err := this.db.Prepare(sqls)
			if err != nil {
				return nil, err
			}
			rows, err := st.Query(datas...)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			val := getValueOfTableRow(model, rows)
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

func (this Db) WorkInTransaction(work TransactionWork) (int, error) {
	result := 0
	tx, err := this.db.Begin()
	if err != nil {
		return result, err
	}
	result, err = work(&Db{db: this.db, factory: this.factory})
	if err != nil {
		tx.Rollback()
		return result, err
	}
	tx.Commit()
	return result, nil
}
func (this Db) execute(createSql CreateSql, table TableModel) (int, error) {
	result := 0
	sql, datas := createSql(table, this.cnd)
	if debug {
		fmt.Println(sql, datas)
	}
	st, ers := this.db.Prepare(sql)
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
	return result, nil
}
func (thiz Db) execute2(createSql CreateSql, vs ...interface{}) (int, error) {
	c := 0
	var er error
	len := len(vs)
	for _, v := range vs {
		kind := reflect.Indirect(reflect.ValueOf(v)).Kind()
		if kind == reflect.Interface {
			panic("can't work with interface")
		} else if kind == reflect.Slice || kind == reflect.Array {
			ct, et := thiz.clone().execute2(createSql, v.([]interface{})...)
			if et != nil {
				return ct, et
			}
			c += ct
		} else if kind == reflect.Struct {
			model, err := getTableModel(v)
			if err == nil {
				if len > 1 {
					ct, et := thiz.clone().execute(createSql, model)
					if et != nil {
						return ct, et
					}
					c += ct
				} else {
					ct, et := thiz.execute(createSql, model)
					if et != nil {
						return ct, et
					}
					c += ct
				}

			} else {
				return 0, err
			}
		}
	}
	return c, er
}
func (thiz Db) Insert(vs ...interface{}) (int, error) {
	return thiz.execute2(thiz.factory.Insert, vs...)
}
func (thiz Db) InsertIgnore(vs ...interface{}) (int, error) {
	return thiz.execute2(thiz.factory.InsertIgnore, vs...)
}

func (thiz Db) Replace(vs ...interface{}) (int, error) {
	return thiz.execute2(thiz.factory.Replace, vs...)
}
func (thiz Db) Delete(vs ...interface{}) (int, error) {
	return thiz.execute2(thiz.factory.Delete, vs...)
}

func (thiz Db) Update(vs interface{}, nameFilters ...string) (int, error) {
	kind := reflect.Indirect(reflect.ValueOf(vs)).Kind()
	if kind == reflect.Interface {
		panic("can't work with interface")
	} else if kind == reflect.Slice || kind == reflect.Array {
		return thiz.Update2(vs.([]interface{})...)
	} else if kind == reflect.Struct {
		model, err := getTableModel(vs, nameFilters...)
		if err == nil {
			return thiz.execute(thiz.factory.Update, model)
		} else {
			return 0, err
		}
	}
	return 0, nil
}

func (thiz Db) Update2(vs ...interface{}) (int, error) {
	c := 0
	var er error
	if len(vs) == 1 {
		return thiz.Update(vs[0])
	} else {
		for v := range vs {
			cc, ers := thiz.clone().Update(v)
			if ers != nil {
				return cc, ers
			}
			c += cc

		}
	}
	return c, er
}
