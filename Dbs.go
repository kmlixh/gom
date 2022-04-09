package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"gitee.com/janyees/gom/structs"
)

type DB struct {
	id       int64
	factory  structs.SqlFactory
	db       *sql.DB
	cnd      *structs.Condition
	table    *string
	rawSql   *string
	rawData  *[]interface{}
	tx       *sql.Tx
	orderBys *[]structs.OrderBy
	page     *structs.Page
}
type TransactionWork func(databaseTx *DB) (int64, error)

func (db DB) RawDb() *sql.DB {
	return db.db
}
func (db DB) Table(table string) DB {
	db.CloneIfDifferentRoutine()
	db.table = &table
	return db
}
func (db *DB) CloneIfDifferentRoutine() {
	if db.id != structs.GetGoid() {
		*db = db.Clone()
	}
}
func (db DB) Raw(sql string, datas ...interface{}) DB {
	db.CloneIfDifferentRoutine()
	db.rawSql = &sql
	var temp = structs.UnZipSlice(datas)
	db.rawData = &temp
	return db
}

func (db DB) OrderBy(field string, t structs.OrderType) DB {
	db.CloneIfDifferentRoutine()
	var temp []structs.OrderBy
	temp = append(temp, structs.MakeOrderBy(field, t))
	db.orderBys = &temp
	return db
}
func (db DB) CleanOrders() DB {
	db.CloneIfDifferentRoutine()
	temp := make([]structs.OrderBy, 0)
	db.orderBys = &temp
	return db
}
func (db DB) OrderByAsc(field string) DB {
	db.OrderBy(field, structs.Asc)
	return db
}
func (db DB) OrderByDesc(field string) DB {
	db.OrderBy(field, structs.Desc)
	return db
}

func (db DB) Where2(sql string, patches ...interface{}) DB {
	db.CloneIfDifferentRoutine()
	return db.Where(structs.CndRaw(sql, patches...))
}
func (db DB) Where(cnd structs.Condition) DB {
	db.CloneIfDifferentRoutine()
	db.cnd = &cnd
	return db
}
func (db DB) Clone() DB {
	return DB{id: structs.GetGoid(), factory: db.factory, db: db.db}
}

func (db DB) Page(index int, pageSize int) DB {
	db.CloneIfDifferentRoutine()
	page := structs.MakePage(index, pageSize)
	db.page = &page
	return db
}

func (db DB) Count(columnName string) structs.CountResult {
	sql := fmt.Sprintf("select count(`%s`) as count from `%s`", columnName, *db.table)
	var countResult structs.CountResult
	tb, er := structs.GetTableModel(&countResult, "count")
	if er != nil {
		panic(er)
	}
	_, er = db.query(sql, nil, tb)
	countResult.Error = er
	return countResult
}

func (db DB) Sum(columnName string) structs.CountResult {
	sql := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, *db.table)
	var countResult structs.CountResult
	tb, er := structs.GetTableModel(&countResult, "count")
	if er != nil {
		panic(er)
	}
	_, er = db.query(sql, nil, tb)
	countResult.Error = er
	return countResult
}

func (db DB) Select(vs interface{}, columns ...string) (interface{}, error) {
	db.CloneIfDifferentRoutine()
	model, er := structs.GetTableModel(vs, columns...)
	if er != nil {
		panic(er)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		return db.query(*db.rawSql, *db.rawData, model)
	} else {
		db.initTableModel(model)
		return db.SelectByModel(model)
	}
}
func (db DB) SelectByModel(model structs.TableModel) (interface{}, error) {
	//TODO 此处逻辑不合理，如果是自定义查询的话，无需生成Model，简单的查询也不需要生成model。
	db.CloneIfDifferentRoutine()
	selectFunc := db.factory.GetSqlFunc(structs.Query)
	sqlProtos := selectFunc(model)
	return db.query(sqlProtos[0].PreparedSql, sqlProtos[0].Data, model)
}
func (db DB) First(vs interface{}) (interface{}, error) {
	return db.Page(0, 1).Select(vs)
}

func (db DB) Update(v interface{}, columns ...string) (int64, int64, error) {
	vs, er := structs.GetTableModel(v, columns...)
	if er != nil {
		panic(er)
	}
	db.initTableModel(vs)
	return db.Execute(structs.Update, []structs.TableModel{vs})
}
func (db DB) Update2(vs ...interface{}) (int64, int64, error) {
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		rs, er := db.execute(*db.rawSql, *db.rawData)
		if er != nil {
			return 0, 0, er
		}
		c, er := rs.RowsAffected()
		return c, 0, er
	}
	if len(vs) == 0 {
		return 0, 0, errors.New("insert nothing")
	} else {
		var vvs []structs.TableModel
		for _, v := range vs {
			t, er := structs.GetTableModel(v)
			if er != nil {
				panic(er)
			}
			db.initTableModel(t)
			vvs = append(vvs, t)
		}
		return db.Execute(structs.Update, vvs)
	}
}

func (db DB) Insert(v interface{}, columns ...string) (int64, int64, error) {
	vs, er := structs.GetTableModel(v, columns...)
	if er != nil {
		panic(er)
	}
	db.initTableModel(vs)
	return db.Execute(structs.Insert, []structs.TableModel{vs})
}
func (db DB) Insert2(vs []interface{}) (int64, int64, error) {
	if len(vs) == 0 {
		return 0, 0, errors.New("insert nothing")
	} else {
		var vvs []structs.TableModel
		for _, v := range vs {
			t, er := structs.GetTableModel(v)
			if er != nil {
				panic(er)
			}
			db.initTableModel(t)
			vvs = append(vvs, t)
		}
		return db.Execute(structs.Insert, vvs)
	}
}
func (db DB) Delete(vs ...interface{}) (int64, int64, error) {
	if len(vs) == 0 {
		vvs, er := structs.GetTableModel(nil)
		if er != nil {
			panic(er)
		}
		db.initTableModel(vvs)
		return db.Execute(structs.Delete, []structs.TableModel{vvs})
	} else {
		var vvs []structs.TableModel
		for _, v := range vs {
			t, er := structs.GetTableModel(v)
			if er != nil {
				panic(er)
			}
			db.initTableModel(t)
			vvs = append(vvs, t)
		}
		return db.Execute(structs.Delete, vvs)
	}
}
func (db DB) Execute(sqlType structs.SqlType, models []structs.TableModel) (int64, int64, error) {
	db.CloneIfDifferentRoutine()
	var lastInsertId = int64(0)
	genFunc := db.factory.GetSqlFunc(sqlType)
	//此处应当判断是否已经在事物中，如果不在事务中才开启事物
	count := int64(0)
	sqlProtos := genFunc(models...)
	for _, sqlProto := range sqlProtos {
		if Debug {
			fmt.Println(sqlProto)
		}
		rs, er := db.execute(sqlProto.PreparedSql, sqlProto.Data...)
		if er != nil {
			return 0, 0, er
		}
		cs, err := rs.RowsAffected()
		if cs == 1 && len(sqlProtos) == len(models) && sqlType == structs.Insert {
			//
			id, er := rs.LastInsertId()
			if er == nil {
				lastInsertId = id
			}
		}
		if err != nil {
			return cs, 0, err
		}
		count += cs
	}
	db.CloneIfDifferentRoutine()
	return count, lastInsertId, nil
}

func (db DB) execute(sql string, data ...interface{}) (sql.Result, error) {
	st, err := db.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return st.Exec(data...)
}

func (db DB) getCnd() structs.Condition {
	if db.cnd != nil && *db.cnd != nil {
		return *db.cnd
	}
	return nil
}

func (db DB) query(statement string, data []interface{}, model structs.TableModel) (interface{}, error) {
	if Debug {
		fmt.Println("Execute query,PreparedSql:", statement, "data was:", data)
	}
	st, err := db.db.Prepare(statement)
	defer st.Close()
	if err != nil {
		return nil, err
	}
	rows, errs := st.Query(data...)
	if errs != nil {
		return nil, errs
	}
	defer rows.Close()
	db.CloneIfDifferentRoutine()
	return model.Scan(rows)

}

func (db DB) Transaction(work TransactionWork) (int64, error) {
	result := int64(0)
	tx := db.tx
	if tx == nil { //if transaction is nil create it
		var err error
		tx, err = db.db.Begin()
		if err != nil {
			return result, err
		}
	}

	result, err := work(&db)
	if err != nil {
		tx.Rollback()
		return result, err
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println("transaction commit err:", err)
	}
	return result, err
}

func (db DB) getOrderBys() []structs.OrderBy {
	if db.orderBys != nil && *db.orderBys != nil {
		return *db.orderBys
	}
	return nil
}

func (db DB) getPage() structs.Page {
	if db.page != nil && *db.page != nil {
		return *db.page
	}
	return nil
}
func (db *DB) initTableModel(t structs.TableModel) {
	if db.table != nil {
		t.SetTable(*db.table)
	}
	if db.cnd != nil {
		t.SetCondition(*db.cnd)
	}

}
func (db *DB) CleanDb() *DB {
	db.table = nil
	db.page = nil
	db.orderBys = nil
	db.rawSql = nil
	db.cnd = nil
	return db
}
