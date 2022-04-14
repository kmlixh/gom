package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"gitee.com/janyees/gom/v2/arrays"
	"gitee.com/janyees/gom/v2/cnds"
	"gitee.com/janyees/gom/v2/structs"
)

type DB struct {
	id       int64
	factory  structs.SqlFactory
	db       *sql.DB
	cnd      *cnds.Condition
	table    *string
	rawSql   *string
	rawData  *[]interface{}
	tx       *sql.Tx
	orderBys *[]structs.OrderBy
	page     *structs.Page
}

type TransactionWork func(databaseTx *DB) (interface{}, error)

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
	return db.Where(cnds.NewRaw(sql, patches...))
}
func (db DB) Where(cnd cnds.Condition) DB {
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

func (db DB) Count(columnName string) (int64, error) {
	statements := fmt.Sprintf("select count(`%s`) as count from `%s`", columnName, *db.table)
	var count int64
	tb, er := structs.GetTableModel(&count, "count")
	if er != nil {
		panic(er)
	}
	_, er = db.query(statements, nil, tb)

	return count, er
}

func (db DB) Sum(columnName string) structs.CountResult {
	statements := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, *db.table)
	var countResult structs.CountResult
	tb, er := structs.GetTableModel(&countResult, "count")
	if er != nil {
		panic(er)
	}
	_, er = db.query(statements, nil, tb)
	countResult.Error = er
	return countResult
}

func (db DB) Select(vs interface{}, columns ...string) (interface{}, error) {
	db.CloneIfDifferentRoutine()
	model, er := structs.GetTableModel(vs, columns...)
	if er != nil {
		return nil, er
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
func (db DB) Insert(v interface{}, columns ...string) (int64, int64, error) {
	return db.execute(structs.Insert, arrays.Of(v), columns...)

}
func (db DB) Delete(vs ...interface{}) (int64, int64, error) {
	return db.execute(structs.Delete, vs)

}
func (db DB) Update(v interface{}, columns ...string) (int64, int64, error) {
	return db.execute(structs.Update, arrays.Of(v), columns...)
}

func (db DB) execute(sqlType structs.SqlType, v []interface{}, columns ...string) (int64, int64, error) {
	var vs []interface{}
	if v != nil && len(v) > 0 {
		vs = append(vs, structs.UnZipSlice(v)...)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		if vs != nil && len(vs) > 0 {
			return 0, 0, errors.New("when the RawSql is not nil or empty,data should be nil")
		}
		return db.ExecuteRaw()
	}
	if len(vs) == 0 && db.table == nil && db.cnd == nil {
		return 0, 0, errors.New("there was nothing to do")
	} else {
		var vvs []structs.TableModel
		if len(vs) == 0 {
			t, _ := structs.GetTableModel(nil, columns...)
			db.initTableModel(t)
			if sqlType == structs.Update && t.Condition() == nil {
				return 0, 0, errors.New("can't update Database without Conditions")
			}
			vvs = append(vvs, t)
		} else {
			for _, v := range vs {
				t, er := structs.GetTableModel(v, columns...)
				if er != nil {
					panic(er)
				}
				db.initTableModel(t)
				if sqlType == structs.Update && t.Condition() == nil {
					return 0, 0, errors.New("can't update Database without Conditions")
				}
				vvs = append(vvs, t)
			}
		}
		return db.executeTableModel(sqlType, vvs)
	}
}
func (db DB) ExecuteRaw() (int64, int64, error) {
	rs, er := db.ExecuteStatement(*db.rawSql, *db.rawData...)
	if er != nil {
		return 0, 0, er
	}
	c, er := rs.RowsAffected()
	return c, 0, er
}

func (db DB) executeTableModel(sqlType structs.SqlType, models []structs.TableModel) (int64, int64, error) {
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
		rs, er := db.ExecuteStatement(sqlProto.PreparedSql, sqlProto.Data...)
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
	return count, lastInsertId, nil
}

func (db DB) ExecuteStatement(statement string, data ...interface{}) (sql.Result, error) {
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

func (db DB) prepare(query string) (*sql.Stmt, error) {
	if db.IsInTransaction() {
		st, er := db.tx.Prepare(query)
		if er != nil {
			db.Rollback()
		}
		return st, er
	}
	return db.db.Prepare(query)
}

func (db DB) getCnd() cnds.Condition {
	if db.cnd != nil && *db.cnd != nil {
		return *db.cnd
	}
	return nil
}

func (db DB) query(statement string, data []interface{}, model structs.TableModel) (interface{}, error) {
	if Debug {
		fmt.Println("executeTableModel query,PreparedSql:", statement, "data was:", data)
	}
	st, err := db.prepare(statement)
	if err != nil {
		return nil, err
	}
	defer func(st *sql.Stmt, err error) {
		if err == nil {
			st.Close()
		}

	}(st, err)
	if err != nil {
		return nil, err
	}
	rows, errs := st.Query(data...)
	if errs != nil {
		return nil, errs
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			fmt.Println(err)
		}
		result := recover()
		if result != nil {
			db.Rollback()
		}
	}(rows)
	return model.Scan(rows)

}
func (db *DB) Begin() error {
	if db.tx != nil {
		return errors.New(" there was a DoTransaction")
	}
	tx, err := db.db.Begin()
	db.tx = tx
	return err
}
func (db DB) IsInTransaction() bool {
	return db.tx != nil
}
func (db *DB) Commit() {
	if db.IsInTransaction() {
		db.tx.Commit()
		db.tx = nil
	}
}
func (db *DB) Rollback() {
	if db.tx != nil {
		db.tx.Rollback()
		db.tx = nil
	}
}

func (db DB) DoTransaction(work TransactionWork) (interface{}, error) {
	//Create A New Db And set Tx for it
	dbTx := db.Clone()
	eb := dbTx.Begin()
	if eb != nil {
		return nil, eb
	}
	defer func(dbTx *DB) { //catch the panic of 'work' function
		if r := recover(); r != nil {
			dbTx.Rollback()
		}
	}(&dbTx)
	i, es := work(&dbTx)
	if es != nil {
		dbTx.Rollback()
	} else {
		dbTx.Commit()
	}
	return i, es
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
		err := t.SetCondition(*db.cnd)
		if err != nil {
			panic(err)
		}
	}
	if db.page != nil {
		err := t.SetPage(*db.page)
		if err != nil {
			panic(err)
		}
	}
	if db.orderBys != nil {
		err := t.SetOrderBys(*db.orderBys)
		if err != nil {
			panic(err)
		}
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
