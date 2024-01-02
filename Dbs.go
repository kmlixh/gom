package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type DB struct {
	id       int64
	factory  SqlFactory
	db       *sql.DB
	cnd      *Condition
	table    *string
	rawSql   *string
	rawData  *[]interface{}
	tx       *sql.Tx
	orderBys *[]OrderBy
	page     *PageInfo
	sqlType  SqlType
}

type TransactionWork func(databaseTx *DB) (interface{}, error)

func (db DB) GetRawDb() *sql.DB {
	return db.db
}
func (db DB) Factory() SqlFactory {
	return db.factory
}
func (db *DB) SetTable(table string) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	db.table = &table
	return db
}
func (db *DB) cloneSelfIfDifferentGoRoutine() {
	if db.id != getGrouteId() {
		*db = db.Clone()
	}
}
func (db *DB) RawSql(sql string, datas ...interface{}) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	db.rawSql = &sql
	var temp = UnZipSlice(datas)
	db.rawData = &temp
	return db
}

func (db *DB) OrderBy(field string, t OrderType) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	var temp []OrderBy
	temp = append(temp, MakeOrderBy(field, t))
	db.orderBys = &temp
	return db
}
func (db *DB) OrderBys(orderbys []OrderBy) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	var temp []OrderBy
	temp = append(temp, orderbys...)
	db.orderBys = &temp
	return db
}
func (db *DB) CleanOrders() *DB {
	db.cloneSelfIfDifferentGoRoutine()
	temp := make([]OrderBy, 0)
	db.orderBys = &temp
	return db
}
func (db *DB) OrderByAsc(field string) *DB {
	return db.OrderBy(field, Asc)
}
func (db *DB) OrderByDesc(field string) *DB {
	return db.OrderBy(field, Desc)
}

func (db *DB) Where2(sql string, patches ...interface{}) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	return db.Where(CndRaw(sql, patches...))
}
func (db *DB) Where(cnd Condition) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	db.cnd = &cnd
	return db
}
func (db DB) Clone() DB {
	return DB{id: getGrouteId(), factory: db.factory, db: db.db}
}

func (db *DB) Page(page int64, pageSize int64) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	pages := MakePage(page, pageSize)
	db.page = &pages
	return db
}

func (db DB) Count(columnName string) (int64, error) {
	statements := fmt.Sprintf("select count(`%s`) as count from `%s`", columnName, *db.table)
	var data []interface{}
	if db.cnd != nil && *db.cnd != nil {
		cndString, cndData := db.factory.ConditionToSql(false, *db.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64
	tb, er := db.GetTableModel(&count, "count")
	if er != nil {
		panic(er)
	}
	_, er = db.query(statements, data, tb.GetRowScanner())

	return count, er
}

func (db DB) Sum(columnName string) (int64, error) {
	statements := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, *db.table)
	var data []interface{}
	if db.cnd != nil && *db.cnd != nil {
		cndString, cndData := db.factory.ConditionToSql(false, *db.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64
	tb, er := db.GetTableModel(&count, "count")
	if er != nil {
		panic(er)
	}
	_, er = db.query(statements, data, tb.GetRowScanner())

	return count, er
}

func (db DB) Select(vs interface{}, columns ...string) (interface{}, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = Query
	model, er := db.GetTableModel(vs, columns...)
	if er != nil {
		return nil, er
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		return db.query(*db.rawSql, *db.rawData, model.GetRowScanner())
	} else {
		db.initTableModel(model)
		selectFunc := db.factory.GetSqlFunc(Query)
		sqlProtos := selectFunc(model)
		return db.query(sqlProtos[0].PreparedSql, sqlProtos[0].Data, model.GetRowScanner())
	}
}
func (db DB) First(vs interface{}) (interface{}, error) {
	return db.Page(0, 1).Select(vs)
}
func (db DB) Insert(v interface{}, columns ...string) (int64, int64, error) {
	db.sqlType = Insert
	return db.executeInside(ArrayOf(v), columns...)

}
func (db DB) Delete(vs ...interface{}) (int64, int64, error) {
	db.sqlType = Delete
	return db.executeInside(vs)

}
func (db DB) Update(v interface{}, columns ...string) (int64, int64, error) {
	db.sqlType = Update
	return db.executeInside(ArrayOf(v), columns...)
}

func (db DB) executeInside(v []interface{}, columns ...string) (int64, int64, error) {
	var vs []interface{}
	if v != nil && len(v) > 0 {
		vs = append(vs, UnZipSlice(v)...)
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
		var vvs []TableModel
		if len(vs) == 0 {
			t, _ := db.GetTableModel(nil, columns...)
			db.initTableModel(t)
			if db.sqlType == Update && t.Condition() == nil {
				return 0, 0, errors.New("can't update Database without Conditions")
			}
			vvs = append(vvs, t)
		} else {
			for _, v := range vs {
				t, er := db.GetTableModel(v, columns...)
				if er != nil {
					panic(er)
				}
				db.initTableModel(t)
				if db.sqlType == Update && t.Condition() == nil {
					return 0, 0, errors.New("can't update Database without Conditions")
				}
				vvs = append(vvs, t)
			}
		}
		return db.executeTableModel(db.sqlType, vvs)
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

func (db DB) executeTableModel(sqlType SqlType, models []TableModel) (int64, int64, error) {
	db.cloneSelfIfDifferentGoRoutine()
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
		if cs == 1 && len(sqlProtos) == len(models) && sqlType == Insert {
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

func (db DB) GetCnd() Condition {
	if db.cnd != nil && *db.cnd != nil {
		return *db.cnd
	}
	return nil
}

func (db DB) query(statement string, data []interface{}, rowScanner IRowScanner) (interface{}, error) {
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
			er, ok := result.(error)
			if ok {
				fmt.Println(er)
			}
			db.Rollback()
		}
	}(rows)
	return rowScanner.Scan(rows)
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

func (db DB) GetOrderBys() []OrderBy {
	if db.orderBys != nil && *db.orderBys != nil {
		return *db.orderBys
	}
	return nil
}

func (db DB) GetPage() (int64, int64) {
	if db.page != nil && *db.page != nil {
		return (*db.page).Page()
	}
	return 0, 0
}
func (db *DB) initTableModel(t TableModel) {
	if db.table != nil {
		t.SetTable(*db.table)
	}
	if db.cnd != nil && *db.cnd != nil {
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

var tableModelCache = make(map[string]TableModel)

func (db *DB) GetTableModel(v interface{}, choosedColumns ...string) (TableModel, error) {
	//2024年1月1号，此处的时间类型
	//防止重复创建map，需要对map创建过程加锁
	if v == nil {
		return &DefaultModel{}, nil
	}
	rawTableInfo := GetRawTableInfo(v)
	if !rawTableInfo.IsStruct && (choosedColumns == nil || len(choosedColumns) != 1) {
		return nil, errors.New("basic Type Only Support [1] Column Or2 nil")
	}
	tableName := rawTableInfo.RawTableName
	if db.table != nil {
		tableName = *db.table
	}
	var model TableModel
	cacheName := rawTableInfo.PkgPath() + "-" + tableName
	cachedModel, ok := tableModelCache[cacheName]
	if ok {
		model = cachedModel.Clone()
	} else {
		columns := db.factory.GetColumns(tableName, db.db)
		var temp TableModel
		var scanners []IScanner
		tempVal := reflect.Indirect(reflect.New(rawTableInfo.Type))
		if rawTableInfo.IsStruct {
			if rawTableInfo.IsStruct && rawTableInfo.Type.NumField() == 0 {
				_, ok := reflect.Indirect(reflect.New(rawTableInfo.Type)).Interface().(DefaultStruct)
				if !ok {
					return nil, errors.New(fmt.Sprintf("[%s] was a \"empty struct\",it has no field or All fields has been ignored", rawTableInfo.Type.Name()))
				} else {
					return &DefaultModel{}, nil
				}
			}
			columns = combineColumns(tempVal, columns)
			for _, column := range columns {
				scanners = append(scanners, GetIScannerOfColumn(column.Data))
			}
			temp = &DefaultModel{rawScanners: scanners, rawType: rawTableInfo.Type, rawTable: rawTableInfo.RawTableName, rawColumns: columns, rawColumnNames: columnNames, rawColumnIdxMap: columnIdxMap, primaryAuto: columns[0].PrimaryAuto}
		} else {
			scanners = append(scanners, GetIScannerOfColumn(reflect.Indirect(reflect.New(rawTableInfo.Type)).Interface()))
			temp = &DefaultModel{rawScanners: scanners, rawType: rawTableInfo.Type, rawTable: "", primaryAuto: false}
		}
		tableModelCache[cacheName] = temp
		model = temp.Clone()
	}
	model.SetData(v, reflect.Indirect(reflect.ValueOf(v)), rawTableInfo.IsStruct, rawTableInfo.IsPtr, rawTableInfo.IsSlice)
	er := model.SetColumns(choosedColumns)
	return model, er
}

func (db *DB) CleanDb() *DB {
	db.table = nil
	db.page = nil
	db.orderBys = nil
	db.rawSql = nil
	db.cnd = nil
	return db
}
