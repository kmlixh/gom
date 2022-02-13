package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"gitee.com/janyees/gom/structs"
	"reflect"
	"strings"
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
	model    structs.StructModel
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
	db.CloneIfDifferentRoutine()
	var countResult structs.CountResult
	db.Select(&countResult, "count("+columnName+") as count")
	return countResult
}

func (db DB) Sum(columnName string) structs.CountResult {
	db.CloneIfDifferentRoutine()
	var countResult structs.CountResult
	db.Select(&countResult, "SUM("+columnName+") as count")
	return countResult
}

func (db DB) Select(vs interface{}, columns ...string) (interface{}, error) {
	db.CloneIfDifferentRoutine()
	model, er := structs.GetStructModel(vs)
	if er != nil {
		panic(er)
	}
	return db.SelectByModel(model)
}
func (db DB) SelectByModel(model structs.StructModel) (interface{}, error) {
	db.CloneIfDifferentRoutine()
	db.model = model
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		return db.query(*db.rawSql, *db.rawData, model)
	} else {
		selectFunc := db.factory.GetSqlFunc(structs.Query)
		sqlProtos := selectFunc(structs.TableModel{Table: db.getTableName(), Columns: db.getQueryColumns(), Condition: db.getCnd(), OrderBys: db.getOrderBys(), Page: db.getPage()})
		return db.query(sqlProtos[0].Sql, sqlProtos[0].Data, model)
	}
}
func (db DB) First(vs interface{}) (interface{}, error) {
	return db.Page(0, 1).Select(vs)
}
func (db DB) Update(vs interface{}, columns ...string) (int64, error) {
	_, _, slice := structs.GetType(vs)
	if slice && len(columns) > 0 {
		panic(errors.New("can't update slice or array,please use UpdateMulti"))
	}
	db.CloneIfDifferentRoutine()
	return db.Execute(structs.Update, []interface{}{vs}, columns...)
}
func (db DB) UpdateMulti(vs ...interface{}) (int64, error) {
	return db.Execute(structs.Update, vs)
}
func (db DB) Insert(vs interface{}, columns ...string) (int64, error) {
	_, _, slice := structs.GetType(vs)
	if slice && len(columns) > 0 {
		panic(errors.New("can't Insert slice or array,please use UpdateMulti"))
	}
	db.CloneIfDifferentRoutine()
	return db.Execute(structs.Insert, []interface{}{vs}, columns...)
}

func (db DB) InsertMulti(vs ...interface{}) (int64, error) {
	return db.Execute(structs.Insert, vs)
}
func (db DB) Delete(vs ...interface{}) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, structs.DefaultStruct{})
	}
	db.CloneIfDifferentRoutine()
	return db.Execute(structs.Delete, vs)
}
func (db DB) Execute(sqlType structs.SqlType, vs []interface{}, columns ...string) (int64, error) {
	db.CloneIfDifferentRoutine()
	genFunc := db.factory.GetSqlFunc(sqlType)
	//此处应当判断是否已经在事物中，如果不在事务中才开启事物
	count := int64(0)
	var vmap = structs.SliceToGroupSlice(vs)
	for i, v := range vmap {
		if Debug {
			fmt.Println("Model Type was:", i, "slice counts:", len(v))
		}
		var models []structs.TableModel
		for _, v := range vmap[i] {
			structModel, er := structs.GetStructModel(v, columns...)
			if er != nil {
				return 0, er
			}
			db.model = structModel
			models = append(models, db.genTableModel(sqlType, v, columns...))
		}
		sqlProtos := genFunc(models...)
		cc := int64(0)
		for _, sqlProto := range sqlProtos {
			if Debug {
				fmt.Println(sqlProto)
			}
			rs, er := db.execute(sqlProto.Sql, sqlProto.Data...)
			if er != nil {
				return 0, er
			}
			cs, err := rs.RowsAffected()
			if err != nil {
				return cs, err
			}
			cc += cs
		}

		count += cc
	}
	return count, nil
}

func (db DB) genTableModel(sqlType structs.SqlType, v interface{}, columns ...string) structs.TableModel {

	//TODO 此处应当根据sql的类型生成对应类型的TableModel，如插入是所有，搜索是全部实体，而更新则需要将主键做条件，其他列作为更新值
	if len(columns) == 0 {
		switch sqlType {
		case structs.Update:
			columns = db.getUpdateColumns(columns...)
		case structs.Insert:
			columns = db.getInsertColumns(columns...)
		}
	}
	maps, er := structs.StructToMap(v, columns...)
	if er != nil {
		panic(er)
	}
	model := structs.TableModel{Table: db.getTableName(), Columns: columns, Data: maps, Condition: db.getCnd(), OrderBys: db.getOrderBys(), Page: db.getPage()}
	return model
}

func (db DB) execute(sql string, data ...interface{}) (sql.Result, error) {
	st, err := db.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return st.Exec(data...)
}

func (db DB) getTableName() string {
	if db.table == nil || len(*db.table) == 0 {
		return db.model.TableName
	}
	return *db.table
}
func (db DB) getQueryColumns() []string {
	if db.cols != nil && len(*db.cols) > 0 {
		return *db.cols
	}
	return db.model.ColumnNames
}

func (db DB) getUpdateColumns(v interface{}, columns ...string) []string {
	model, er := structs.GetStructModel(v, columns...)
	if er != nil {
		panic("get table model failed")
	}
	if columns != nil && len(columns) > 0 {
		return columns
	}
	var cols []string
	if db.cols != nil && len(*db.cols) > 0 {
		cols = *db.cols
	} else {
		cols = db.model.ColumnNames
	}
	//del primary key
	primary := db.model.Primary.ColumnName
	var dst []string
	for _, k := range cols {
		if !strings.EqualFold(k, primary) {
			dst = append(dst, k)
		}
	}
	return dst
}
func (db DB) getCnd() structs.Condition {
	if db.cnd != nil && *db.cnd != nil {
		return *db.cnd
	}
	return db.model.GetPrimaryCondition()
}
func (db DB) getInsertColumns(model structs.StructModel) []string {
	var cols []string
	if db.cols != nil && len(*db.cols) > 0 {
		cols = *db.cols
	} else {
		cols = model.ColumnNames
	}
	if model.Primary.Auto {
		return cols
	} else {
		//del primary key
		primary := model.Primary.ColumnName
		var dst []string
		for _, k := range cols {
			if !strings.EqualFold(k, primary) {
				dst = append(dst, k)
			}
		}
		return dst
	}
}

func (db DB) query(sql string, data []interface{}, model structs.StructModel) (interface{}, error) {
	st, err := db.db.Prepare(sql)
	defer st.Close()
	if err != nil {
		return nil, err
	}
	rows, errs := st.Query(data...)
	if errs != nil {
		return nil, errs
	}
	defer rows.Close()
	columns, er := rows.Columns()
	transfer := structs.GetDataTransfer(structs.Md5Text(sql), columns, model)
	if er != nil {
		panic(er)
	}
	if transfer.Model().Value.Kind() == reflect.Slice {
		results := transfer.Model().Value

		for rows.Next() {
			val := transfer.GetValueOfTableRow(rows)
			results.Set(reflect.Append(results, val))
		}
		return results.Interface(), nil
	} else {
		if rows.Next() {
			val := transfer.GetValueOfTableRow(rows)
			vt := transfer.Model().Value
			vt.Set(val)
			return vt.Interface(), nil
		} else {
			return nil, nil
		}

	}
	return nil, nil
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
