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
	cnd      structs.Condition
	table    string
	rawSql   string
	rawData  []interface{}
	tx       *sql.Tx
	orderBys []structs.OrderBy
	cols     []string
	page     structs.Page
	model    structs.StructModel
}
type TransactionWork func(databaseTx *DB) (int64, error)

func (this DB) RawDb() *sql.DB {
	return this.db
}
func (this *DB) Table(table string) *DB {
	this.CloneIfDifferentRoutine()
	this.table = table
	return this
}
func (this *DB) CloneIfDifferentRoutine() {
	if this.id != structs.GetGoid() {
		*this = this.Clone()
	}
}
func (this *DB) Raw(sql string, datas ...interface{}) *DB {
	this.CloneIfDifferentRoutine()
	this.rawSql = sql
	this.rawData = structs.UnZipSlice(datas)
	return this
}

func (this *DB) Columns(cols ...string) *DB {
	this.CloneIfDifferentRoutine()
	this.cols = cols
	return this
}
func (this *DB) OrderBy(field string, t structs.OrderType) *DB {
	this.CloneIfDifferentRoutine()
	this.orderBys = append(this.orderBys, structs.MakeOrderBy(field, t))
	return this
}
func (this *DB) CleanOrders() *DB {
	this.CloneIfDifferentRoutine()
	this.orderBys = make([]structs.OrderBy, 0)
	return this
}
func (this *DB) OrderByAsc(field string) *DB {
	this.CloneIfDifferentRoutine()
	this.orderBys = append(this.orderBys, structs.MakeOrderBy(field, structs.Asc))
	return this
}
func (this *DB) OrderByDesc(field string) *DB {
	this.CloneIfDifferentRoutine()
	this.orderBys = append(this.orderBys, structs.MakeOrderBy(field, structs.Desc))
	return this
}

func (this *DB) Where2(sql string, patches ...interface{}) *DB {
	this.CloneIfDifferentRoutine()
	return this.Where(structs.CndRaw(sql, patches...))
}
func (this *DB) Where(cnd structs.Condition) *DB {
	this.CloneIfDifferentRoutine()
	this.cnd = cnd
	return this
}
func (this DB) Clone() DB {
	return DB{id: structs.GetGoid(), factory: this.factory, db: this.db}
}
func (this *DB) Page(index int, pageSize int) *DB {
	this.CloneIfDifferentRoutine()
	this.page = structs.MakePage(index, pageSize)
	return this
}

func (this DB) Count(columnName string) structs.CountResult {
	this.CloneIfDifferentRoutine()
	var countResult structs.CountResult
	this.Columns("count(" + columnName + ") as count")
	this.Select(&countResult)
	return countResult
}

func (this DB) Sum(columnName string) structs.CountResult {
	this.CloneIfDifferentRoutine()
	var countResult structs.CountResult
	this.Columns("SUM(" + columnName + ") as count")
	this.Select(&countResult)
	return countResult
}

func (this DB) Select(vs interface{}) (interface{}, error) {
	this.CloneIfDifferentRoutine()
	model, er := structs.GetStructModel(vs)
	if er != nil {
		panic(er)
	}
	return this.SelectByModel(model)
}
func (this DB) SelectByModel(model structs.StructModel) (interface{}, error) {
	this.CloneIfDifferentRoutine()
	this.model = model
	if len(this.rawSql) > 0 {
		return this.query(this.rawSql, this.rawData, model)
	} else {
		selectFunc := this.factory.GetSqlFunc(structs.Query)
		sqlProtos := selectFunc(structs.TableModel{Table: this.getTableName(), Columns: this.getQueryColumns(), Condition: this.getCnd(), OrderBys: this.orderBys, Page: this.page})
		return this.query(sqlProtos[0].Sql, sqlProtos[0].Data, model)
	}
}
func (this DB) First(vs interface{}) (interface{}, error) {
	return this.Page(0, 1).Select(vs)
}
func (thiz DB) Update(vs interface{}, columns ...string) (int64, error) {
	_, _, slice := structs.GetType(vs)
	if slice && len(columns) > 0 {
		panic(errors.New("can't update slice or array,please use UpdateMulti"))
	}
	thiz.CloneIfDifferentRoutine()

	return thiz.Execute(structs.Update, []interface{}{vs}, columns...)
}
func (thiz DB) UpdateMulti(vs ...interface{}) (int64, error) {
	return thiz.Execute(structs.Update, vs)
}
func (thiz DB) Insert(vs interface{}, columns ...string) (int64, error) {
	_, _, slice := structs.GetType(vs)
	if slice && len(columns) > 0 {
		panic(errors.New("can't Insert slice or array,please use UpdateMulti"))
	}
	thiz.CloneIfDifferentRoutine()
	return thiz.Execute(structs.Insert, []interface{}{vs}, columns...)
}

func (thiz DB) InsertMulti(vs ...interface{}) (int64, error) {
	return thiz.Execute(structs.Insert, vs)
}
func (thiz DB) Delete(vs ...interface{}) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, structs.DefaultStruct{})
	}
	thiz.CloneIfDifferentRoutine()
	return thiz.Execute(structs.Delete, vs)
}
func (thiz DB) Execute(sqlType structs.SqlType, vs []interface{}, columns ...string) (int64, error) {
	thiz.CloneIfDifferentRoutine()
	genFunc := thiz.factory.GetSqlFunc(sqlType)
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
			thiz.model = structModel
			maps, er := structs.StructToMap(v, columns...)
			if er != nil {
				panic(er)
			}
			model := structs.TableModel{thiz.getTableName(), thiz.getUpdateColumns(), maps, thiz.getCnd(), thiz.orderBys, thiz.page}
			models = append(models, model)
		}
		sqlProtos := genFunc(models...)
		cc := int64(0)
		for _, sqlProto := range sqlProtos {
			if Debug {
				fmt.Println(sqlProto)
			}
			rs, er := thiz.execute(sqlProto.Sql, sqlProto.Data...)
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
func (this DB) execute(sql string, data ...interface{}) (sql.Result, error) {
	st, err := this.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return st.Exec(data...)
}

func (this DB) getTableName() string {
	if len(this.table) == 0 {
		return this.model.TableName
	}
	return this.table
}
func (this DB) getQueryColumns() []string {
	if len(this.cols) > 0 {
		return this.cols
	}
	return this.model.ColumnNames
}

func (this DB) getUpdateColumns() []string {
	var cols []string
	if len(this.cols) > 0 {
		cols = this.cols
	} else {
		cols = this.model.ColumnNames
	}
	//del primary key
	primary := this.model.Primary.ColumnName
	var dst []string
	for _, k := range cols {
		if !strings.EqualFold(k, primary) {
			dst = append(dst, k)
		}
	}
	return dst
}
func (this DB) getCnd() structs.Condition {
	if this.cnd != nil {
		return this.cnd
	}
	return this.model.GetPrimaryCondition()
}
func (this DB) getInsertColumns(model structs.StructModel) []string {
	var cols []string
	if len(this.cols) > 0 {
		cols = this.cols
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

func (this DB) query(sql string, data []interface{}, model structs.StructModel) (interface{}, error) {
	st, err := this.db.Prepare(sql)
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

func (this DB) Transaction(work TransactionWork) (int64, error) {
	result := int64(0)
	tx := this.tx
	if tx == nil { //if transaction is nil create it
		var err error
		tx, err = this.db.Begin()
		if err != nil {
			return result, err
		}
	}

	result, err := work(&this)
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
