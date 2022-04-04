package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"gom/structs"
	"reflect"
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
	db.CloneIfDifferentRoutine()
	var countResult structs.CountResult
	colName := fmt.Sprintf("COUNT(%s) as count", columnName)
	m := structs.StructModel{
		Type:        reflect.TypeOf(countResult),
		Value:       reflect.ValueOf(&countResult).Elem(),
		TableName:   "",
		ColumnNames: []string{colName},
		ColumnMap: map[string]structs.Column{"count": {
			Type:       reflect.TypeOf(int64(0)),
			ColumnName: "count",
			FieldName:  "Count",
			IsPrimary:  false,
			Auto:       false,
		}},
		Primary:         structs.Column{},
		HasColumnFilter: false,
		DataMap:         nil,
	} //Select(&countResult, "SUM("+columnName+") as count")
	db.SelectByModel(m)
	return countResult
}

func (db DB) Sum(columnName string) structs.CountResult {
	db.CloneIfDifferentRoutine()
	var countResult structs.CountResult
	colName := fmt.Sprintf("SUM(%s) as count", columnName)
	m := structs.StructModel{
		Type:        reflect.TypeOf(countResult),
		Value:       reflect.ValueOf(&countResult).Elem(),
		TableName:   "",
		ColumnNames: []string{colName},
		ColumnMap: map[string]structs.Column{"count": {
			Type:       reflect.TypeOf(int64(0)),
			ColumnName: "count",
			FieldName:  "Count",
			IsPrimary:  false,
			Auto:       false,
		}},
		Primary:         structs.Column{},
		HasColumnFilter: false,
		DataMap:         nil,
	} //Select(&countResult, "SUM("+columnName+") as count")
	db.SelectByModel(m)
	return countResult
}

func (db DB) Select(vs interface{}, columns ...string) (interface{}, error) {
	db.CloneIfDifferentRoutine()
	model, er := structs.GetStructModel(vs, columns...)
	if er != nil {
		panic(er)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		return db.query(*db.rawSql, *db.rawData, model)
	} else {
		return db.SelectByModel(model)
	}
}
func (db DB) SelectByModel(model structs.StructModel) (interface{}, error) {
	//TODO 此处逻辑不合理，如果是自定义查询的话，无需生成Model，简单的查询也不需要生成model。
	db.CloneIfDifferentRoutine()
	selectFunc := db.factory.GetSqlFunc(structs.Query)
	sqlProtos := selectFunc(structs.TableModel{Table: db.getTableName(model), Columns: getQueryColumns(model), Condition: db.getCnd(), OrderBys: db.getOrderBys(), Page: db.getPage()})
	return db.query(sqlProtos[0].Sql, sqlProtos[0].Data, model)
}
func (db DB) First(vs interface{}) (interface{}, error) {
	return db.Page(0, 1).Select(vs)
}
func (db DB) Update(vs interface{}, columns ...string) (int64, int64, error) {
	_, _, slice := structs.GetType(vs)
	if slice && len(columns) > 0 {
		return -1, -1, errors.New("can't update slice or array,please use UpdateMulti")
	}
	db.CloneIfDifferentRoutine()
	return db.Execute(structs.Update, []interface{}{vs}, columns...)
}
func (db DB) UpdateMulti(vs ...interface{}) (int64, int64, error) {
	return db.Execute(structs.Update, vs)
}
func (db DB) Insert(vs interface{}, columns ...string) (int64, int64, error) {
	_, _, slice := structs.GetType(vs)
	if slice && len(columns) > 0 {
		return -1, -1, errors.New("can't Insert slice or array,please use UpdateMulti")
	}
	db.CloneIfDifferentRoutine()
	return db.Execute(structs.Insert, []interface{}{vs}, columns...)
}

func (db DB) InsertMulti(vs ...interface{}) (int64, int64, error) {
	return db.Execute(structs.Insert, vs)
}
func (db DB) Delete(vs ...interface{}) (int64, int64, error) {
	if len(vs) == 0 {
		vs = append(vs, structs.DefaultStruct{})
	}
	db.CloneIfDifferentRoutine()
	return db.Execute(structs.Delete, vs)
}
func (db DB) Execute(sqlType structs.SqlType, vs []interface{}, columns ...string) (int64, int64, error) {
	db.CloneIfDifferentRoutine()
	var lastInsertId = int64(0)
	genFunc := db.factory.GetSqlFunc(sqlType)
	//此处应当判断是否已经在事物中，如果不在事务中才开启事物
	count := int64(0)
	var vmap = structs.SliceToGroupSlice(vs)
	for i, v := range vmap {
		if Debug {
			fmt.Println("Model Type was:", i, "slice counts:", len(v))
		}
		var models []structs.TableModel
		for _, vv := range vmap[i] {
			structModel, er := structs.GetStructModel(vv, columns...)
			if er != nil {
				return 0, 0, er
			}
			models = append(models, db.genTableModel(sqlType, structModel))
		}
		sqlProtos := genFunc(models...)
		cc := int64(0)
		for _, sqlProto := range sqlProtos {
			if Debug {
				fmt.Println(sqlProto)
			}
			rs, er := db.execute(sqlProto.Sql, sqlProto.Data...)
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
			cc += cs
		}

		count += cc
	}
	return count, lastInsertId, nil
}
func (db DB) lastInsertId() int64 {
	result := int64(0)

	return result
}

func (db DB) genTableModel(sqlType structs.SqlType, sm structs.StructModel) structs.TableModel {

	//TODO 此处应当根据sql的类型生成对应类型的TableModel，如插入是所有，搜索是全部实体，而更新则需要将主键做条件，其他列作为更新值
	var cols []string
	switch sqlType {
	case structs.Query:
		cols = getQueryColumns(sm)
	case structs.Update:
		cols = getUpdateColumns(sm)
	case structs.Insert:
		cols = getInsertColumns(sm)
	case structs.Delete:
		cols = getDeleteColumns(sm)
	}
	maps, _, er := structs.ModelToMap(sm)
	if er != nil {
		panic(er)
	}
	cnd := db.getCnd()
	if cnd == nil {
		cnd = sm.GetPrimaryCondition()
		if cnd == nil {
			panic("primary key was nil")
		}
	}
	m := structs.TableModel{Table: db.getTableName(sm), Columns: cols, Data: maps, Condition: cnd, OrderBys: db.getOrderBys(), Page: db.getPage()}
	return m
}

func getDeleteColumns(model structs.StructModel, columns ...string) []string {

	if columns != nil && len(columns) > 0 {
		nn := structs.Intersect(model.ColumnNames, columns)
		nn = structs.Difference(nn, []string{model.Primary.ColumnName})
		return nn
	}
	return model.ColumnNames
}

func (db DB) execute(sql string, data ...interface{}) (sql.Result, error) {
	st, err := db.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	return st.Exec(data...)
}

func (db DB) getTableName(model structs.StructModel) string {
	if db.table == nil || len(*db.table) == 0 {
		return model.TableName
	}
	return *db.table
}
func getQueryColumns(model structs.StructModel, columns ...string) []string {

	if columns != nil && len(columns) > 0 {
		return structs.Intersect(model.ColumnNames, columns)
	}
	return model.ColumnNames
}

func getUpdateColumns(model structs.StructModel, columns ...string) []string {
	nn := model.ColumnNames
	if columns != nil && len(columns) > 0 {
		nn = structs.Intersect(nn, columns)
	}
	nn = structs.Difference(nn, []string{model.Primary.ColumnName})
	return nn
}
func (db DB) getCnd() structs.Condition {
	if db.cnd != nil && *db.cnd != nil {
		return *db.cnd
	}
	return nil
}
func getInsertColumns(model structs.StructModel, columns ...string) []string {
	var cols = model.ColumnNames
	if columns != nil && len(columns) > 0 {
		cols = structs.Intersect(cols, columns)
	}
	if model.Primary.Auto {
		//del primary key
		cols = structs.Difference(cols, []string{model.Primary.ColumnName})
	}
	return cols
}

func (db DB) query(statement string, data []interface{}, model structs.StructModel) (interface{}, error) {
	if Debug {
		fmt.Println("Execute query,Sql:", statement, "data was:", data)
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
	columns, er := rows.Columns()
	if er != nil {
		return nil, er
	}
	//columnTypes, er := rows.ColumnTypes()
	//for _, colType := range columnTypes {
	//	lens, lensOk := colType.Length()
	//	preciss, scales, decimalOk := colType.DecimalSize()
	//	fmt.Println(colType.Name(), colType.ScanType().String(), colType.DatabaseTypeName(), "length:", lens, lensOk, "decimal:", preciss, scales, decimalOk)
	//}
	transfer := structs.GetDataTransfer(structs.Md5Text(statement), columns, model)

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
