package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/kmlixh/gom/v4/define"
	dberrors "github.com/kmlixh/gom/v4/errors"
)

type Chain struct {
	id       int64
	factory  define.SqlFactory
	db       *sql.DB
	cnd      define.Condition
	table    *string
	rawSql   *string
	rawData  []any
	tx       *sql.Tx
	orderBys *[]define.OrderBy
	page     define.PageInfo
	dataMap  map[string]any
	fields   []string // 允许操作的列名
	rawMeta  any
}

func (chain *Chain) Table(table string) *Chain {
	chain.table = &table
	return chain
}
func (chain *Chain) GetTable() string {
	if chain.table != nil {
		return *chain.table
	}
	return ""
}
func (chain *Chain) Clone() *Chain {
	return &Chain{id: getGrouteId(), factory: chain.factory, db: chain.db, cnd: define.CndEmpty()}
}
func (chain *Chain) cloneSelfIfDifferentGoRoutine() {
	if chain.id != getGrouteId() {
		chain = chain.Clone()
	}
}
func (chain *Chain) RawSql(sql string, datas ...any) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	chain.rawSql = &sql
	var temp = define.UnZipSlice(datas)
	chain.rawData = temp
	return chain
}

func (chain *Chain) OrderBy(field string, t define.OrderType) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, MakeOrderBy(field, t))
	chain.orderBys = &temp
	return chain
}
func (chain *Chain) OrderBys(orderbys []define.OrderBy) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, orderbys...)
	chain.orderBys = &temp
	return chain
}
func (chain *Chain) CleanOrders() *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	temp := make([]define.OrderBy, 0)
	chain.orderBys = &temp
	return chain
}
func (chain *Chain) OrderByAsc(field string) *Chain {
	return chain.OrderBy(field, define.Asc)
}
func (chain *Chain) OrderByDesc(field string) *Chain {
	return chain.OrderBy(field, define.Desc)
}

func (chain *Chain) Where2(sql string, patches ...interface{}) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	return chain.Where(define.CndRaw(sql, patches...))
}
func (chain *Chain) Where(cnd define.Condition) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	chain.cnd.And2(cnd)
	return chain
}

func (chain *Chain) Page(page int64, pageSize int64) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	pages := MakePage(page, pageSize)
	chain.page = pages
	return chain
}

func (chain *Chain) Count(columnName string) define.Result {
	statements := fmt.Sprintf("select count(%s) as count from %s", columnName, chain.GetTable())
	var data []interface{}
	if chain.cnd != nil && chain.cnd.PayLoads() > 0 {
		cndString, cndData := chain.factory.ConditionToSql(false, chain.cnd)
		if cndString != "" {
			data = append(data, cndData...)
			statements = statements + " WHERE " + cndString
		}
	}
	var count int64 = 0
	scanners, er := define.GetDefaultScanner(&count)
	if er != nil {
		return define.ErrorResult(er)
	}
	result := chain.execute(define.NewSqlProto(statements, data, scanners))

	return result
}

func (chain *Chain) Sum(columnName string) define.Result {
	statements := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, chain.GetTable())
	var data []interface{}
	if chain.cnd != nil && chain.cnd.PayLoads() > 0 {
		cndString, cndData := chain.factory.ConditionToSql(false, chain.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64 = 0
	scanners, er := define.GetDefaultScanner(&count)
	if er != nil {
		return define.ErrorResult(er)
	}
	result := chain.execute(define.NewSqlProto(statements, data, scanners))
	return result
}
func (chain *Chain) From(vs any) *Chain {
	if _, ok := vs.(string); ok {
		chain.Table(vs.(string))
	} else {
		chain.rawMeta = vs
	}
	return chain

}
func (chain *Chain) Set(name string, val interface{}) *Chain {
	chain.dataMap[name] = val
	return chain
}

func (chain *Chain) Select(vt ...any) define.Result {
	chain.cloneSelfIfDifferentGoRoutine()
	var vs any
	if len(vt) > 1 {
		return define.ErrorResult(errors.New("data can't large then one"))
	} else if len(vt) == 1 {
		vs = vt[0]
	} else if chain.rawMeta != nil {
		vs = chain.rawMeta
	} else if chain.table != nil && len(*chain.table) > 0 {
		temp := make([]map[string]any, 0)
		vs = &temp
	}
	scanner, er := define.GetDefaultScanner(vs, chain.fields...)
	if er != nil {
		return define.ErrorResult(er)
	}
	if chain.rawSql != nil && len(*chain.rawSql) > 0 {
		return chain.execute(define.NewSqlProto(*chain.rawSql, chain.rawData, scanner))
	} else {
		rawInfo := define.GetRawTableInfo(vs)
		if rawInfo.IsStruct {
			//检查列缺失
			colMap, cols := define.GetDefaultsColumnFieldMap(rawInfo.Type)
			if len(chain.fields) > 0 {
				for _, c := range chain.fields {
					if _, ok := colMap[c]; !ok {
						return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "Select", fmt.Errorf("'%s' not exist in variable", c), nil))
					}
				}
			}
			if chain.fields == nil || len(chain.fields) == 0 {
				chain.fields = cols
			} else {
				chain.fields = define.ArrayIntersect(chain.fields, cols)
			}
		}

		table := chain.GetTable()
		if len(table) == 0 {
			table = rawInfo.TableName
		}
		cnd := chain.cnd

		model := &DefaultModel{
			table:         table,
			primaryKeys:   nil,
			columns:       chain.fields,
			columnDataMap: nil,
			condition:     cnd,
			orderBys:      chain.GetOrderBys(),
			page:          chain.GetPageInfo(),
			target:        vs,
		}
		selectFunc := chain.factory.GetSqlFunc(define.Query)
		sqlProtos := selectFunc(model)
		if er != nil {
			return define.ErrorResult(er)
		}
		defer chain.CleanDb()
		return chain.execute(sqlProtos[0])
	}
}
func (chain *Chain) First(vs ...interface{}) define.Result {
	chain.cloneSelfIfDifferentGoRoutine()
	return chain.Page(1, 1).Select(vs[0])
}
func (chain *Chain) Insert(v ...interface{}) define.Result {
	chain.cloneSelfIfDifferentGoRoutine()
	if len(v) > 1 {
		return define.ErrorResult(errors.New("data can't large then one"))
	}
	if len(v) == 1 {
		chain.rawMeta = v[0]
	}
	return chain.executeInside(define.Insert)
}
func (chain *Chain) Save(v ...interface{}) define.Result {
	return chain.Insert(v...)

}
func (chain *Chain) Delete(v ...interface{}) define.Result {
	chain.cloneSelfIfDifferentGoRoutine()
	if len(v) > 1 {
		return define.ErrorResult(errors.New("data can't large then one"))
	}
	if len(v) == 1 {
		chain.rawMeta = v[0]
	}
	return chain.executeInside(define.Delete)

}

func (chain *Chain) Update(v ...interface{}) define.Result {
	chain.cloneSelfIfDifferentGoRoutine()
	if len(v) > 1 {
		return define.ErrorResult(errors.New("data can't large then one"))
	}
	if len(v) == 1 {
		chain.rawMeta = v[0]
	}
	return chain.executeInside(define.Update)
}

func (chain *Chain) executeInside(sqlType define.SqlType) define.Result {
	if chain.rawSql != nil && len(*chain.rawSql) > 0 {
		if chain.rawMeta != nil {
			return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "executeInside", fmt.Errorf("when the RawSql is not nil or empty, data should be nil"), nil))
		}
		return chain.Raw(nil, *chain.rawSql, chain.rawData...)
	}
	if chain.rawMeta == nil && len(chain.dataMap) == 0 {
		return define.ErrorResult(errors.New("data was null"))
	} else if len(chain.dataMap) == 0 && chain.rawMeta != nil {
		dataMap, er := define.StructToMap(chain.rawMeta)
		if er != nil {
			return define.ErrorResult(er)
		}
		chain.dataMap = dataMap
	}
	table := chain.GetTable()
	rawInfo := define.GetRawTableInfo(chain.rawMeta)
	if len(table) == 0 {
		if chain.rawMeta == nil {
			return define.ErrorResult(errors.New("can't get table Name"))
		}
		table = rawInfo.TableName
	}
	colMap, _ := define.GetDefaultsColumnFieldMap(rawInfo.Type)
	dbCols, er := chain.factory.GetColumns(table, chain.db)
	if er != nil {
		return define.ErrorResult(er)
	}
	primaryKey := make([]string, 0)
	primaryAuto := make([]string, 0)
	dbColMap := make(map[string]define.Column)
	dbColNames := make([]string, 0)
	for _, dbCol := range dbCols {
		dbColNames = append(dbColNames, dbCol.ColumnName)
		dbColMap[dbCol.ColumnName] = dbCol
		if _, ok := colMap[dbCol.ColumnName]; !ok && dbCol.IsPrimary {
			return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "validateColumns", fmt.Errorf("column '%s' not exist in variable", dbCol.ColumnName), nil))
		}
		if dbCol.IsPrimary && !dbCol.IsPrimaryAuto {
			primaryKey = append(primaryKey, dbCol.ColumnName)
		}
		if dbCol.IsPrimaryAuto {
			primaryAuto = append(primaryAuto, dbCol.ColumnName)
		}
	}
	columns := chain.fields
	if len(columns) > 0 {
		for _, c := range columns {
			if _, ok := colMap[c]; !ok {
				return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "validateColumns", fmt.Errorf("'%s' not exist in variable", c), nil))
			}
			if _, ok := dbColMap[c]; !ok {
				return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "validateColumns", fmt.Errorf("'%s' not exist in table '%s'", c, table), nil))
			}
		}
	}
	if len(columns) > 0 {
		columns = append(primaryKey, append(primaryAuto, columns...)...)
	}

	if er != nil {
		return define.ErrorResult(er)
	}
	dataCol := make([]string, 0)
	for key, _ := range chain.dataMap {
		dataCol = append(dataCol, key)
	}
	columns = define.ArrayIntersect(dbColNames, dataCol)

	// 如果设置了允许的字段列表,则取交集
	if len(chain.fields) > 0 {
		columns = define.ArrayIntersect(columns, chain.fields)
	}
	if chain.cnd.IsEmpty() && (sqlType == define.Update || sqlType == define.Delete) {
		primaryMap := make(map[string]interface{})
		for _, key := range append(primaryKey, primaryAuto...) {
			primaryMap[key] = chain.dataMap[key]
		}
		chain.cnd = define.MapToCondition(primaryMap)
	}

	dm := &DefaultModel{
		table:         table,
		primaryKeys:   append(primaryKey, primaryAuto...),
		primaryAuto:   primaryAuto,
		columns:       columns,
		columnDataMap: chain.dataMap,
		condition:     chain.cnd,
		orderBys:      chain.GetOrderBys(),
		page:          chain.GetPageInfo(),
		target:        chain.rawMeta,
	}

	var lastInsertId = int64(0)
	genFunc := chain.factory.GetSqlFunc(sqlType)
	//此处应当判断是否已经在事物中，如果不在事务中才开启事物
	count := int64(0)
	sqlProtos := genFunc(dm)
	for _, sqlProto := range sqlProtos {
		if define.Debug {
			fmt.Println(sqlProto)
		}
		rs := chain.execute(sqlProto)
		if rs.Error() != nil {
			return rs
		}
		cs := rs.RowsAffected()
		id := rs.LastInsertId()
		lastInsertId = id
		count += cs
	}
	defer chain.CleanDb()
	return define.NewResult(lastInsertId, count, nil, nil)
}
func (chain *Chain) Raw(scanner define.IRowScanner, rawSql string, datas ...any) define.Result {
	return chain.execute(define.NewSqlProto(rawSql, datas, scanner))
}

func (chain *Chain) prepare(query string) (*sql.Stmt, error) {
	if chain.IsInTransaction() {
		st, er := chain.tx.Prepare(query)
		if er != nil {
			chain.Rollback()
		}
		return st, er
	}
	return chain.db.Prepare(query)
}

func (chain *Chain) GetCondition() define.Condition {
	if chain.cnd != nil {
		return chain.cnd
	}
	return nil
}

func (chain *Chain) execute(sqlProto define.SqlProto) define.Result {
	if define.Debug {
		fmt.Println("execute sql:", sqlProto.PreparedSql, "data:", sqlProto.Data)
	}
	var err error
	var st *sql.Stmt
	if chain.tx != nil {
		st, err = chain.tx.Prepare(sqlProto.PreparedSql)
	} else {
		st, err = chain.db.Prepare(sqlProto.PreparedSql)
	}

	defer func(st *sql.Stmt, err error) {
		if err == nil {
			st.Close()
		}
	}(st, err)
	if err != nil {
		return define.ErrorResult(err)
	}
	if sqlProto.Scanner != nil {
		rows, errs := st.Query(sqlProto.Data...)
		if errs != nil {
			return define.ErrorResult(errs)
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
			}

		}(rows)

		result := sqlProto.Scanner.Scan(rows)
		return result
	} else {
		rs, er := st.Exec(sqlProto.Data...)
		if er != nil {
			return define.ErrorResult(er)
		}
		lastInsertId, _ := rs.LastInsertId()
		rowsEffect, _ := rs.RowsAffected()
		defer chain.CleanDb()
		return define.NewResult(lastInsertId, rowsEffect, nil, nil)
	}
}
func (chain *Chain) Begin() error {
	if chain.tx != nil {
		return dberrors.New(dberrors.ErrCodeTransaction, "Begin", fmt.Errorf("there was a transaction"), nil)
	}
	tx, err := chain.db.Begin()
	chain.tx = tx
	return err
}
func (chain *Chain) IsInTransaction() bool {
	return chain.tx != nil
}
func (chain *Chain) Commit() {
	if chain.IsInTransaction() {
		err := chain.tx.Commit()
		if err != nil {
			panic(err)
		}
		chain.tx = nil
	}
}
func (chain *Chain) Rollback() {
	if chain.tx != nil {
		err := chain.tx.Rollback()
		if err != nil {
			panic(err)
		}
		chain.tx = nil
	}
}

type TransactionWork func(databaseTx *Chain) (interface{}, error)

func (chain *Chain) DoTransaction(work TransactionWork) (interface{}, error) {
	//Create A New Db And set Tx for it
	dbTx := chain.Clone()
	eb := dbTx.Begin()
	if eb != nil {
		return nil, eb
	}
	defer func(dbTx *Chain) {
		//catch the panic of 'work' function
		r := recover()
		if r != nil {
			dbTx.Rollback()
		}
	}(dbTx)
	i, es := work(dbTx)
	if es != nil {
		dbTx.Rollback()
	} else {
		dbTx.Commit()
	}
	return i, es
}

func (chain *Chain) GetOrderBys() []define.OrderBy {
	if chain.orderBys != nil && *chain.orderBys != nil {
		return *chain.orderBys
	}
	return nil
}

func (chain *Chain) GetPageInfo() define.PageInfo {
	if chain.page != nil {
		return chain.page
	}
	return nil
}
func (chain *Chain) GetPage() (int64, int64) {
	if chain.page != nil {
		return chain.page.Page()
	}
	return 0, 0
}
func (chain *Chain) CleanDb() *Chain {
	chain.table = nil
	chain.page = nil
	chain.fields = nil
	chain.orderBys = nil
	chain.rawSql = nil
	chain.dataMap = nil
	chain.cnd = define.CndEmpty()
	return chain
}
func (chain *Chain) Eq(field string, values interface{}) *Chain {

	chain.cnd.Eq(field, values)
	return chain
}
func (chain *Chain) EqBool(b bool, field string, value interface{}) *Chain {

	chain.cnd.EqBool(b, field, value)
	return chain
}
func (chain *Chain) OrEq(field string, value interface{}) *Chain {

	chain.cnd.OrEq(field, value)
	return chain
}
func (chain *Chain) OrEqBool(b bool, field string, value interface{}) *Chain {

	chain.cnd.OrEqBool(b, field, value)
	return chain
}
func (chain *Chain) Ge(field string, value interface{}) *Chain {

	chain.cnd.Ge(field, value)
	return chain
}
func (chain *Chain) GeBool(b bool, field string, value interface{}) *Chain {

	chain.cnd.GeBool(b, field, value)
	return chain
}
func (chain *Chain) OrGe(field string, value interface{}) *Chain {

	chain.cnd.OrGe(field, value)
	return chain
}
func (chain *Chain) OrGeBool(b bool, field string, value interface{}) *Chain {

	chain.cnd.OrGeBool(b, field, value)
	return chain
}
func (chain *Chain) Gt(field string, values interface{}) *Chain {

	chain.cnd.Gt(field, values)
	return chain
}
func (chain *Chain) GtBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.GtBool(b, field, values)
	return chain
}
func (chain *Chain) OrGt(field string, values interface{}) *Chain {

	chain.cnd.OrGt(field, values)
	return chain
}
func (chain *Chain) OrGtBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrGtBool(b, field, values)
	return chain
}
func (chain *Chain) Le(field string, values interface{}) *Chain {

	chain.cnd.Le(field, values)
	return chain
}
func (chain *Chain) LeBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.LeBool(b, field, values)
	return chain
}
func (chain *Chain) OrLe(field string, values interface{}) *Chain {

	chain.cnd.OrLe(field, values)
	return chain
}
func (chain *Chain) OrLeBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrLeBool(b, field, values)
	return chain
}
func (chain *Chain) Lt(field string, values interface{}) *Chain {

	chain.cnd.Lt(field, values)
	return chain
}
func (chain *Chain) LtBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.LtBool(b, field, values)
	return chain
}
func (chain *Chain) OrLt(field string, values interface{}) *Chain {

	chain.cnd.OrLt(field, values)
	return chain
}
func (chain *Chain) OrLtBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrLtBool(b, field, values)
	return chain
}
func (chain *Chain) NotEq(field string, values interface{}) *Chain {

	chain.cnd.NotEq(field, values)
	return chain
}
func (chain *Chain) NotEqBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.NotEqBool(b, field, values)
	return chain
}
func (chain *Chain) OrNotEq(field string, values interface{}) *Chain {

	chain.cnd.OrNotEq(field, values)
	return chain
}
func (chain *Chain) OrNotEqBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrNotEqBool(b, field, values)
	return chain
}
func (chain *Chain) In(field string, values ...interface{}) *Chain {

	chain.cnd.In(field, values...)
	return chain
}
func (chain *Chain) InBool(b bool, field string, values ...interface{}) *Chain {

	chain.cnd.InBool(b, field, values...)
	return chain
}
func (chain *Chain) OrIn(field string, values ...interface{}) *Chain {

	chain.cnd.OrIn(field, values...)
	return chain
}
func (chain *Chain) OrInBool(b bool, field string, values ...interface{}) *Chain {

	chain.cnd.OrInBool(b, field, values...)
	return chain
}
func (chain *Chain) NotIn(field string, values ...interface{}) *Chain {

	chain.cnd.NotIn(field, values...)
	return chain
}
func (chain *Chain) NotInBool(b bool, field string, values ...interface{}) *Chain {

	chain.cnd.NotInBool(b, field, values...)
	return chain
}
func (chain *Chain) OrNotIn(field string, values ...interface{}) *Chain {

	chain.cnd.OrNotIn(field, values...)
	return chain
}
func (chain *Chain) OrNotInBool(b bool, field string, values ...interface{}) *Chain {

	chain.cnd.OrNotInBool(b, field, values...)
	return chain
}
func (chain *Chain) Like(field string, values interface{}) *Chain {

	chain.cnd.Like(field, values)
	return chain
}
func (chain *Chain) LikeBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.LikeBool(b, field, values)
	return chain
}
func (chain *Chain) OrLike(field string, values interface{}) *Chain {

	chain.cnd.OrLike(field, values)
	return chain
}
func (chain *Chain) OrLikeBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrLikeBool(b, field, values)
	return chain
}
func (chain *Chain) NotLike(field string, values interface{}) *Chain {

	chain.cnd.NotLike(field, values)
	return chain
}
func (chain *Chain) NotLikeBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.NotLikeBool(b, field, values)
	return chain
}
func (chain *Chain) OrNotLike(field string, values interface{}) *Chain {

	chain.cnd.OrNotLike(field, values)
	return chain
}
func (chain *Chain) OrNotLikeBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrNotLikeBool(b, field, values)
	return chain
}
func (chain *Chain) LikeIgnoreStart(field string, values interface{}) *Chain {

	chain.cnd.LikeIgnoreStart(field, values)
	return chain
}
func (chain *Chain) LikeIgnoreStartBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.LikeIgnoreStartBool(b, field, values)
	return chain
}
func (chain *Chain) OrLikeIgnoreStart(field string, values interface{}) *Chain {

	chain.cnd.OrLikeIgnoreStart(field, values)
	return chain
}
func (chain *Chain) OrLikeIgnoreStartBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrLikeIgnoreStartBool(b, field, values)
	return chain
}
func (chain *Chain) LikeIgnoreEnd(field string, values interface{}) *Chain {

	chain.cnd.LikeIgnoreEnd(field, values)
	return chain
}
func (chain *Chain) LikeIgnoreEndBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.LikeIgnoreEndBool(b, field, values)
	return chain
}
func (chain *Chain) OrLikeIgnoreEnd(field string, values interface{}) *Chain {

	chain.cnd.OrLikeIgnoreEnd(field, values)
	return chain
}
func (chain *Chain) OrLikeIgnoreEndBool(b bool, field string, values interface{}) *Chain {

	chain.cnd.OrLikeIgnoreEndBool(b, field, values)
	return chain
}
func (chain *Chain) IsNull(filed string) *Chain {

	chain.cnd.IsNull(filed)
	return chain
}
func (chain *Chain) IsNullBool(b bool, field string) *Chain {

	chain.cnd.IsNullBool(b, field)
	return chain
}
func (chain *Chain) IsNotNull(field string) *Chain {

	chain.cnd.IsNotNull(field)
	return chain
}
func (chain *Chain) IsNotNullBool(b bool, field string) *Chain {

	chain.cnd.IsNotNullBool(b, field)
	return chain
}
func (chain *Chain) OrIsNull(filed string) *Chain {

	chain.cnd.OrIsNull(filed)
	return chain
}
func (chain *Chain) OrIsNullBool(b bool, field string) *Chain {

	chain.cnd.OrIsNullBool(b, field)
	return chain
}
func (chain *Chain) OrIsNotNull(field string) *Chain {

	chain.cnd.OrIsNotNull(field)
	return chain
}
func (chain *Chain) OrIsNotNullBool(b bool, field string) *Chain {

	chain.cnd.OrIsNotNullBool(b, field)
	return chain
}
func (chain *Chain) And(field string, operation define.Operation, value ...interface{}) *Chain {

	chain.cnd.And(field, operation, value...)
	return chain
}
func (chain *Chain) AndBool(b bool, field string, operation define.Operation, value ...interface{}) *Chain {

	chain.cnd.AndBool(b, field, operation, value...)
	return chain
}
func (chain *Chain) And2(condition define.Condition) *Chain {

	chain.cnd.And2(condition)
	return chain
}
func (chain *Chain) And3(rawExpresssion string, values ...interface{}) *Chain {

	chain.cnd.And3(rawExpresssion, values...)
	return chain
}
func (chain *Chain) And3Bool(b bool, rawExpresssion string, values ...interface{}) *Chain {

	chain.cnd.And3Bool(b, rawExpresssion, values...)
	return chain
}
func (chain *Chain) Or(field string, operation define.Operation, value ...interface{}) *Chain {

	chain.cnd.Or(field, operation, value...)
	return chain
}
func (chain *Chain) OrBool(b bool, field string, operation define.Operation, value ...interface{}) *Chain {

	chain.cnd.OrBool(b, field, operation, value...)
	return chain
}
func (chain *Chain) Or2(condition define.Condition) *Chain {

	chain.cnd.Or2(condition)
	return chain
}
func (chain *Chain) Or3(rawExpresssion string, values ...interface{}) *Chain {

	chain.cnd.Or3(rawExpresssion, values...)
	return chain
}
func (chain *Chain) Or3Bool(b bool, rawExpresssion string, values ...interface{}) *Chain {

	chain.cnd.Or3Bool(b, rawExpresssion, values...)
	return chain
}

// Fields 设置允许操作的列名
func (chain *Chain) Fields(columns ...string) *Chain {
	chain.cloneSelfIfDifferentGoRoutine()
	chain.fields = columns
	return chain
}

// validateFields 验证列名是否在允许的范围内
func (chain *Chain) validateFields(columns []string) error {
	if len(chain.fields) == 0 {
		return nil // 未设置fields时不做验证
	}

	allowedFields := make(map[string]bool)
	for _, field := range chain.fields {
		allowedFields[field] = true
	}

	for _, col := range columns {
		if !allowedFields[col] {
			return dberrors.New(dberrors.ErrCodeValidation, "ValidateFields", fmt.Errorf("column '%s' is not allowed to operate", col), nil)
		}
	}
	return nil
}
func getGrouteId() int64 {
	var (
		buf [64]byte
		n   = runtime.Stack(buf[:], false)
		stk = strings.TrimPrefix(string(buf[:n]), "goroutine ")
	)

	idField := strings.Fields(stk)[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Errorf("can not get goroutine id: %v", err))
	}

	return int64(id)
}
