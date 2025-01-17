package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/kmlixh/gom/v4/define"
	dberrors "github.com/kmlixh/gom/v4/errors"
	"reflect"
	"runtime"
	"strconv"
	"strings"
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
	fields   []string // 允许操作的列名
	rawMeta  any
}

func (db *Chain) Table(table string) *Chain {
	db.table = &table
	return db
}
func (c *Chain) GetTable() string {
	if c.table != nil {
		return *c.table
	}
	return ""
}
func (db *Chain) Clone() *Chain {
	return &Chain{id: getGrouteId(), factory: db.factory, db: db.db, cnd: define.CndEmpty()}
}
func (db *Chain) cloneSelfIfDifferentGoRoutine() {
	if db.id != getGrouteId() {
		db = db.Clone()
	}
}
func (db *Chain) RawSql(sql string, datas ...any) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	db.rawSql = &sql
	var temp = define.UnZipSlice(datas)
	db.rawData = temp
	return db
}

func (db *Chain) OrderBy(field string, t define.OrderType) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, MakeOrderBy(field, t))
	db.orderBys = &temp
	return db
}
func (db *Chain) OrderBys(orderbys []define.OrderBy) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, orderbys...)
	db.orderBys = &temp
	return db
}
func (db *Chain) CleanOrders() *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	temp := make([]define.OrderBy, 0)
	db.orderBys = &temp
	return db
}
func (db *Chain) OrderByAsc(field string) *Chain {
	return db.OrderBy(field, define.Asc)
}
func (db *Chain) OrderByDesc(field string) *Chain {
	return db.OrderBy(field, define.Desc)
}

func (db *Chain) Where2(sql string, patches ...interface{}) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	return db.Where(define.CndRaw(sql, patches...))
}
func (db *Chain) Where(cnd define.Condition) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	db.cnd.And2(cnd)
	return db
}

func (db *Chain) Page(page int64, pageSize int64) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	pages := MakePage(page, pageSize)
	db.page = pages
	return db
}

func (db *Chain) Count(columnName string) define.Result {
	statements := fmt.Sprintf("select count(%s) as count from %s", columnName, db.GetTable())
	var data []interface{}
	if db.cnd != nil && db.cnd.PayLoads() > 0 {
		cndString, cndData := db.factory.ConditionToSql(false, db.cnd)
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
	result := db.execute(define.NewSqlProto(statements, data, scanners))

	return result
}

func (db *Chain) Sum(columnName string) define.Result {
	statements := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, db.GetTable())
	var data []interface{}
	if db.cnd != nil && db.cnd.PayLoads() > 0 {
		cndString, cndData := db.factory.ConditionToSql(false, db.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64 = 0
	scanners, er := define.GetDefaultScanner(&count)
	if er != nil {
		return define.ErrorResult(er)
	}
	result := db.execute(define.NewSqlProto(statements, data, scanners))
	return result
}
func (db *Chain) From(vs any) *Chain {
	if _, ok := vs.(string); ok {
		db.Table(vs.(string))
	} else {
		db.rawMeta = vs
	}
	return db

}

func (db *Chain) Select(vt ...any) define.Result {
	db.cloneSelfIfDifferentGoRoutine()
	var vs any
	if len(vt) > 1 {
		return define.ErrorResult(errors.New("data can't large then one"))
	} else if len(vt) == 1 {
		vs = vt[0]
	} else if db.rawMeta != nil {
		vs = db.rawMeta
	} else if db.table != nil && len(*db.table) > 0 {
		temp := make([]map[string]any, 0)
		vs = &temp
	}
	scanner, er := define.GetDefaultScanner(vs, db.fields...)
	if er != nil {
		return define.ErrorResult(er)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		return db.execute(define.NewSqlProto(*db.rawSql, db.rawData, scanner))
	} else {
		rawInfo := define.GetRawTableInfo(vs)
		if rawInfo.IsStruct {
			//检查列缺失
			colMap, cols := define.GetDefaultsColumnFieldMap(rawInfo.Type)
			if len(db.fields) > 0 {
				for _, c := range db.fields {
					if _, ok := colMap[c]; !ok {
						return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "Select", fmt.Errorf("'%s' not exist in variable", c), nil))
					}
				}
			}
			if db.fields == nil || len(db.fields) == 0 {
				db.fields = cols
			} else {
				db.fields = define.ArrayIntersect(db.fields, cols)
			}
		}

		table := db.GetTable()
		if len(table) == 0 {
			table = rawInfo.TableName
		}
		cnd := db.cnd

		model := &DefaultModel{
			table:         table,
			primaryKeys:   nil,
			columns:       db.fields,
			columnDataMap: nil,
			condition:     cnd,
			orderBys:      db.GetOrderBys(),
			page:          db.GetPageInfo(),
			target:        vs,
		}
		selectFunc := db.factory.GetSqlFunc(define.Query)
		sqlProtos := selectFunc(model)
		if er != nil {
			return define.ErrorResult(er)
		}
		defer db.CleanDb()
		return db.execute(sqlProtos[0])
	}
}
func (db *Chain) First(vs interface{}) define.Result {
	db.cloneSelfIfDifferentGoRoutine()
	return db.Page(0, 1).Select(vs)
}
func (db *Chain) Insert(v interface{}) define.Result {
	db.cloneSelfIfDifferentGoRoutine()
	return db.executeInside(define.Insert, define.ArrayOf(v))
}
func (db *Chain) Save(v interface{}) define.Result {
	return db.Insert(v)

}
func (db *Chain) Delete(vs ...interface{}) define.Result {
	db.cloneSelfIfDifferentGoRoutine()
	return db.executeInside(define.Delete, vs)

}

func (db *Chain) Update(v interface{}) define.Result {
	db.cloneSelfIfDifferentGoRoutine()
	return db.executeInside(define.Update, define.ArrayOf(v))
}

func (db *Chain) executeInside(sqlType define.SqlType, vi []interface{}) define.Result {
	var vs []interface{}
	if vi != nil && len(vi) > 0 {
		vs = append(vs, define.UnZipSlice(vi)...)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		if vs != nil && len(vs) > 0 {
			return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "executeInside", fmt.Errorf("when the RawSql is not nil or empty, data should be nil"), nil))
		}
		return db.Raw(nil, *db.rawSql, db.rawData...)
	}
	if len(vs) == 0 && db.table == nil && db.cnd == nil {
		return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "executeInside", fmt.Errorf("there was nothing to do"), nil))
	} else {
		var vvs []define.TableModel
		if vs != nil && len(vs) > 0 {
			for _, v := range vs {
				rawInfo := define.GetRawTableInfo(v)
				table := db.GetTable()
				if len(table) == 0 {
					table = rawInfo.TableName
				}
				//检查列缺失
				colMap, _ := define.GetDefaultsColumnFieldMap(rawInfo.Type)
				dbCols, er := db.factory.GetColumns(table, db.db)
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
				columns := db.fields
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
				var cnd define.Condition
				cnd = db.GetCondition()
				dataMap, er := define.StructToMap(v, columns...)
				if er != nil {
					return define.ErrorResult(er)
				}
				dataCol := make([]string, 0)
				for key, _ := range dataMap {
					dataCol = append(dataCol, key)
				}
				columns = define.ArrayIntersect(dbColNames, dataCol)

				// 如果设置了允许的字段列表,则取交集
				if len(db.fields) > 0 {
					columns = define.ArrayIntersect(columns, db.fields)
				}

				if sqlType == define.Update {
					if cnd == nil {
						prs := append(primaryKey, primaryAuto...)
						if len(prs) == 0 {
							return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "validatePrimaryKey", fmt.Errorf("can't find primary Key"), nil))
						}
						cndMap := make(map[string]interface{})
						for _, key := range prs {
							data, ok := dataMap[key]
							if !ok {
								return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "validatePrimaryKey", fmt.Errorf("can't find data for primary Key '%s'", key), nil))
							}
							if reflect.ValueOf(data).IsZero() {
								return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "validatePrimaryKey", fmt.Errorf("value of Key '%s' can't be nil or empty", key), nil))
							}
							cndMap[key] = data
						}
						cnd = define.MapToCondition(cndMap)
					}

					columns, _, _ = define.ArrayIntersect2(columns, append(primaryKey, primaryAuto...))
				} else if sqlType == define.Delete && cnd == nil {
					cnd = define.MapToCondition(dataMap)
					columns = make([]string, 0)
				}

				if sqlType == define.Insert {
					columns = define.ArrayIntersect(dbColNames, columns)
					if len(primaryAuto) > 0 {
						columns, _, _ = define.ArrayIntersect2(columns, primaryAuto)
					}
				}

				dm := &DefaultModel{
					table:         table,
					primaryKeys:   append(primaryKey, primaryAuto...),
					primaryAuto:   primaryAuto,
					columns:       columns,
					columnDataMap: dataMap,
					condition:     cnd,
					orderBys:      db.GetOrderBys(),
					page:          db.GetPageInfo(),
					target:        v,
				}

				if sqlType == define.Update && cnd == nil {
					return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation, "executeInside", fmt.Errorf("can't update Database without Conditions"), nil))
				}
				vvs = append(vvs, dm)
			}
		} else if sqlType == define.Delete && db.GetTable() != "" && db.GetCondition() != nil {
			dm := &DefaultModel{
				table:         db.GetTable(),
				primaryKeys:   nil,
				columns:       nil,
				columnDataMap: nil,
				condition:     db.GetCondition(),
				orderBys:      db.GetOrderBys(),
				page:          db.GetPageInfo(),
			}
			vvs = append(vvs, dm)
		}
		var lastInsertId = int64(0)
		genFunc := db.factory.GetSqlFunc(sqlType)
		//此处应当判断是否已经在事物中，如果不在事务中才开启事物
		count := int64(0)
		sqlProtos := genFunc(vvs...)
		for _, sqlProto := range sqlProtos {
			if define.Debug {
				fmt.Println(sqlProto)
			}
			rs := db.execute(sqlProto)
			if rs.Error() != nil {
				return rs
			}
			cs := rs.RowsAffected()
			if cs == 1 && len(sqlProtos) == len(vvs) && sqlType == define.Insert {
				//
				id := rs.LastInsertId()
				lastInsertId = id
			}
			count += cs
		}
		defer db.CleanDb()
		return define.NewResult(lastInsertId, count, nil, nil)
	}
}
func (db *Chain) Raw(scanner define.IRowScanner, rawSql string, datas ...any) define.Result {
	return db.execute(define.NewSqlProto(rawSql, datas, scanner))
}

func (db *Chain) prepare(query string) (*sql.Stmt, error) {
	if db.IsInTransaction() {
		st, er := db.tx.Prepare(query)
		if er != nil {
			db.Rollback()
		}
		return st, er
	}
	return db.db.Prepare(query)
}

func (db *Chain) GetCondition() define.Condition {
	if db.cnd != nil {
		return db.cnd
	}
	return nil
}

func (db *Chain) execute(sqlProto define.SqlProto) define.Result {
	if define.Debug {
		fmt.Println("execute sql:", sqlProto.PreparedSql, "data:", sqlProto.Data)
	}
	var err error
	var st *sql.Stmt
	if db.tx != nil {
		st, err = db.tx.Prepare(sqlProto.PreparedSql)
	} else {
		st, err = db.db.Prepare(sqlProto.PreparedSql)
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
		defer db.CleanDb()
		return define.NewResult(lastInsertId, rowsEffect, nil, nil)
	}
}
func (db *Chain) Begin() error {
	if db.tx != nil {
		return dberrors.New(dberrors.ErrCodeTransaction, "Begin", fmt.Errorf("there was a transaction"), nil)
	}
	tx, err := db.db.Begin()
	db.tx = tx
	return err
}
func (db *Chain) IsInTransaction() bool {
	return db.tx != nil
}
func (db *Chain) Commit() {
	if db.IsInTransaction() {
		err := db.tx.Commit()
		if err != nil {
			panic(err)
		}
		db.tx = nil
	}
}
func (db *Chain) Rollback() {
	if db.tx != nil {
		err := db.tx.Rollback()
		if err != nil {
			panic(err)
		}
		db.tx = nil
	}
}

type TransactionWork func(databaseTx *Chain) (interface{}, error)

func (db *Chain) DoTransaction(work TransactionWork) (interface{}, error) {
	//Create A New Db And set Tx for it
	dbTx := db.Clone()
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

func (db *Chain) GetOrderBys() []define.OrderBy {
	if db.orderBys != nil && *db.orderBys != nil {
		return *db.orderBys
	}
	return nil
}

func (db *Chain) GetPageInfo() define.PageInfo {
	if db.page != nil {
		return db.page
	}
	return nil
}
func (db *Chain) GetPage() (int64, int64) {
	if db.page != nil {
		return db.page.Page()
	}
	return 0, 0
}
func (db *Chain) CleanDb() *Chain {
	db.table = nil
	db.page = nil
	db.fields = nil
	db.orderBys = nil
	db.rawSql = nil
	db.cnd = define.CndEmpty()
	return db
}
func (c *Chain) Eq(field string, values interface{}) *Chain {

	c.cnd.Eq(field, values)
	return c
}
func (c *Chain) EqBool(b bool, field string, value interface{}) *Chain {

	c.cnd.EqBool(b, field, value)
	return c
}
func (c *Chain) OrEq(field string, value interface{}) *Chain {

	c.cnd.OrEq(field, value)
	return c
}
func (c *Chain) OrEqBool(b bool, field string, value interface{}) *Chain {

	c.cnd.OrEqBool(b, field, value)
	return c
}
func (c *Chain) Ge(field string, value interface{}) *Chain {

	c.cnd.Ge(field, value)
	return c
}
func (c *Chain) GeBool(b bool, field string, value interface{}) *Chain {

	c.cnd.GeBool(b, field, value)
	return c
}
func (c *Chain) OrGe(field string, value interface{}) *Chain {

	c.cnd.OrGe(field, value)
	return c
}
func (c *Chain) OrGeBool(b bool, field string, value interface{}) *Chain {

	c.cnd.OrGeBool(b, field, value)
	return c
}
func (c *Chain) Gt(field string, values interface{}) *Chain {

	c.cnd.Gt(field, values)
	return c
}
func (c *Chain) GtBool(b bool, field string, values interface{}) *Chain {

	c.cnd.GtBool(b, field, values)
	return c
}
func (c *Chain) OrGt(field string, values interface{}) *Chain {

	c.cnd.OrGt(field, values)
	return c
}
func (c *Chain) OrGtBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrGtBool(b, field, values)
	return c
}
func (c *Chain) Le(field string, values interface{}) *Chain {

	c.cnd.Le(field, values)
	return c
}
func (c *Chain) LeBool(b bool, field string, values interface{}) *Chain {

	c.cnd.LeBool(b, field, values)
	return c
}
func (c *Chain) OrLe(field string, values interface{}) *Chain {

	c.cnd.OrLe(field, values)
	return c
}
func (c *Chain) OrLeBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrLeBool(b, field, values)
	return c
}
func (c *Chain) Lt(field string, values interface{}) *Chain {

	c.cnd.Lt(field, values)
	return c
}
func (c *Chain) LtBool(b bool, field string, values interface{}) *Chain {

	c.cnd.LtBool(b, field, values)
	return c
}
func (c *Chain) OrLt(field string, values interface{}) *Chain {

	c.cnd.OrLt(field, values)
	return c
}
func (c *Chain) OrLtBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrLtBool(b, field, values)
	return c
}
func (c *Chain) NotEq(field string, values interface{}) *Chain {

	c.cnd.NotEq(field, values)
	return c
}
func (c *Chain) NotEqBool(b bool, field string, values interface{}) *Chain {

	c.cnd.NotEqBool(b, field, values)
	return c
}
func (c *Chain) OrNotEq(field string, values interface{}) *Chain {

	c.cnd.OrNotEq(field, values)
	return c
}
func (c *Chain) OrNotEqBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrNotEqBool(b, field, values)
	return c
}
func (c *Chain) In(field string, values ...interface{}) *Chain {

	c.cnd.In(field, values...)
	return c
}
func (c *Chain) InBool(b bool, field string, values ...interface{}) *Chain {

	c.cnd.InBool(b, field, values...)
	return c
}
func (c *Chain) OrIn(field string, values ...interface{}) *Chain {

	c.cnd.OrIn(field, values...)
	return c
}
func (c *Chain) OrInBool(b bool, field string, values ...interface{}) *Chain {

	c.cnd.OrInBool(b, field, values...)
	return c
}
func (c *Chain) NotIn(field string, values ...interface{}) *Chain {

	c.cnd.NotIn(field, values...)
	return c
}
func (c *Chain) NotInBool(b bool, field string, values ...interface{}) *Chain {

	c.cnd.NotInBool(b, field, values...)
	return c
}
func (c *Chain) OrNotIn(field string, values ...interface{}) *Chain {

	c.cnd.OrNotIn(field, values...)
	return c
}
func (c *Chain) OrNotInBool(b bool, field string, values ...interface{}) *Chain {

	c.cnd.OrNotInBool(b, field, values...)
	return c
}
func (c *Chain) Like(field string, values interface{}) *Chain {

	c.cnd.Like(field, values)
	return c
}
func (c *Chain) LikeBool(b bool, field string, values interface{}) *Chain {

	c.cnd.LikeBool(b, field, values)
	return c
}
func (c *Chain) OrLike(field string, values interface{}) *Chain {

	c.cnd.OrLike(field, values)
	return c
}
func (c *Chain) OrLikeBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrLikeBool(b, field, values)
	return c
}
func (c *Chain) NotLike(field string, values interface{}) *Chain {

	c.cnd.NotLike(field, values)
	return c
}
func (c *Chain) NotLikeBool(b bool, field string, values interface{}) *Chain {

	c.cnd.NotLikeBool(b, field, values)
	return c
}
func (c *Chain) OrNotLike(field string, values interface{}) *Chain {

	c.cnd.OrNotLike(field, values)
	return c
}
func (c *Chain) OrNotLikeBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrNotLikeBool(b, field, values)
	return c
}
func (c *Chain) LikeIgnoreStart(field string, values interface{}) *Chain {

	c.cnd.LikeIgnoreStart(field, values)
	return c
}
func (c *Chain) LikeIgnoreStartBool(b bool, field string, values interface{}) *Chain {

	c.cnd.LikeIgnoreStartBool(b, field, values)
	return c
}
func (c *Chain) OrLikeIgnoreStart(field string, values interface{}) *Chain {

	c.cnd.OrLikeIgnoreStart(field, values)
	return c
}
func (c *Chain) OrLikeIgnoreStartBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrLikeIgnoreStartBool(b, field, values)
	return c
}
func (c *Chain) LikeIgnoreEnd(field string, values interface{}) *Chain {

	c.cnd.LikeIgnoreEnd(field, values)
	return c
}
func (c *Chain) LikeIgnoreEndBool(b bool, field string, values interface{}) *Chain {

	c.cnd.LikeIgnoreEndBool(b, field, values)
	return c
}
func (c *Chain) OrLikeIgnoreEnd(field string, values interface{}) *Chain {

	c.cnd.OrLikeIgnoreEnd(field, values)
	return c
}
func (c *Chain) OrLikeIgnoreEndBool(b bool, field string, values interface{}) *Chain {

	c.cnd.OrLikeIgnoreEndBool(b, field, values)
	return c
}
func (c *Chain) IsNull(filed string) *Chain {

	c.cnd.IsNull(filed)
	return c
}
func (c *Chain) IsNullBool(b bool, field string) *Chain {

	c.cnd.IsNullBool(b, field)
	return c
}
func (c *Chain) IsNotNull(field string) *Chain {

	c.cnd.IsNotNull(field)
	return c
}
func (c *Chain) IsNotNullBool(b bool, field string) *Chain {

	c.cnd.IsNotNullBool(b, field)
	return c
}
func (c *Chain) OrIsNull(filed string) *Chain {

	c.cnd.OrIsNull(filed)
	return c
}
func (c *Chain) OrIsNullBool(b bool, field string) *Chain {

	c.cnd.OrIsNullBool(b, field)
	return c
}
func (c *Chain) OrIsNotNull(field string) *Chain {

	c.cnd.OrIsNotNull(field)
	return c
}
func (c *Chain) OrIsNotNullBool(b bool, field string) *Chain {

	c.cnd.OrIsNotNullBool(b, field)
	return c
}
func (c *Chain) And(field string, operation define.Operation, value ...interface{}) *Chain {

	c.cnd.And(field, operation, value...)
	return c
}
func (c *Chain) AndBool(b bool, field string, operation define.Operation, value ...interface{}) *Chain {

	c.cnd.AndBool(b, field, operation, value...)
	return c
}
func (c *Chain) And2(condition define.Condition) *Chain {

	c.cnd.And2(condition)
	return c
}
func (c *Chain) And3(rawExpresssion string, values ...interface{}) *Chain {

	c.cnd.And3(rawExpresssion, values...)
	return c
}
func (c *Chain) And3Bool(b bool, rawExpresssion string, values ...interface{}) *Chain {

	c.cnd.And3Bool(b, rawExpresssion, values...)
	return c
}
func (c *Chain) Or(field string, operation define.Operation, value ...interface{}) *Chain {

	c.cnd.Or(field, operation, value...)
	return c
}
func (c *Chain) OrBool(b bool, field string, operation define.Operation, value ...interface{}) *Chain {

	c.cnd.OrBool(b, field, operation, value...)
	return c
}
func (c *Chain) Or2(condition define.Condition) *Chain {

	c.cnd.Or2(condition)
	return c
}
func (c *Chain) Or3(rawExpresssion string, values ...interface{}) *Chain {

	c.cnd.Or3(rawExpresssion, values...)
	return c
}
func (c *Chain) Or3Bool(b bool, rawExpresssion string, values ...interface{}) *Chain {

	c.cnd.Or3Bool(b, rawExpresssion, values...)
	return c
}

// Fields 设置允许操作的列名
func (db *Chain) Fields(columns ...string) *Chain {
	db.cloneSelfIfDifferentGoRoutine()
	db.fields = columns
	return db
}

// validateFields 验证列名是否在允许的范围内
func (db *Chain) validateFields(columns []string) error {
	if len(db.fields) == 0 {
		return nil // 未设置fields时不做验证
	}

	allowedFields := make(map[string]bool)
	for _, field := range db.fields {
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
