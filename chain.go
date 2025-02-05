package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/kmlixh/gom/v4/define"
	dberrors "github.com/kmlixh/gom/v4/errors"
)

type Chain struct {
	id              int64
	factory         define.SqlFactory
	db              *sql.DB
	cnd             define.Condition
	table           *string
	rawSql          *string
	rawData         []any
	tx              *sql.Tx
	orderBys        *[]define.OrderBy
	page            define.PageInfo
	dataMap         map[string]any
	fields          []string // 允许操作的列名
	rawMeta         any
	BatchInsertSize int // 批量插入时的分批大小，默认1000
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
	newChain := &Chain{
		id:      getGrouteId(),
		factory: chain.factory,
		db:      chain.db,
		cnd:     define.CndEmpty(),
		tx:      chain.tx, // 事务需要共享
	}

	// 深度复制必要的字段
	if chain.table != nil {
		tableCopy := *chain.table
		newChain.table = &tableCopy
	}

	if chain.rawSql != nil {
		sqlCopy := *chain.rawSql
		newChain.rawSql = &sqlCopy
	}

	if chain.rawData != nil {
		newChain.rawData = make([]any, len(chain.rawData))
		copy(newChain.rawData, chain.rawData)
	}

	if chain.orderBys != nil && *chain.orderBys != nil {
		orderByCopy := make([]define.OrderBy, len(*chain.orderBys))
		copy(orderByCopy, *chain.orderBys)
		newChain.orderBys = &orderByCopy
	}

	if chain.fields != nil {
		newChain.fields = make([]string, len(chain.fields))
		copy(newChain.fields, chain.fields)
	}

	// 延迟创建 dataMap
	if chain.dataMap != nil {
		newChain.dataMap = make(map[string]any, len(chain.dataMap))
		for k, v := range chain.dataMap {
			newChain.dataMap[k] = v
		}
	}

	return newChain
}
func (chain *Chain) cloneSelfIfDifferentGoRoutine() *Chain {
	if chain.id != getGrouteId() {
		chain = chain.Clone()
	}
	return chain
}
func (chain *Chain) RawSql(sql string, datas ...any) *Chain {
	chain = chain.cloneSelfIfDifferentGoRoutine()
	if strings.Contains(strings.ToUpper(sql), "DROP") ||
		strings.Contains(strings.ToUpper(sql), "TRUNCATE") {
		panic("potentially dangerous SQL operation detected")
	}
	chain.rawSql = &sql
	var temp = define.UnZipSlice(datas)
	chain.rawData = temp
	return chain
}

func (chain *Chain) OrderBy(field string, t define.OrderType) *Chain {
	chain = chain.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, MakeOrderBy(field, t))
	chain.orderBys = &temp
	return chain
}
func (chain *Chain) OrderBys(orderbys []define.OrderBy) *Chain {
	chain = chain.cloneSelfIfDifferentGoRoutine()
	if orderbys == nil {
		return chain
	}
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
	if cnd == nil {
		return chain
	}
	chain = chain.cloneSelfIfDifferentGoRoutine()
	if chain.cnd == nil {
		chain.cnd = define.CndEmpty()
	}
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
	chain = chain.cloneSelfIfDifferentGoRoutine()
	if len(v) == 0 {
		return define.ErrorResult(errors.New("no data provided for insert"))
	}

	// 处理单个值的情况
	if len(v) == 1 {
		value := v[0]
		// 检查是否为数组或切片
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		// 如果是数组或切片，进行批量插入
		if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
			return chain.batchInsert(val)
		}

		// 单个值的插入
		chain.rawMeta = value
		return chain.executeInside(define.Insert)
	}

	// 多个值的批量插入
	values := reflect.ValueOf(v)
	return chain.batchInsert(values)
}

// batchInsert 处理批量插入操作
func (chain *Chain) batchInsert(values reflect.Value) define.Result {
	length := values.Len()
	if length == 0 {
		return define.ErrorResult(errors.New("empty array/slice provided for batch insert"))
	}

	// 如果未设置BatchInsertSize，使用默认值1000
	if chain.BatchInsertSize <= 0 {
		chain.BatchInsertSize = 1000
	}

	// 计算需要分多少批次
	batchCount := (length + chain.BatchInsertSize - 1) / chain.BatchInsertSize
	var lastInsertId int64
	var totalAffected int64

	// 开启事务进行批量插入
	_, err := chain.DoTransaction(func(tx *Chain) (interface{}, error) {
		for i := 0; i < batchCount; i++ {
			start := i * chain.BatchInsertSize
			end := start + chain.BatchInsertSize
			if end > length {
				end = length
			}

			// 准备当前批次的数据
			batch := make([]interface{}, end-start)
			for j := start; j < end; j++ {
				batch[j-start] = values.Index(j).Interface()
			}

			// 构建批量插入的model
			table := chain.GetTable()
			if len(table) == 0 {
				if len(batch) > 0 {
					rawInfo := define.GetRawTableInfo(batch[0])
					table = rawInfo.TableName
				}
				if len(table) == 0 {
					return nil, fmt.Errorf("table name not provided")
				}
			}

			// 构建批量插入的model
			model := &DefaultModel{
				table:         table,
				columns:       chain.fields,
				columnDataMap: nil,
				condition:     nil,
				target:        batch,
				isBatch:       true,
				batchSize:     len(batch),
			}

			// 执行批量插入
			insertFunc := chain.factory.GetSqlFunc(define.Insert)
			sqlProtos := insertFunc(model)

			for _, sqlProto := range sqlProtos {
				result := tx.execute(sqlProto)
				if result.Error() != nil {
					return nil, result.Error()
				}

				if affected := result.RowsAffected(); affected > 0 {
					totalAffected += affected
					if id := result.LastInsertId(); id > 0 {
						lastInsertId = id
					}
				}
			}
		}
		return nil, nil
	})

	if err != nil {
		return define.ErrorResult(err)
	}

	return define.NewResult(lastInsertId, totalAffected, nil, nil)
}

func (chain *Chain) Save(v ...interface{}) define.Result {
	return chain.Insert(v...)

}
func (chain *Chain) Delete(v ...interface{}) define.Result {
	chain = chain.cloneSelfIfDifferentGoRoutine()
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
			return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
				"executeInside",
				fmt.Errorf("when using RawSql, data should be nil"),
				nil))
		}
		return chain.Raw(nil, *chain.rawSql, chain.rawData...)
	}

	if chain.rawMeta == nil && len(chain.dataMap) == 0 {
		return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
			"executeInside",
			fmt.Errorf("no data provided for %v operation", sqlType),
			nil))
	}

	if len(chain.dataMap) == 0 && chain.rawMeta != nil {
		dataMap, er := define.StructToMap(chain.rawMeta)
		if er != nil {
			return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
				"executeInside",
				fmt.Errorf("failed to convert struct to map: %w", er),
				nil))
		}
		chain.dataMap = dataMap
	}

	table := chain.GetTable()
	rawInfo := define.GetRawTableInfo(chain.rawMeta)
	if len(table) == 0 {
		if chain.rawMeta == nil {
			return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
				"executeInside",
				fmt.Errorf("table name not provided"),
				nil))
		}
		table = rawInfo.TableName
	}

	// 预分配切片容量
	dbCols, er := chain.factory.GetColumns(table, chain.db)
	if er != nil {
		return define.ErrorResult(er)
	}

	primaryKey := make([]string, 0, len(dbCols))
	primaryAuto := make([]string, 0, len(dbCols))
	dbColMap := make(map[string]define.Column, len(dbCols))
	dbColNames := make([]string, 0, len(dbCols))

	colMap, _ := define.GetDefaultsColumnFieldMap(rawInfo.Type)

	// 优化循环
	for _, dbCol := range dbCols {
		dbColNames = append(dbColNames, dbCol.ColumnName)
		dbColMap[dbCol.ColumnName] = dbCol

		if dbCol.IsPrimary {
			if _, ok := colMap[dbCol.ColumnName]; !ok {
				return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
					"validateColumns",
					fmt.Errorf("primary key column '%s' not exist in variable", dbCol.ColumnName),
					nil))
			}

			if !dbCol.IsPrimaryAuto {
				primaryKey = append(primaryKey, dbCol.ColumnName)
			} else {
				primaryAuto = append(primaryAuto, dbCol.ColumnName)
			}
		}
	}

	// 验证字段
	if len(chain.fields) > 0 {
		for _, c := range chain.fields {
			if _, ok := colMap[c]; !ok {
				return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
					"validateColumns",
					fmt.Errorf("field '%s' not exist in variable", c),
					nil))
			}
			if _, ok := dbColMap[c]; !ok {
				return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
					"validateColumns",
					fmt.Errorf("field '%s' not exist in table '%s'", c, table),
					nil))
			}
		}
	}

	// 优化字段处理
	columns := chain.fields
	if len(columns) > 0 {
		newColumns := make([]string, 0, len(primaryKey)+len(primaryAuto)+len(columns))
		newColumns = append(newColumns, primaryKey...)
		newColumns = append(newColumns, primaryAuto...)
		newColumns = append(newColumns, columns...)
		columns = newColumns
	}

	// 构建数据列
	dataCol := make([]string, 0, len(chain.dataMap))
	for key := range chain.dataMap {
		dataCol = append(dataCol, key)
	}
	columns = define.ArrayIntersect(dbColNames, dataCol)

	if len(chain.fields) > 0 {
		columns = define.ArrayIntersect(columns, chain.fields)
	}

	// 处理空条件
	if chain.cnd.IsEmpty() && (sqlType == define.Update || sqlType == define.Delete) {
		primaryMap := make(map[string]interface{}, len(primaryKey)+len(primaryAuto))
		for _, key := range append(primaryKey, primaryAuto...) {
			if val, ok := chain.dataMap[key]; !ok {
				return define.ErrorResult(dberrors.New(dberrors.ErrCodeValidation,
					"validateColumns",
					fmt.Errorf("primary key '%s' not provided for %v operation", key, sqlType),
					nil))
			} else {
				primaryMap[key] = val
			}
		}
		chain.cnd = define.MapToCondition(primaryMap)
	}

	// 构建模型
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

	// 执行SQL
	genFunc := chain.factory.GetSqlFunc(sqlType)
	sqlProtos := genFunc(dm)

	var lastInsertId int64
	var totalAffected int64

	for _, sqlProto := range sqlProtos {
		if define.Debug {
			fmt.Printf("Executing SQL: %s with data: %v\n", sqlProto.PreparedSql, sqlProto.Data)
		}

		rs := chain.execute(sqlProto)
		if rs.Error() != nil {
			return rs
		}

		if affected := rs.RowsAffected(); affected > 0 {
			totalAffected += affected
			if id := rs.LastInsertId(); id > 0 {
				lastInsertId = id
			}
		}
	}

	chain.CleanDb()
	return define.NewResult(lastInsertId, totalAffected, nil, nil)
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

	var st *sql.Stmt
	var err error

	if chain.tx != nil {
		st, err = chain.tx.Prepare(sqlProto.PreparedSql)
	} else {
		st, err = chain.db.Prepare(sqlProto.PreparedSql)
	}

	if err != nil {
		return define.ErrorResult(fmt.Errorf("prepare statement failed: %w", err))
	}

	if st != nil {
		defer func() {
			_ = st.Close()
		}()
	}

	if sqlProto.Scanner != nil {
		rows, err := st.Query(sqlProto.Data...)
		if err != nil {
			return define.ErrorResult(fmt.Errorf("query execution failed: %w", err))
		}

		defer func() {
			if rows != nil {
				if err := rows.Close(); err != nil {
					fmt.Printf("error closing rows: %v\n", err)
				}
			}
		}()

		result := sqlProto.Scanner.Scan(rows)
		if result.Error() != nil {
			return define.ErrorResult(fmt.Errorf("scan failed: %w", result.Error()))
		}
		return result
	}

	rs, err := st.Exec(sqlProto.Data...)
	if err != nil {
		return define.ErrorResult(fmt.Errorf("exec failed: %w", err))
	}

	lastInsertId, _ := rs.LastInsertId()
	rowsEffect, _ := rs.RowsAffected()

	defer chain.CleanDb()
	return define.NewResult(lastInsertId, rowsEffect, nil, nil)
}
func (chain *Chain) Begin() error {
	if chain.tx != nil {
		return fmt.Errorf("transaction already started")
	}
	tx, err := chain.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	chain.tx = tx
	return nil
}
func (chain *Chain) IsInTransaction() bool {
	return chain.tx != nil
}
func (chain *Chain) Commit() error {
	if !chain.IsInTransaction() {
		return fmt.Errorf("no active transaction")
	}
	err := chain.tx.Commit()
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	chain.tx = nil
	return nil
}
func (chain *Chain) Rollback() error {
	if !chain.IsInTransaction() {
		return fmt.Errorf("no active transaction")
	}
	err := chain.tx.Rollback()
	if err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}
	chain.tx = nil
	return nil
}

type TransactionWork func(databaseTx *Chain) (interface{}, error)

func (chain *Chain) DoTransaction(work TransactionWork) (interface{}, error) {
	dbTx := chain.Clone()
	if err := dbTx.Begin(); err != nil {
		return nil, fmt.Errorf("begin transaction failed: %w", err)
	}

	var result interface{}
	var err error

	defer func() {
		if r := recover(); r != nil {
			rollbackErr := dbTx.Rollback()
			if rollbackErr != nil {
				fmt.Printf("rollback failed after panic: %v\n", rollbackErr)
			}
			// 重新抛出panic
			panic(r)
		}

		if err != nil {
			rollbackErr := dbTx.Rollback()
			if rollbackErr != nil {
				err = fmt.Errorf("transaction failed: %v, rollback failed: %v", err, rollbackErr)
			}
		}
	}()

	result, err = work(dbTx)
	if err != nil {
		return nil, err
	}

	if err = dbTx.Commit(); err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	return result, nil
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
	chain.rawMeta = nil
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

	chain.cnd.NotInBool(b, field, values)
	return chain
}
func (chain *Chain) OrNotIn(field string, values ...interface{}) *Chain {

	chain.cnd.OrNotIn(field, values...)
	return chain
}
func (chain *Chain) OrNotInBool(b bool, field string, values ...interface{}) *Chain {

	chain.cnd.OrNotInBool(b, field, values)
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
