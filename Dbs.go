package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/kmlixh/gom/v3/define"
	"reflect"
)

type DB struct {
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
	sqlType  define.SqlType
}

func (db DB) GetCurrentSchema() (string, error) {
	return db.Factory().GetCurrentSchema(db.db)
}

func (db DB) GetColumns(table string) ([]define.Column, error) {
	return db.Factory().GetColumns(table, db.db)
}

func (db DB) GetTables() ([]string, error) {
	return db.Factory().GetTables(db.db)
}

func (db DB) GetTableStruct(table string) (define.ITableStruct, error) {
	return db.Factory().GetTableStruct(table, db.db)
}
func (db DB) GetTableStruct2(i any) (define.ITableStruct, error) {
	rawInfo := GetRawTableInfo(i)
	if rawInfo.TableName == "" {
		panic("table name was nil")
	}
	return db.GetTableStruct(rawInfo.TableName)
}

type TransactionWork func(databaseTx *DB) (interface{}, error)

func (db DB) GetRawDb() *sql.DB {
	return db.db
}
func (db DB) Factory() define.SqlFactory {
	return db.factory
}

func (db *DB) Table(table string) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	db.table = &table
	return db
}
func (db DB) GetTable() string {
	if db.table != nil {
		return *db.table
	}
	return ""
}
func (db *DB) cloneSelfIfDifferentGoRoutine() {
	if db.id != getGrouteId() {
		*db = db.Clone()
	}
}
func (db *DB) RawSql(sql string, datas ...any) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	db.rawSql = &sql
	var temp = UnZipSlice(datas)
	db.rawData = temp
	return db
}

func (db *DB) OrderBy(field string, t define.OrderType) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, MakeOrderBy(field, t))
	db.orderBys = &temp
	return db
}
func (db *DB) OrderBys(orderbys []define.OrderBy) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	var temp []define.OrderBy
	temp = append(temp, orderbys...)
	db.orderBys = &temp
	return db
}
func (db *DB) CleanOrders() *DB {
	db.cloneSelfIfDifferentGoRoutine()
	temp := make([]define.OrderBy, 0)
	db.orderBys = &temp
	return db
}
func (db *DB) OrderByAsc(field string) *DB {
	return db.OrderBy(field, define.Asc)
}
func (db *DB) OrderByDesc(field string) *DB {
	return db.OrderBy(field, define.Desc)
}

func (db *DB) Where2(sql string, patches ...interface{}) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	return db.Where(CndRaw(sql, patches...))
}
func (db *DB) Where(cnd define.Condition) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	db.cnd = cnd
	return db
}
func (db DB) Clone() DB {
	return DB{id: getGrouteId(), factory: db.factory, db: db.db}
}

func (db *DB) Page(page int64, pageSize int64) *DB {
	db.cloneSelfIfDifferentGoRoutine()
	pages := MakePage(page, pageSize)
	db.page = pages
	return db
}

func (db DB) Count(columnName string) (int64, error) {
	statements := fmt.Sprintf("select count(%s) as count from %s", columnName, *db.table)
	var data []interface{}
	if db.cnd != nil && db.cnd.PayLoads() > 0 {
		cndString, cndData := db.factory.ConditionToSql(false, db.cnd)
		if cndString != "" {
			data = append(data, cndData...)
			statements = statements + " WHERE " + cndString
		}
	}
	var count int64 = 0
	scanners, er := getDefaultScanner(&count)
	if er != nil {
		return 0, er
	}
	_, er = db.query(statements, data, scanners)

	return count, er
}

func (db *DB) Sum(columnName string) (int64, error) {
	statements := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, *db.table)
	var data []interface{}
	if db.cnd != nil && db.cnd.PayLoads() > 0 {
		cndString, cndData := db.factory.ConditionToSql(false, db.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64 = 0
	scanners, er := getDefaultScanner(&count)
	if er != nil {
		return 0, er
	}
	_, er = db.query(statements, data, scanners)

	return count, er
}

func (db *DB) Select(vs any, columns ...string) (interface{}, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = define.Query
	scanner, er := getDefaultScanner(vs, columns...)
	if er != nil {
		return 0, er
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		return db.query(*db.rawSql, db.rawData, scanner)
	} else {
		rawInfo := GetRawTableInfo(vs)
		if rawInfo.IsStruct {
			//检查列缺失
			colMap, cols := getDefaultsColumnFieldMap(rawInfo.Type)
			if len(columns) > 0 {
				for _, c := range columns {
					if _, ok := colMap[c]; !ok {
						return nil, errors.New(fmt.Sprintf("'%s' not exist in variable ", c))
					}
				}
			}
			if columns == nil || len(columns) == 0 {
				columns = cols
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
			columns:       columns,
			columnDataMap: nil,
			condition:     cnd,
			orderBys:      db.GetOrderBys(),
			page:          db.GetPageInfo(),
		}
		selectFunc := db.factory.GetSqlFunc(define.Query)
		sqlProtos := selectFunc(model)
		if er != nil {
			return nil, er
		}
		return db.query(sqlProtos[0].PreparedSql, sqlProtos[0].Data, scanner)
	}
}
func (db *DB) First(vs interface{}) (interface{}, error) {
	db.cloneSelfIfDifferentGoRoutine()
	return db.Page(0, 1).Select(vs)
}
func (db *DB) Insert(v interface{}, columns ...string) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = define.Insert
	return db.executeInside(ArrayOf(v), columns...)

}
func (db *DB) Delete(vs ...interface{}) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = define.Delete
	return db.executeInside(vs)

}

func (db *DB) Update(v interface{}, columns ...string) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = define.Update
	return db.executeInside(ArrayOf(v), columns...)
}

func (db *DB) executeInside(vi []interface{}, customColumns ...string) (sql.Result, error) {
	var vs []interface{}
	if vi != nil && len(vi) > 0 {
		vs = append(vs, UnZipSlice(vi)...)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		if vs != nil && len(vs) > 0 {
			return nil, errors.New("when the RawSql is not nil or empty,data should be nil")
		}
		return db.ExecuteRaw(*db.rawSql, db.rawData...)
	}
	if len(vs) == 0 && db.table == nil && db.cnd == nil {
		return nil, errors.New("there was nothing to do")
	} else {
		var vvs []define.TableModel
		if vs != nil && len(vs) > 0 {
			for _, v := range vs {
				rawInfo := GetRawTableInfo(v)
				table := db.GetTable()
				if len(table) == 0 {
					table = rawInfo.TableName
				}
				//检查列缺失
				colMap, _ := getDefaultsColumnFieldMap(rawInfo.Type)
				dbCols, er := db.factory.GetColumns(table, db.db)
				if er != nil {
					return nil, er
				}
				primaryKey := make([]string, 0)
				primaryAuto := make([]string, 0)
				dbColMap := make(map[string]define.Column)
				dbColNames := make([]string, 0)
				for _, dbCol := range dbCols {
					dbColNames = append(dbColNames, dbCol.ColumnName)
					dbColMap[dbCol.ColumnName] = dbCol
					if _, ok := colMap[dbCol.ColumnName]; !ok && dbCol.IsPrimary {
						return nil, errors.New(fmt.Sprintf("column '%s' not exist in variable ", dbCol.ColumnName))
					}
					if dbCol.IsPrimary && !dbCol.IsPrimaryAuto {
						primaryKey = append(primaryKey, dbCol.ColumnName)
					}
					if dbCol.IsPrimaryAuto {
						primaryAuto = append(primaryAuto, dbCol.ColumnName)
					}
				}
				columns := customColumns
				if len(columns) > 0 {
					for _, c := range columns {
						if _, ok := colMap[c]; !ok {
							return nil, errors.New(fmt.Sprintf("'%s' not exist in variable ", c))
						}
						if _, ok := dbColMap[c]; !ok {
							return nil, errors.New(fmt.Sprintf("'%s' not exist in table '%s' ", c, table))
						}
					}
				}

				if len(columns) > 0 {
					columns = append(primaryKey, append(primaryAuto, columns...)...)
				}
				var cnd define.Condition
				cnd = db.GetCondition()
				dataMap, er := StructToMap(v, columns...)
				dataCol := make([]string, 0)
				for key, _ := range dataMap {
					dataCol = append(dataCol, key)
				}
				columns = ArrayIntersect(dbColNames, dataCol)

				if db.sqlType == define.Update {
					if cnd == nil {
						if er != nil {
							return nil, er
						}
						prs := append(primaryKey, primaryAuto...)
						if len(prs) == 0 {
							return nil, errors.New("can't find primary Key")
						}
						cndMap := make(map[string]interface{})
						for _, key := range prs {
							data, ok := dataMap[key]
							if !ok {
								return nil, errors.New(fmt.Sprintf("can't find data for primary Key '%s'", key))
							}
							if reflect.ValueOf(data).IsZero() {
								return nil, errors.New(fmt.Sprintf("value of Key '%s' can't be nil or empty", key))
							}
							cndMap[key] = data
						}
						cnd = MapToCondition(cndMap)
					}

					columns, _, _ = ArrayIntersect2(columns, append(primaryKey, primaryAuto...))

				} else if db.sqlType == define.Delete && cnd == nil {
					if er != nil {
						return nil, er
					}
					cnd = MapToCondition(dataMap)
					columns = make([]string, 0)
				}

				if db.sqlType == define.Insert {
					columns = ArrayIntersect(dbColNames, columns)
					if len(primaryAuto) > 0 {
						columns, _, _ = ArrayIntersect2(columns, primaryAuto)
					}
				}

				dm := &DefaultModel{
					table:         table,
					primaryKeys:   append(primaryKey, primaryAuto...),
					columns:       columns,
					columnDataMap: dataMap,
					condition:     cnd,
					orderBys:      db.GetOrderBys(),
					page:          db.GetPageInfo(),
				}

				if db.sqlType == define.Update && cnd == nil {
					return nil, errors.New("can't update Database without Conditions")
				}
				vvs = append(vvs, dm)
			}
		} else if db.sqlType == define.Delete && db.GetTable() != "" && db.GetCondition() != nil {
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
		genFunc := db.factory.GetSqlFunc(db.sqlType)
		//此处应当判断是否已经在事物中，如果不在事务中才开启事物
		count := int64(0)
		sqlProtos := genFunc(vvs...)
		for _, sqlProto := range sqlProtos {
			if Debug {
				fmt.Println(sqlProto)
			}
			rs, er := db.ExecuteStatement(sqlProto.PreparedSql, sqlProto.Data...)
			if er != nil {
				return CommonSqlResult{0, 0, er}, nil
			}
			cs, err := rs.RowsAffected()
			if cs == 1 && len(sqlProtos) == len(vvs) && db.sqlType == define.Insert {
				//
				id, er := rs.LastInsertId()
				if er == nil {
					lastInsertId = id
				}
			}
			if err != nil {
				return nil, err
			}
			count += cs
		}
		db.CleanDb()
		return CommonSqlResult{lastInsertId, count, nil}, nil
	}
}
func (db *DB) ExecuteRaw(rawSql string, datas ...any) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	return db.ExecuteStatement(rawSql, datas...)

}

func (db *DB) ExecuteStatement(statement string, data ...any) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
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
			db.CleanDb()
		}
	}()
	return rs, er
}

func (db *DB) prepare(query string) (*sql.Stmt, error) {
	if db.IsInTransaction() {
		st, er := db.tx.Prepare(query)
		if er != nil {
			db.Rollback()
		}
		return st, er
	}
	return db.db.Prepare(query)
}

func (db *DB) GetCondition() define.Condition {
	if db.cnd != nil {
		return db.cnd
	}
	return nil
}

func (db *DB) query(statement string, data []interface{}, rowScanner define.IRowScanner) (interface{}, error) {
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
		db.CleanDb()
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
func (db *DB) IsInTransaction() bool {
	return db.tx != nil
}
func (db *DB) Commit() {
	if db.IsInTransaction() {
		err := db.tx.Commit()
		if err != nil {
			panic(err)
		}
		db.tx = nil
	}
}
func (db *DB) Rollback() {
	if db.tx != nil {
		err := db.tx.Rollback()
		if err != nil {
			panic(err)
		}
		db.tx = nil
	}
}

func (db *DB) DoTransaction(work TransactionWork) (interface{}, error) {
	//Create A New Db And set Tx for it
	dbTx := db.Clone()
	eb := dbTx.Begin()
	if eb != nil {
		return nil, eb
	}
	defer func(dbTx *DB) {
		//catch the panic of 'work' function
		r := recover()
		if r != nil {
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

func (db *DB) GetOrderBys() []define.OrderBy {
	if db.orderBys != nil && *db.orderBys != nil {
		return *db.orderBys
	}
	return nil
}

func (db *DB) GetPageInfo() define.PageInfo {
	if db.page != nil {
		return db.page
	}
	return nil
}
func (db *DB) GetPage() (int64, int64) {
	if db.page != nil {
		return db.page.Page()
	}
	return 0, 0
}
func (db *DB) CleanDb() *DB {
	db.table = nil
	db.page = nil
	db.orderBys = nil
	db.rawSql = nil
	db.cnd = nil
	return db
}
