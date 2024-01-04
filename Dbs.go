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
	cnd      Condition
	table    *string
	rawSql   *string
	rawData  *[]interface{}
	tx       *sql.Tx
	orderBys *[]OrderBy
	page     PageInfo
	sqlType  SqlType
}

type TransactionWork func(databaseTx *DB) (interface{}, error)

func (db DB) GetRawDb() *sql.DB {
	return db.db
}
func (db DB) Factory() SqlFactory {
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
	statements := fmt.Sprintf("select count(`%s`) as count from `%s`", columnName, *db.table)
	var data []interface{}
	if db.cnd != nil {
		cndString, cndData := db.factory.ConditionToSql(false, db.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64
	scanners, er := getDefaultScanner(&count)
	if er != nil {
		return 0, er
	}
	_, er = db.query(statements, data, scanners)

	return count, er
}

func (db DB) Sum(columnName string) (int64, error) {
	statements := fmt.Sprintf("select SUM(`%s`) as count from `%s`", columnName, *db.table)
	var data []interface{}
	if db.cnd != nil {
		cndString, cndData := db.factory.ConditionToSql(false, db.cnd)
		data = append(data, cndData...)
		statements = statements + " WHERE " + cndString
	}
	var count int64
	scanners, er := getDefaultScanner(&count)
	if er != nil {
		return 0, er
	}
	_, er = db.query(statements, data, scanners)

	return count, er
}

func (db DB) Select(vs interface{}, columns ...string) (interface{}, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = Query
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		scanners, er := getDefaultScanner(vs, columns...)
		if er != nil {
			return 0, er
		}
		return db.query(*db.rawSql, *db.rawData, scanners)
	} else {
		rawInfo := GetRawTableInfo(vs)
		//检查列缺失
		colMap, cols := getDefaultsColumnFieldMap(rawInfo.Type)
		if len(columns) > 0 {
			for _, c := range columns {
				if _, ok := colMap[c]; !ok {
					return nil, errors.New(fmt.Sprintf("'%s' not exist in variable "))
				}
			}
		}
		if columns == nil || len(columns) == 0 {
			columns = cols
		}
		table := db.GetTable()
		if len(table) == 0 {
			table = rawInfo.TableName
		}
		cnd := db.cnd
		model := &DefaultModel{
			table:         table,
			columns:       columns,
			columnDataMap: nil,
			condition:     cnd,
			orderBys:      db.GetOrderBys(),
			page:          db.GetPageInfo(),
		}
		selectFunc := db.factory.GetSqlFunc(Query)
		sqlProtos := selectFunc(model)
		scanner, er := getDefaultScanner(vs)
		if er != nil {
			return nil, er
		}
		return db.query(sqlProtos[0].PreparedSql, sqlProtos[0].Data, scanner)
	}
}
func (db DB) First(vs interface{}) (interface{}, error) {
	db.cloneSelfIfDifferentGoRoutine()
	return db.Page(0, 1).Select(vs)
}
func (db DB) Insert(v interface{}, columns ...string) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = Insert
	return db.executeInside(ArrayOf(v), columns...)

}
func (db DB) Delete(vs ...interface{}) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = Delete
	return db.executeInside(vs)

}
func (db DB) Update(v interface{}, columns ...string) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	db.sqlType = Update
	return db.executeInside(ArrayOf(v), columns...)
}

func (db DB) executeInside(vi []interface{}, customColumns ...string) (sql.Result, error) {
	var vs []interface{}
	if vi != nil && len(vi) > 0 {
		vs = append(vs, UnZipSlice(vi)...)
	}
	if db.rawSql != nil && len(*db.rawSql) > 0 {
		if vs != nil && len(vs) > 0 {
			return nil, errors.New("when the RawSql is not nil or empty,data should be nil")
		}
		return db.ExecuteRaw(*db.rawSql, db.rawData)
	}
	if len(vs) == 0 && db.table == nil && db.cnd == nil {
		return nil, errors.New("there was nothing to do")
	} else {
		var vvs []TableModel
		if vs != nil && len(vs) > 0 {
			for _, v := range vs {
				rawInfo := GetRawTableInfo(v)
				table := db.GetTable()
				if len(table) == 0 {
					table = rawInfo.TableName
				}
				//检查列缺失
				colMap, rawCols := getDefaultsColumnFieldMap(rawInfo.Type)
				dbCols, er := db.factory.GetColumns(table, db.db)
				if er != nil {
					return nil, er
				}
				primaryKey := make([]string, 0)
				primaryAuto := make([]string, 0)
				dbColMap := make(map[string]Column)
				dbColNames := make([]string, 0)
				for _, dbCol := range dbCols {
					dbColNames = append(dbColNames, dbCol.ColumnName)
					dbColMap[dbCol.ColumnName] = dbCol
					if dbCol.Primary && !dbCol.PrimaryAuto {
						primaryKey = append(primaryKey, dbCol.ColumnName)
					}
					if dbCol.PrimaryAuto {
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

				if columns == nil || len(columns) == 0 {
					columns = ArrayIntersect(dbColNames, rawCols)
				} else {
					columns = ArrayIntersect(dbColNames, columns)
					columns = ArrayIntersect(columns, rawCols)
				}
				dataMap := make(map[string]interface{})
				var cnd Condition
				cnd = db.GetCondition()
				if cnd == nil {
					if db.sqlType == Update {
						rawDataMap, er := StructToMap(v, ArrayIntersect(dbColNames, append(append(primaryKey, primaryAuto...), columns...))...)
						if er != nil {
							return nil, er
						}
						if len(primaryKey) == 0 {
							return nil, errors.New("can't find primary Key")
						}
						cndMap := make(map[string]interface{})
						for _, key := range primaryKey {
							data, ok := rawDataMap[key]
							if !ok {
								return nil, errors.New(fmt.Sprintf("can't find data for primary Key '%s'", key))
							}
							if reflect.ValueOf(data).IsZero() {
								return nil, errors.New(fmt.Sprintf("value of Key '%s' can't be nil or empty", key))
							}
							cndMap[key] = data
							delete(rawDataMap, key)
						}
						cnd = MapToCondition(cndMap)
						for _, col := range columns {
							data, ok := rawDataMap[col]
							if ok && (len(columns) != len(dbCols) || !reflect.ValueOf(data).IsZero()) {
								dataMap[col] = data
							}
						}

					} else if db.sqlType == Delete {
						rawDataMap, er := StructToMap(v, columns...)
						if er != nil {
							return nil, er
						}
						cnd = MapToCondition(rawDataMap)
					}
				}
				if db.sqlType == Insert {

					rawDataMap, er := StructToMap(v, ArrayIntersect(dbColNames, append(primaryKey, columns...))...)
					if er != nil {
						return nil, er
					}
					if len(primaryKey) > 0 {
						for _, key := range primaryKey {
							_, ok := rawDataMap[key]
							if !dbColMap[key].PrimaryAuto && !ok {
								return nil, errors.New("primary key was null")
							}
							if dbColMap[key].PrimaryAuto && ok {
								//自增情况下，从dataMap中删除key

								delete(rawDataMap, key)
							}
						}
					}
					dataMap = rawDataMap
					columns = ArrayIntersect(dbColNames, append(primaryKey, columns...))

				}

				dm := &DefaultModel{
					table:         table,
					columns:       columns,
					columnDataMap: dataMap,
					condition:     cnd,
					orderBys:      db.GetOrderBys(),
					page:          db.GetPageInfo(),
				}

				if db.sqlType == Update && cnd == nil {
					return nil, errors.New("can't update Database without Conditions")
				}
				vvs = append(vvs, dm)
			}
		} else if db.sqlType == Delete && db.GetTable() != "" && db.GetCondition() != nil {
			dm := &DefaultModel{
				table:         db.GetTable(),
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
			if cs == 1 && len(sqlProtos) == len(vvs) && db.sqlType == Insert {
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
func (db DB) ExecuteRaw(rawSql string, datas ...interface{}) (sql.Result, error) {
	db.cloneSelfIfDifferentGoRoutine()
	return db.ExecuteStatement(rawSql, datas...)

}

func (db DB) ExecuteStatement(statement string, data ...interface{}) (sql.Result, error) {
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

func (db DB) GetCondition() Condition {
	if db.cnd != nil {
		return db.cnd
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

func (db DB) GetPageInfo() PageInfo {
	if db.page != nil {
		return db.page
	}
	return nil
}
func (db DB) GetPage() (int64, int64) {
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
	db.tx = nil
	return db
}
