package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
)

type MyCndStruct struct {
	Linker     string
	Expression string
	Data       []interface{}
}

var keywordMap map[string]string

var funcMap map[define.SqlType]define.SqlFunc

var factory = Factory{}

type Factory struct {
}

func (m Factory) GetSqlTypeDefaultValue(sqlType string) any {
	sqlType = strings.ToLower(sqlType)
	switch {
	// Numeric types
	case strings.Contains(sqlType, "tinyint"):
		return int8(0)
	case strings.Contains(sqlType, "smallint"):
		return int16(0)
	case strings.Contains(sqlType, "mediumint"):
		return int32(0)
	case strings.Contains(sqlType, "int"), strings.Contains(sqlType, "integer"):
		return int(0)
	case strings.Contains(sqlType, "bigint"):
		return int64(0)
	case strings.Contains(sqlType, "decimal"), strings.Contains(sqlType, "numeric"):
		return float64(0.0)
	case strings.Contains(sqlType, "float"):
		return float32(0.0)
	case strings.Contains(sqlType, "double"):
		return float64(0.0)
	case strings.Contains(sqlType, "bit"):
		return uint8(0)
	case strings.Contains(sqlType, "boolean"), strings.Contains(sqlType, "bool"):
		return false

	// Date and time types
	case strings.Contains(sqlType, "date"):
		return time.Now()
	case strings.Contains(sqlType, "time"):
		return time.Now()
	case strings.Contains(sqlType, "datetime"):
		return time.Now()
	case strings.Contains(sqlType, "timestamp"):
		return time.Now()
	case strings.Contains(sqlType, "year"):
		return 0

	// String types
	case strings.Contains(sqlType, "char"), strings.Contains(sqlType, "varchar"):
		return ""
	case strings.Contains(sqlType, "text"), strings.Contains(sqlType, "tinytext"), strings.Contains(sqlType, "mediumtext"), strings.Contains(sqlType, "longtext"):
		return ""
	case strings.Contains(sqlType, "enum"), strings.Contains(sqlType, "set"):
		return ""
	case strings.Contains(sqlType, "json"):
		return ""
	case strings.Contains(sqlType, "binary"), strings.Contains(sqlType, "varbinary"):
		return []byte{}
	case strings.Contains(sqlType, "blob"), strings.Contains(sqlType, "tinyblob"), strings.Contains(sqlType, "mediumblob"), strings.Contains(sqlType, "longblob"):
		return []byte{}

	default:
		return nil
	}
}

func (m Factory) OpenDb(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}

var dbTableColsCache = make(map[string][]define.Column)
var dbTableCache = make(map[string]define.ITableStruct)

func init() {
	InitFactory()
}
func InitFactory() {
	define.RegisterFactory("mysql", &factory)
	define.RegisterFactory("Mysql", &factory)
	initKeywordMap()
	funcMap = make(map[define.SqlType]define.SqlFunc)
	funcMap[define.Query] = func(models ...define.TableModel) []define.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "SELECT "
		counts := len(model.Columns())
		if counts == 0 {
			panic(errors.New("columns is null or empty"))
		} else {
			for i := 0; i < len(model.Columns()); i++ {
				if i == 0 {
					sql += wrapperName(model.Columns()[i]) + " "
				} else {
					sql += ", " + wrapperName(model.Columns()[i]) + " "
				}
			}
		}
		sql += " FROM " + model.Table() + " "

		if model.Condition() != nil && model.Condition().PayLoads() > 0 {
			cndString, cndData := factory.ConditionToSql(false, model.Condition())
			datas = append(datas, cndData...)
			sql += " WHERE " + cndString
		}
		if len(model.OrderBys()) > 0 {
			sql += " ORDER BY"
			for i := 0; i < len(model.OrderBys()); i++ {
				if i > 0 {
					sql += ","
				}
				t := ""
				if model.OrderBys()[i].Type() == define.Asc {
					t = " ASC"
				} else {
					t = " DESC"
				}
				sql += " " + wrapperName(model.OrderBys()[i].Name()) + t
			}
		}
		if model.Page() != nil {
			idx, size := model.Page().Page()
			datas = append(datas, idx, size)
			sql += " LIMIT ?,?"
		}
		sql += ";"
		var result []define.SqlProto
		result = append(result, define.SqlProto{PreparedSql: sql, Data: datas})
		return result
	}
	funcMap[define.Update] = func(models ...define.TableModel) []define.SqlProto {
		if models == nil || len(models) == 0 {
			panic(errors.New("model was nil or empty"))
		}
		var result []define.SqlProto
		for _, model := range models {
			if model.ColumnDataMap() == nil {
				panic(errors.New("nothing to update"))
			}
			var datas []interface{}
			sql := "UPDATE "
			sql += " " + model.Table() + " SET "
			i := 0
			for _, k := range model.Columns() {
				if i > 0 {
					sql += ", "
				}
				sql += wrapperName(k) + " = ? "
				datas = append(datas, model.ColumnDataMap()[k])
				i++
			}
			if model.Condition() != nil && model.Condition().PayLoads() > 0 {
				conditionSql, dds := factory.ConditionToSql(false, model.Condition())
				datas = append(datas, dds...)
				sql += " WHERE " + conditionSql + ";"
			}
			result = append(result, define.SqlProto{sql, datas})
		}

		return result
	}
	funcMap[define.Insert] = func(models ...define.TableModel) []define.SqlProto {
		var result []define.SqlProto
		for _, model := range models {
			var datas []interface{}

			sql := "INSERT INTO " + model.Table() + " ("
			valuesPattern := "VALUES("
			i := 0
			for _, c := range model.Columns() {
				if i > 0 {
					sql += ","
					valuesPattern += ","
				}
				sql += wrapperName(c)
				valuesPattern += "?"
				datas = append(datas, model.ColumnDataMap()[c])
				i++
			}
			sql += ")"
			valuesPattern += ");"
			sql += valuesPattern
			result = append(result, define.SqlProto{sql, datas})
		}
		return result
	}
	funcMap[define.Delete] = func(models ...define.TableModel) []define.SqlProto {
		var result []define.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "DELETE FROM "
			sql += " " + model.Table()

			if model.Condition() != nil && model.Condition().PayLoads() > 0 {
				conditionSql, dds := factory.ConditionToSql(false, model.Condition())
				datas = append(datas, dds...)
				sql += " WHERE " + conditionSql + ";"
			}
			result = append(result, define.SqlProto{sql, datas})
		}
		return result
	}
}
func (m Factory) GetCurrentSchema(db *sql.DB) (string, error) {
	dbName := ""
	dbSql := "SELECT DATABASE() as db;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return dbName, er
	}
	if !rows.Next() {
		return dbName, errors.New("can not get Schema")
	}
	er = rows.Scan(&dbName)
	return dbName, er
}
func (m Factory) GetTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)
	dbSql := "SHOW TABLES;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return nil, er
	}
	for rows.Next() {
		tableName := ""
		rows.Scan(&tableName)
		tables = append(tables, tableName)
	}
	return tables, nil
}

var columnSql = "select COLUMN_NAME as columnName,DATA_TYPE as dataType,COLUMN_KEY as columnKey,EXTRA as extra, IFNULL(COLUMN_COMMENT,'') as comment from information_schema.columns  where table_schema=?  and table_name= ? order by ordinal_position;"

func (f Factory) Execute(db *sql.DB, sqlType define.SqlType, st *sql.Stmt, data []interface{}, rowScanner define.IRowScanner) define.Result {
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

func (m Factory) GetTableStruct(tableName string, db *sql.DB) (define.ITableStruct, error) {
	var tableStruct define.TableStruct
	if table, ok := dbTableCache[tableName]; ok {
		return table, nil
	}

	dbSql := "SELECT DATABASE() as db;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return nil, er
	}
	dbName := ""
	if !rows.Next() {
		return nil, errors.New("can not get Schema")
	}
	er = rows.Scan(&dbName)
	if er != nil {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}
	//查询表信息
	tbSql := `SELECT 
    TABLE_NAME AS 'tableName',
    IFNULL(TABLE_COMMENT,'') AS 'comment'
FROM 
    information_schema.TABLES
WHERE 
    TABLE_SCHEMA = ?
    AND TABLE_NAME = ?;`
	//-------------------
	tbRow, er := db.Query(tbSql, dbName, tableName)
	if er != nil {
		return nil, er
	}
	tbName := ""
	tbComment := ""
	if !tbRow.Next() {
		return nil, errors.New("can not get Schema")
	}
	er = tbRow.Scan(&tbName, &tbComment)
	if er != nil {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}

	if cols, ok := dbTableColsCache[tableName]; ok {
		tableStruct = define.TableStruct{tbName, tbComment, cols}
	} else {
		rows, er = db.Query(columnSql, dbName, tableName)
		if er != nil {
			return nil, er
		}
		cols = make([]define.Column, 0)
		for rows.Next() {
			columnName := ""
			columnType := ""
			columnKey := ""
			extra := ""
			comment := ""
			er = rows.Scan(&columnName, &columnType, &columnKey, &extra, &comment)
			if er == nil {
				cols = append(cols, define.Column{ColumnName: columnName, ColumnTypeName: columnType, IsPrimary: columnKey == "PRI", IsPrimaryAuto: columnKey == "PRI" && extra == "auto_increment", ColumnValue: m.GetSqlTypeDefaultValue(columnType), Comment: comment})
			} else {
				return nil, er
			}
		}

		dbTableColsCache[tableName] = cols
		tableStruct = define.TableStruct{tbName, tbComment, cols}
	}
	return tableStruct, nil
}

func (m Factory) GetColumns(tableName string, db *sql.DB) ([]define.Column, error) {

	dbSql := "SELECT DATABASE() as db;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return nil, er
	}
	dbName := ""
	if !rows.Next() {
		return nil, errors.New("can not get Schema")
	}
	er = rows.Scan(&dbName)
	if er != nil {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}
	if cols, ok := dbTableColsCache[dbName+"-"+tableName]; ok {
		return cols, nil
	}
	rows, er = db.Query(columnSql, dbName, tableName)
	if er != nil {
		return nil, er
	}
	columns := make([]define.Column, 0)
	for rows.Next() {
		columnName := ""
		columnType := ""
		columnKey := ""
		extra := ""
		comment := ""
		er = rows.Scan(&columnName, &columnType, &columnKey, &extra, &comment)
		if er == nil {
			columns = append(columns, define.Column{ColumnName: columnName, ColumnTypeName: columnType, IsPrimary: columnKey == "PRI", IsPrimaryAuto: columnKey == "PRI" && extra == "auto_increment", Comment: comment})
		} else {
			return nil, er
		}
	}
	dbTableColsCache[dbName+"-"+tableName] = columns
	return columns, nil

}

func (m Factory) GetSqlFunc(sqlType define.SqlType) define.SqlFunc {
	return funcMap[sqlType]
}
func (m Factory) ConditionToSql(preTag bool, cnd define.Condition) (string, []interface{}) {
	if cnd == nil || cnd.PayLoads() == 0 {
		return "", nil
	}
	myCnd := cndToMyCndStruct(cnd)

	var data []interface{}
	data = append(data, myCnd.Data...)
	sql := ""
	if preTag {
		sql += myCnd.Linker
	}
	if preTag && cnd.PayLoads() > 1 {
		sql += " ("
	}
	curTag := len(myCnd.Expression) > 0
	sql += myCnd.Expression

	if cnd.HasSubConditions() {
		for _, v := range cnd.Items() {
			if v.PayLoads() > 0 {
				s, dd := m.ConditionToSql(curTag || preTag, v)
				if len(s) > 0 {
					curTag = true
				}
				sql += s
				data = append(data, dd...)
			}
		}
	}

	if preTag && cnd.PayLoads() > 1 {
		sql += ")"
	}

	return sql, data

}
func initKeywordMap() {
	keywordMap = make(map[string]string)
	keys := strings.Split("ACTIVE,ADMIN,ARRAY,ATTRIBUTE,AUTHENTICATION,BUCKETS,BULK,CHALLENGE_RESPONSE,CLONE,COMPONENT,CUME_DIST,DEFINITION,DENSE_RANK,DESCRIPTION,EMPTY,ENFORCED,ENGINE_ATTRIBUTE,EXCEPT,EXCLUDE,FACTOR,FAILED_LOGIN_ATTEMPTS,FINISH,FIRST_VALUE,FOLLOWING,GENERATE,GEOMCOLLECTION,GET_MASTER_PUBLIC_KEY,GET_SOURCE_PUBLIC_KEY,GROUPING,GROUPS,GTID_ONLY,HISTOGRAM,HISTORY,INACTIVE,INITIAL,INITIATE,INTERSECT,INVISIBLE,JSON_TABLE,JSON_VALUE,KEYRING,LAG,LAST_VALUE,LATERAL,LEAD,LOCKED,MASTER_COMPRESSION_ALGORITHMS,MASTER_PUBLIC_KEY_PATH,MASTER_TLS_CIPHERSUITES,MASTER_ZSTD_COMPRESSION_LEVEL,MEMBER,NESTED,NETWORK_NAMESPACE,NOWAIT,NTH_VALUE,NTILE,NULLS,OF,OFF,OJ,OLD,OPTIONAL,ORDINALITY,ORGANIZATION,OTHERS,OVER,PASSWORD_LOCK_TIME,PATH,PERCENT_RANK,PERSIST,PERSIST_ONLY,PRECEDING,PRIVILEGE_CHECKS_USER,PROCESS,RANDOM,RANK,RECURSIVE,REFERENCE,REGISTRATION,REPLICA,REPLICAS,REQUIRE_ROW_FORMAT,RESOURCE,RESPECT,RESTART,RETAIN,RETURNING,REUSE,ROLE,ROW_NUMBER,SECONDARY,SECONDARY_ENGINE,SECONDARY_ENGINE_ATTRIBUTE,SECONDARY_LOAD,SECONDARY_UNLOAD,SKIP,SOURCE_AUTO_POSITION,SOURCE_BIND,SOURCE_COMPRESSION_ALGORITHMS,SOURCE_CONNECT_RETRY,SOURCE_DELAY,SOURCE_HEARTBEAT_PERIOD,SOURCE_HOST,SOURCE_LOG_FILE,SOURCE_LOG_POS,SOURCE_PASSWORD,SOURCE_PORT,SOURCE_PUBLIC_KEY_PATH,SOURCE_RETRY_COUNT,SOURCE_SSL,SOURCE_SSL_CA,SOURCE_SSL_CAPATH,SOURCE_SSL_CERT,SOURCE_SSL_CIPHER,SOURCE_SSL_CRL,SOURCE_SSL_CRLPATH,SOURCE_SSL_KEY,SOURCE_SSL_VERIFY_SERVER_CERT,SOURCE_TLS_CIPHERSUITES,SOURCE_TLS_VERSION,SOURCE_USER,SOURCE_ZSTD_COMPRESSION_LEVEL,SRID,STREAM,SYSTEM,THREAD_PRIORITY,TIES,TLS,UNBOUNDED,UNREGISTER,URL,VCPU,VISIBLE,WINDOW,ZONE", ",")
	for _, key := range keys {
		keywordMap[key] = key
	}
}

func wrapperName(name string) string {
	if strings.IndexAny(name, " ") > 0 {
		return name
	} else {
		name = strings.TrimSpace(name)
		return "`" + name + "`"
	}
}

func cndToMyCndStruct(cnd define.Condition) MyCndStruct {
	if len(cnd.RawExpression()) > 0 {
		return MyCndStruct{linkerToString(cnd), cnd.RawExpression(), cnd.Values()}
	}
	opers := cnd.Field()
	switch cnd.Operation() {
	case define.Eq:
		opers += " = ? "
	case define.NotEq:
		opers += " <> ? "
	case define.Ge:
		opers += " >= ? "
	case define.Gt:
		opers += " > ? "
	case define.Le:
		opers += " <= ? "
	case define.Lt:
		opers += " < ? "
	case define.In:
		opers += " IN " + valueSpace(len(cnd.Values()))
	case define.NotIn:
		opers += " NOT IN " + valueSpace(len(cnd.Values()))
	case define.Like:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case define.NotLike:
		opers += " NOT LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case define.LikeIgnoreStart:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string)
		cnd.SetValues(vals)
	case define.LikeIgnoreEnd:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = vals[0].(string) + "%"
		cnd.SetValues(vals)
	case define.IsNull:
		opers += " IS NULL "
	case define.IsNotNull:
		opers += " IS NOT NULL "
	}
	return MyCndStruct{linkerToString(cnd), opers, cnd.Values()}
}

func linkerToString(cnd define.Condition) string {
	switch cnd.Linker() {
	case define.And:
		return " AND "
	case define.Or:
		return " OR "
	default:
		return " AND "
	}
}

func valueSpace(count int) string {
	if count == 1 {
		return " ( ? ) "
	} else {
		str := "("
		for i := 0; i < count-1; i++ {
			str += "?,"
		}
		str += "?)"
		return str
	}
}
