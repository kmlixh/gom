package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v3"
	"strings"
)

type MyCndStruct struct {
	Linker     string
	Expression string
	Data       []interface{}
}

var keywordMap map[string]string

var funcMap map[gom.SqlType]gom.SqlFunc

var factory Factory = Factory{}

type Factory struct {
}

func (m Factory) OpenDb(dsn string) (*sql.DB, error) {
	return sql.Open("Mysql", dsn)
}

var dbTableColsCache = make(map[string][]gom.Column)

func init() {
	gom.Register("mysql", &factory)
	InitMysqlFactory()
}
func InitMysqlFactory() {
	initKeywordMap()
	funcMap = make(map[gom.SqlType]gom.SqlFunc)
	funcMap[gom.Query] = func(models ...gom.TableModel) []gom.SqlProto {
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
		cndString, cndData := factory.ConditionToSql(false, model.Condition())
		if len(cndString) > 0 {
			sql += " WHERE " + cndString
		}
		datas = append(datas, cndData...)
		if len(model.OrderBys()) > 0 {
			sql += " ORDER BY"
			for i := 0; i < len(model.OrderBys()); i++ {
				if i > 0 {
					sql += ","
				}
				t := ""
				if model.OrderBys()[i].Type() == gom.Asc {
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
		var result []gom.SqlProto
		result = append(result, gom.SqlProto{PreparedSql: sql, Data: datas})
		return result
	}
	funcMap[gom.Update] = func(models ...gom.TableModel) []gom.SqlProto {
		if models == nil || len(models) == 0 {
			panic(errors.New("model was nil or empty"))
		}
		var result []gom.SqlProto
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
			conditionSql, dds := factory.ConditionToSql(false, model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, gom.SqlProto{sql, datas})
		}

		return result
	}
	funcMap[gom.Insert] = func(models ...gom.TableModel) []gom.SqlProto {
		var result []gom.SqlProto
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
			result = append(result, gom.SqlProto{sql, datas})
		}
		return result
	}
	funcMap[gom.Delete] = func(models ...gom.TableModel) []gom.SqlProto {
		var result []gom.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "DELETE FROM "
			sql += " " + model.Table()
			conditionSql, dds := factory.ConditionToSql(false, model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, gom.SqlProto{sql, datas})
		}
		return result
	}
}
func (m Factory) GetColumns(tableName string, db *sql.DB) ([]gom.Column, error) {

	dbSql := "SELECT DATABASE() as db;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return nil, er
	}
	dbName := ""
	if !rows.Next() {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}
	er = rows.Scan(&dbName)
	if er != nil {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}
	if cols, ok := dbTableColsCache[dbName+"-"+tableName]; ok {
		return cols, nil
	}
	columnSql := "select COLUMN_NAME as columnName,DATA_TYPE as dataType,COLUMN_KEY as columnKey,EXTRA as extra from information_schema.columns  where table_schema=?  and table_name= ? order by ordinal_position;"
	rows, er = db.Query(columnSql, dbName, tableName)
	if er != nil {
		return nil, er
	}
	columns := make([]gom.Column, 0)
	for rows.Next() {
		columnName := ""
		columnType := ""
		columnKey := ""
		extra := ""
		er = rows.Scan(&columnName, &columnType, &columnKey, &extra)
		if er == nil {
			columns = append(columns, gom.Column{ColumnName: columnName, ColumnType: columnType, Primary: columnKey == "PRI", PrimaryAuto: columnKey == "PRI" && extra == "auto_increment"})
		} else {
			return nil, er
		}
	}
	dbTableColsCache[dbName+"-"+tableName] = columns
	return columns, nil

}

func (m Factory) GetSqlFunc(sqlType gom.SqlType) gom.SqlFunc {
	return funcMap[sqlType]
}
func (m Factory) ConditionToSql(preTag bool, cnd gom.Condition) (string, []interface{}) {
	if cnd == nil {
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

func cndToMyCndStruct(cnd gom.Condition) MyCndStruct {
	if len(cnd.RawExpression()) > 0 {
		return MyCndStruct{linkerToString(cnd), cnd.RawExpression(), cnd.Values()}
	}
	opers := cnd.Field()
	switch cnd.Operation() {
	case gom.Eq:
		opers += " = ? "
	case gom.NotEq:
		opers += " <> ? "
	case gom.Ge:
		opers += " >= ? "
	case gom.Gt:
		opers += " > ? "
	case gom.Le:
		opers += " <= ? "
	case gom.Lt:
		opers += " < ? "
	case gom.In:
		opers += " IN " + valueSpace(len(cnd.Values()))
	case gom.NotIn:
		opers += " NOT IN " + valueSpace(len(cnd.Values()))
	case gom.Like:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case gom.NotLike:
		opers += " NOT LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case gom.LikeIgnoreStart:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string)
		cnd.SetValues(vals)
	case gom.LikeIgnoreEnd:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = vals[0].(string) + "%"
		cnd.SetValues(vals)
	case gom.IsNull:
		opers += " IS NULL "
	case gom.IsNotNull:
		opers += " IS NOT NULL "
	}
	return MyCndStruct{linkerToString(cnd), opers, cnd.Values()}
}

func linkerToString(cnd gom.Condition) string {
	switch cnd.Linker() {
	case gom.And:
		return " AND "
	case gom.Or:
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
