package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
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
	case strings.Contains(sqlType, "smallint"):
		return int16(0)
	case strings.Contains(sqlType, "integer"):
		return int(0)
	case strings.Contains(sqlType, "bigint"):
		return int64(0)
	case strings.Contains(sqlType, "decimal"), strings.Contains(sqlType, "numeric"):
		return float64(0.0)
	case strings.Contains(sqlType, "real"):
		return float32(0.0)
	case strings.Contains(sqlType, "double precision"):
		return float64(0.0)
	case strings.Contains(sqlType, "serial"):
		return int(0)
	case strings.Contains(sqlType, "bigserial"):
		return int64(0)

	// Monetary types
	case strings.Contains(sqlType, "money"):
		return float64(0.0)

	// Date and time types
	case strings.Contains(sqlType, "date"):
		return time.Now()
	case strings.Contains(sqlType, "time"):
		return time.Now()
	case strings.Contains(sqlType, "timestamp"):
		return time.Now()
	case strings.Contains(sqlType, "interval"):
		return ""

	// Boolean
	case strings.Contains(sqlType, "boolean"):
		return false

	// UUID
	case strings.Contains(sqlType, "uuid"):
		return ""

	// Geometric types
	case strings.Contains(sqlType, "point"), strings.Contains(sqlType, "line"), strings.Contains(sqlType, "lseg"), strings.Contains(sqlType, "box"), strings.Contains(sqlType, "path"), strings.Contains(sqlType, "polygon"), strings.Contains(sqlType, "circle"):
		return ""

	// Network Address types
	case strings.Contains(sqlType, "cidr"), strings.Contains(sqlType, "inet"), strings.Contains(sqlType, "macaddr"):
		return ""

	// Character types
	case strings.Contains(sqlType, "char"), strings.Contains(sqlType, "varchar"):
		return ""
	case strings.Contains(sqlType, "text"):
		return ""

	// JSON types
	case strings.Contains(sqlType, "json"), strings.Contains(sqlType, "jsonb"):
		return ""

	// Binary types
	case strings.Contains(sqlType, "bytea"):
		return []byte{}

	// Array types (simply represented as Go slices here)
	case strings.Contains(sqlType, "[]"):
		return []interface{}{}

	default:
		return nil
	}
}
func (m Factory) OpenDb(dsn string) (*sql.DB, error) {
	return sql.Open("pgx", dsn)
}

var dbTableColsCache = make(map[string][]define.Column)
var dbTableCache = make(map[string]define.ITableStruct)

func init() {
	InitFactory()
}
func InitFactory() {
	define.RegisterFactory("postgres", &factory)
	define.RegisterFactory("Postgres", &factory)
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
		sql += " FROM " + wrapperName(model.Table()) + " "

		if model.Condition() != nil && model.Condition().PayLoads() > 0 {
			cndString, cndData := factory.ConditionToSql(false, model.Condition())
			sql += " WHERE " + cndString
			datas = append(datas, cndData...)
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
			datas = append(datas, size, idx)
			sql += " LIMIT ? OFFSET ?"
		}
		sql += ";"
		var result []define.SqlProto
		scanner, er := define.GetDefaultScanner(model.Model(), model.Columns()...)
		if er != nil {
			panic(er)
		}
		result = append(result, define.SqlProto{PreparedSql: pgSql(sql), Data: datas, Scanner: scanner})
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
			sql += " " + wrapperName(model.Table()) + " SET "
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
				cndString, cndData := factory.ConditionToSql(false, model.Condition())
				sql += " WHERE " + cndString
				datas = append(datas, cndData...)
			}
			result = append(result, define.SqlProto{pgSql(sql), datas, nil})
		}

		return result
	}
	funcMap[define.Insert] = func(models ...define.TableModel) []define.SqlProto {
		var result []define.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "INSERT INTO " + wrapperName(model.Table()) + " ("

			// 构建列名部分
			var columnNames []string
			for _, c := range model.Columns() {
				columnNames = append(columnNames, wrapperName(c))
			}
			sql += strings.Join(columnNames, ",")
			sql += ") VALUES "

			// 处理批量插入
			if model.IsBatch() {
				target := reflect.ValueOf(model.Model())
				if target.Kind() == reflect.Ptr {
					target = target.Elem()
				}
				if target.Kind() != reflect.Slice && target.Kind() != reflect.Array {
					panic(errors.New("batch insert requires slice or array"))
				}

				// 构建值占位符
				placeholderStart := 1
				values := make([]string, target.Len())

				for i := 0; i < target.Len(); i++ {
					item := target.Index(i).Interface()
					itemMap, err := define.StructToMap(item, model.Columns()...)
					if err != nil {
						panic(err)
					}

					placeholders := make([]string, len(model.Columns()))
					for j := range model.Columns() {
						placeholders[j] = fmt.Sprintf("$%d", placeholderStart)
						placeholderStart++
						datas = append(datas, itemMap[model.Columns()[j]])
					}
					values[i] = "(" + strings.Join(placeholders, ",") + ")"
				}
				sql += strings.Join(values, ",")
			} else {
				// 单条插入
				placeholders := make([]string, len(model.Columns()))
				for i := range placeholders {
					placeholders[i] = fmt.Sprintf("$%d", i+1)
				}
				sql += "(" + strings.Join(placeholders, ",") + ")"

				for _, c := range model.Columns() {
					datas = append(datas, model.ColumnDataMap()[c])
				}
			}

			if len(model.PrimaryAutos()) > 0 {
				sql += " RETURNING " + strings.Join(model.PrimaryAutos(), ",")
			}
			sql += ";"

			var scanner define.IRowScanner = nil
			var er error
			if len(model.PrimaryAutos()) > 0 {
				scanner, er = define.GetDefaultScanner(model.Model(), model.PrimaryAutos()...)
				if er != nil {
					panic(er)
				}
			}
			result = append(result, define.SqlProto{PreparedSql: sql, Data: datas, Scanner: scanner})
		}
		return result
	}
	funcMap[define.Delete] = func(models ...define.TableModel) []define.SqlProto {
		var result []define.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "DELETE FROM "
			sql += " " + wrapperName(model.Table())
			if model.Condition() != nil && model.Condition().PayLoads() > 0 {
				cndString, cndData := factory.ConditionToSql(false, model.Condition())
				sql += " WHERE " + cndString
				datas = append(datas, cndData...)
			}
			result = append(result, define.SqlProto{pgSql(sql), datas, nil})
		}
		return result
	}
}
func pgSql(sql string) string {
	n := 0
	for {
		i := strings.Index(sql, "?")
		if i == -1 {
			return sql
		}
		n++
		seed := "$" + fmt.Sprintf("%d", n)
		sql = strings.Replace(sql, "?", seed, 1)
	}
}
func (m Factory) GetCurrentSchema(db *sql.DB) (string, error) {
	dbName := ""
	dbSql := "SELECT CURRENT_SCHEMA;"
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
	dbSql := "SELECT CURRENT_SCHEMA;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return nil, er
	}
	dbName := ""
	if !rows.Next() {
		return nil, errors.New("can not get Schema")
	}
	er = rows.Scan(&dbName)
	tables := make([]string, 0)
	tbSql := fmt.Sprintf("select tablename from pg_tables WHERE schemaname='%s' order by tablename;", dbName)
	rows, er = db.Query(tbSql)
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

var colSql = `
SELECT 
    c.column_name AS columnName,
    c.data_type AS dataType,
    CASE 
        WHEN EXISTS (
            SELECT 1
            FROM pg_constraint pc
            JOIN pg_class t ON pc.conrelid = t.oid
            JOIN pg_attribute a ON a.attnum = ANY(pc.conkey) AND a.attrelid = t.oid
            WHERE 
                pc.contype = 'p' 
                AND t.relname = c.table_name
                AND a.attname = c.column_name
        ) THEN 'YES'
        ELSE ''
    END AS columnKey,
    CASE 
        WHEN c.column_default LIKE 'nextval%' THEN 'ALWAYS'
        ELSE ''
    END AS extra,
    COALESCE(col_description(pg_class.oid, pg_attribute.attnum), '') AS comment
FROM 
    information_schema.columns c
JOIN 
    pg_class 
    ON pg_class.relname = c.table_name
    AND pg_class.relnamespace = (
        SELECT oid 
        FROM pg_namespace 
        WHERE nspname = c.table_schema
    )
JOIN 
    pg_attribute 
    ON pg_attribute.attrelid = pg_class.oid 
    AND pg_attribute.attname = c.column_name
WHERE 
    c.table_schema = $1  -- 替换为 schema 名称
    AND c.table_name = $2  -- 替换为表名称
ORDER BY 
    c.ordinal_position;

`

func (m Factory) GetTableStruct(tableName string, db *sql.DB) (define.ITableStruct, error) {
	var tableStruct define.TableStruct
	if table, ok := dbTableCache[tableName]; ok {
		return table, nil
	}
	dbSql := "SELECT CURRENT_SCHEMA;"
	rows, er := db.Query(dbSql)
	if er != nil {
		return nil, er
	}
	schema := ""
	if !rows.Next() {
		return nil, errors.New("can not get Schema")
	}
	er = rows.Scan(&schema)
	if er != nil {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}
	//查询表信息
	tbSql := `
SELECT
    table_name,
    COALESCE(obj_description(('"' || table_schema || '"."' || table_name || '"')::regclass, 'pg_class'),'') AS comment
FROM
    information_schema.tables
WHERE
    table_schema = $1 
  AND table_name = $2;`

	//-------------------
	tbRow, er := db.Query(tbSql, schema, tableName)
	if er != nil {
		return nil, er
	}
	tbName := ""
	var tbComment interface{} = ""
	if !tbRow.Next() {
		return nil, errors.New("can not get Schema")
	}
	er = tbRow.Scan(&tbName, &tbComment)
	if tbComment == nil {
		tbComment = ""
	}
	if er != nil {
		return nil, errors.New(fmt.Sprintf("column of table %s was empty", tableName))
	}
	if cols, ok := dbTableColsCache[tableName]; ok {
		tableStruct = define.TableStruct{tbName, tbComment.(string), cols}
	} else {
		rows, er = db.Query(colSql, schema, tableName)
		if er != nil {
			return nil, er
		}
		cols := make([]define.Column, 0)
		for rows.Next() {
			columnName := ""
			columnType := ""
			columnKey := ""
			extra := ""
			comment := ""
			er = rows.Scan(&columnName, &columnType, &columnKey, &extra, &comment)
			if er == nil {
				cols = append(cols, define.Column{ColumnName: columnName, ColumnTypeName: columnType, IsPrimary: columnKey == "YES", IsPrimaryAuto: columnKey == "YES" && extra == "ALWAYS", Comment: comment})
			} else {
				return nil, er
			}
		}
		dbTableColsCache[tableName] = cols
		tableStruct = define.TableStruct{tbName, tbComment.(string), cols}
	}

	dbTableCache[tableName] = tableStruct
	return tableStruct, nil
}
func (m Factory) GetColumns(tableName string, db *sql.DB) ([]define.Column, error) {
	if cols, ok := dbTableColsCache[tableName]; ok {
		return cols, nil
	}
	dbSql := "SELECT CURRENT_SCHEMA;"
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
	rows, er = db.Query(colSql, dbName, tableName)
	columns := make([]define.Column, 0)
	for rows.Next() {
		columnName := ""
		columnType := ""
		columnKey := ""
		extra := ""
		comment := ""
		er = rows.Scan(&columnName, &columnType, &columnKey, &extra, &comment)
		if er == nil {
			columns = append(columns, define.Column{ColumnName: columnName, ColumnTypeName: columnType, IsPrimary: columnKey == "YES", IsPrimaryAuto: columnKey == "YES" && extra == "ALWAYS", Comment: comment})
		} else {
			return nil, er
		}
	}
	dbTableColsCache[tableName] = columns
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
	keys := strings.Split("A,ABORT,ABS,ABSENT,ABSOLUTE,ACCESS,ACCORDING,ACTION,ADA,ADD,ADMIN,AFTER,AGGREGATE,ALL,ALLOCATE,ALSO,ALTER,ALWAYS,ANALYSE,ANALYZE,AND,ANY,ARE,ARRAY,ARRAY_AGG,ARRAY_MAX_CARDINALITY,AS,ASC,ASENSITIVE,ASSERTION,ASSIGNMENT,ASYMMETRIC,AT,ATOMIC,ATTRIBUTE,ATTRIBUTES,AUTHORIZATION,AVG,BACKWARD,BASE64,BEFORE,BEGIN,BEGIN_FRAME,BEGIN_PARTITION,BERNOULLI,BETWEEN,BIGINT,BINARY,BIT,BIT_LENGTH,BLOB,BLOCKED,BOM,BOOLEAN,BOTH,BREADTH,BY,C,CACHE,CALL,CALLED,CARDINALITY,CASCADE,CASCADED,CASE,CAST,CATALOG,CATALOG_NAME,CEIL,CEILING,CHAIN,CHAR,CHARACTER,CHARACTERISTICS,CHARACTERS,CHARACTER_LENGTH,CHARACTER_SET_CATALOG,CHARACTER_SET_NAME,CHARACTER_SET_SCHEMA,CHAR_LENGTH,CHECK,CHECKPOINT,CLASS,CLASS_ORIGIN,CLOB,CLOSE,CLUSTER,COALESCE,COBOL,COLLATE,COLLATION,COLLATION_CATALOG,COLLATION_NAME,COLLATION_SCHEMA,COLLECT,COLUMN,COLUMNS,COLUMN_NAME,COMMAND_FUNCTION,COMMAND_FUNCTION_CODE,COMMENT,COMMENTS,COMMIT,COMMITTED,CONCURRENTLY,CONDITION,CONDITION_NUMBER,CONFIGURATION,CONNECT,CONNECTION,CONNECTION_NAME,CONSTRAINT,CONSTRAINTS,CONSTRAINT_CATALOG,CONSTRAINT_NAME,CONSTRAINT_SCHEMA,CONSTRUCTOR,CONTAINS,CONTENT,CONTINUE,CONTROL,CONVERSION,CONVERT,COPY,CORR,CORRESPONDING,COST,COUNT,COVAR_POP,COVAR_SAMP,CREATE,CROSS,CSV,CUBE,CUME_DIST,CURRENT,CURRENT_CATALOG,CURRENT_DATE,CURRENT_DEFAULT_TRANSFORM_GROUP,CURRENT_PATH,CURRENT_ROLE,CURRENT_ROW,CURRENT_SCHEMA,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_TRANSFORM_GROUP_FOR_TYPE,CURRENT_USER,CURSOR,CURSOR_NAME,CYCLE,DATA,DATABASE,DATALINK,DATE,DATETIME_INTERVAL_CODE,DATETIME_INTERVAL_PRECISION,DAY,DB,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFAULT,DEFAULTS,DEFERRABLE,DEFERRED,DEFINED,DEFINER,DEGREE,DELETE,DELIMITER,DELIMITERS,DENSE_RANK,DEPTH,DEREF,DERIVED,DESC,DESCRIBE,DESCRIPTOR,DETERMINISTIC,DIAGNOSTICS,DICTIONARY,DISABLE,DISCARD,DISCONNECT,DISPATCH,DISTINCT,DLNEWCOPY,DLPREVIOUSCOPY,DLURLCOMPLETE,DLURLCOMPLETEONLY,DLURLCOMPLETEWRITE,DLURLPATH,DLURLPATHONLY,DLURLPATHWRITE,DLURLSCHEME,DLURLSERVER,DLVALUE,DO,DOCUMENT,DOMAIN,DOUBLE,DROP,DYNAMIC,DYNAMIC_FUNCTION,DYNAMIC_FUNCTION_CODE,EACH,ELEMENT,ELSE,EMPTY,ENABLE,ENCODING,ENCRYPTED,END,END-EXEC,END_FRAME,END_PARTITION,ENFORCED,ENUM,EQUALS,ESCAPE,EVENT,EVERY,EXCEPT,EXCEPTION,EXCLUDE,EXCLUDING,EXCLUSIVE,EXEC,EXECUTE,EXISTS,EXP,EXPLAIN,EXPRESSION,EXTENSION,EXTERNAL,EXTRACT,FALSE,FAMILY,FETCH,FILE,FILTER,FINAL,FIRST,FIRST_VALUE,FLAG,FLOAT,FLOOR,FOLLOWING,FOR,FORCE,FOREIGN,FORTRAN,FORWARD,FOUND,FRAME_ROW,FREE,FREEZE,FROM,FS,FULL,FUNCTION,FUNCTIONS,FUSION,G,GENERAL,GENERATED,GET,GLOBAL,GO,GOTO,GRANT,GRANTED,GREATEST,GROUP,GROUPING,GROUPS,HANDLER,HAVING,HEADER,HEX,HIERARCHY,HOLD,HOUR,ID,IDENTITY,IF,IGNORE,ILIKE,IMMEDIATE,IMMEDIATELY,IMMUTABLE,IMPLEMENTATION,IMPLICIT,IMPORT,IN,INCLUDING,INCREMENT,INDENT,INDEX,INDEXES,INDICATOR,INHERIT,INHERITS,INITIALLY,INLINE,INNER,INOUT,INPUT,INSENSITIVE,INSERT,INSTANCE,INSTANTIABLE,INSTEAD,INT,INTEGER,INTEGRITY,INTERSECT,INTERSECTION,INTERVAL,INTO,INVOKER,IS,ISNULL,ISOLATION,JOIN,K,KEY,KEY_MEMBER,KEY_TYPE,LABEL,LAG,LANGUAGE,LARGE,LAST,LAST_VALUE,LATERAL,LC_COLLATE,LC_CTYPE,LEAD,LEADING,LEAKPROOF,LEAST,LEFT,LENGTH,LEVEL,LIBRARY,LIKE,LIKE_REGEX,LIMIT,LINK,LISTEN,LN,LOAD,LOCAL,LOCALTIME,LOCALTIMESTAMP,LOCATION,LOCATOR,LOCK,LOWER,M,MAP,MAPPING,MATCH,MATCHED,MATERIALIZED,MAX,MAXVALUE,MAX_CARDINALITY,MEMBER,MERGE,MESSAGE_LENGTH,MESSAGE_OCTET_LENGTH,MESSAGE_TEXT,METHOD,MIN,MINUTE,MINVALUE,MOD,MODE,MODIFIES,MODULE,MONTH,MORE,MOVE,MULTISET,MUMPS,NAME,NAMES,NAMESPACE,NATIONAL,NATURAL,NCHAR,NCLOB,NESTING,NEW,NEXT,NFC,NFD,NFKC,NFKD,NIL,NO,NONE,NORMALIZE,NORMALIZED,NOT,NOTHING,NOTIFY,NOTNULL,NOWAIT,NTH_VALUE,NTILE,NULL,NULLABLE,NULLIF,NULLS,NUMBER,NUMERIC,OBJECT,OCCURRENCES_REGEX,OCTETS,OCTET_LENGTH,OF,OFF,OFFSET,OIDS,OLD,ON,ONLY,OPEN,OPERATOR,OPTION,OPTIONS,OR,ORDER,ORDERING,ORDINALITY,OTHERS,OUT,OUTER,OUTPUT,OVER,OVERLAPS,OVERLAY,OVERRIDING,OWNED,OWNER,P,PAD,PARAMETER,PARAMETER_MODE,PARAMETER_NAME,PARAMETER_ORDINAL_POSITION,PARAMETER_SPECIFIC_CATALOG,PARAMETER_SPECIFIC_NAME,PARAMETER_SPECIFIC_SCHEMA,PARSER,PARTIAL,PARTITION,PASCAL,PASSING,PASSTHROUGH,PASSWORD,PATH,PERCENT,PERCENTILE_CONT,PERCENTILE_DISC,PERCENT_RANK,PERIOD,PERMISSION,PLACING,PLANS,PLI,PORTION,POSITION,POSITION_REGEX,POWER,PRECEDES,PRECEDING,PRECISION,PREPARE,PREPARED,PRESERVE,PRIMARY,PRIOR,PRIVILEGES,PROCEDURAL,PROCEDURE,PROGRAM,PUBLIC,QUOTE,RANGE,RANK,READ,READS,REAL,REASSIGN,RECHECK,RECOVERY,RECURSIVE,REF,REFERENCES,REFERENCING,REFRESH,REGR_AVGX,REGR_AVGY,REGR_COUNT,REGR_INTERCEPT,REGR_R2,REGR_SLOPE,REGR_SXX,REGR_SXY,REGR_SYY,REINDEX,RELATIVE,RELEASE,RENAME,REPEATABLE,REPLACE,REPLICA,REQUIRING,RESET,RESPECT,RESTART,RESTORE,RESTRICT,RESULT,RETURN,RETURNED_CARDINALITY,RETURNED_LENGTH,RETURNED_OCTET_LENGTH,RETURNED_SQLSTATE,RETURNING,RETURNS,REVOKE,RIGHT,ROLE,ROLLBACK,ROLLUP,ROUTINE,ROUTINE_CATALOG,ROUTINE_NAME,ROUTINE_SCHEMA,ROW,ROWS,ROW_COUNT,ROW_NUMBER,RULE,SAVEPOINT,SCALE,SCHEMA,SCHEMA_NAME,SCOPE,SCOPE_CATALOG,SCOPE_NAME,SCOPE_SCHEMA,SCROLL,SEARCH,SECOND,SECTION,SECURITY,SELECT,SELECTIVE,SELF,SENSITIVE,SEQUENCE,SEQUENCES,SERIALIZABLE,SERVER,SERVER_NAME,SESSION,SESSION_USER,SET,SETOF,SETS,SHARE,SHOW,SIMILAR,SIMPLE,SIZE,SMALLINT,SNAPSHOT,SOME,SOURCE,SPACE,SPECIFIC,SPECIFICTYPE,SPECIFIC_NAME,SQL,SQLCODE,SQLERROR,SQLEXCEPTION,SQLSTATE,SQLWARNING,SQRT,STABLE,STANDALONE,START,STATE,STATEMENT,STATIC,STATISTICS,STDDEV_POP,STDDEV_SAMP,STDIN,STDOUT,STORAGE,STRICT,STRIP,STRUCTURE,STYLE,SUBCLASS_ORIGIN,SUBMULTISET,SUBSTRING,SUBSTRING_REGEX,SUCCEEDS,SUM,SYMMETRIC,SYSID,SYSTEM,SYSTEM_TIME,SYSTEM_USER,T,TABLE,TABLES,TABLESAMPLE,TABLESPACE,TABLE_NAME,TEMP,TEMPLATE,TEMPORARY,TEXT,THEN,TIES,TIME,TIMESTAMP,TIMEZONE_HOUR,TIMEZONE_MINUTE,TO,TOKEN,TOP_LEVEL_COUNT,TRAILING,TRANSACTION,TRANSACTIONS_COMMITTED,TRANSACTIONS_ROLLED_BACK,TRANSACTION_ACTIVE,TRANSFORM,TRANSFORMS,TRANSLATE,TRANSLATE_REGEX,TRANSLATION,TREAT,TRIGGER,TRIGGER_CATALOG,TRIGGER_NAME,TRIGGER_SCHEMA,TRIM,TRIM_ARRAY,TRUE,TRUNCATE,TRUSTED,TYPE,TYPES,UESCAPE,UNBOUNDED,UNCOMMITTED,UNDER,UNENCRYPTED,UNION,UNIQUE,UNKNOWN,UNLINK,UNLISTEN,UNLOGGED,UNNAMED,UNNEST,UNTIL,UNTYPED,UPDATE,UPPER,URI,USAGE,USER,USER_DEFINED_TYPE_CATALOG,USER_DEFINED_TYPE_CODE,USER_DEFINED_TYPE_NAME,USER_DEFINED_TYPE_SCHEMA,USING,VACUUM,VALID,VALIDATE,VALIDATOR,VALUE,VALUES,VALUE_OF,VARBINARY,VARCHAR,VARIADIC,VARYING,VAR_POP,VAR_SAMP,VERBOSE,VERSION,VERSIONING,VIEW,VOLATILE,WHEN,WHENEVER,WHERE,WHITESPACE,WIDTH_BUCKET,WINDOW,WITH,WITHIN,WITHOUT,WORK,WRAPPER,WRITE,XML,XMLAGG,XMLATTRIBUTES,XMLBINARY,XMLCAST,XMLCOMMENT,XMLCONCAT,XMLDECLARATION,XMLDOCUMENT,XMLELEMENT,XMLEXISTS,XMLFOREST,XMLITERATE,XMLNAMESPACES,XMLPARSE,XMLPI,XMLQUERY,XMLROOT,XMLSCHEMA,XMLSERIALIZE,XMLTABLE,XMLTEXT,XMLVALIDATE,YEAR,YES,ZONE", ",")
	for _, key := range keys {
		keywordMap[key] = key
	}
}

func wrapperName(name string) string {
	if _, ok := keywordMap[name]; ok {
		return "\"" + name + "\""
	}
	if _, ok := keywordMap[strings.ToUpper(name)]; ok {
		return "\"" + name + "\""
	}
	name = strings.TrimSpace(name)
	lowCaseName := strings.ToLower(name)
	if lowCaseName == name {
		return name
	}
	return "\"" + name + "\""
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
