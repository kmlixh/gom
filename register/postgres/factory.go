package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kmlixh/gom/v2/defines"
	"github.com/kmlixh/gom/v2/register"
	"strings"
)

type MyCndStruct struct {
	Linker     string
	Expression string
	Data       []interface{}
}

var keywordMap map[string]string

var funcMap map[defines.SqlType]defines.SqlFunc

var factory = Factory{}

type Factory struct {
}

func (m Factory) OpenDb(dsn string) (*sql.DB, error) {
	return sql.Open("pgx/v5", dsn)
}

var dbTableColsCache = make(map[string][]defines.Column)

func init() {
	register.Register("Postgres", &factory)
	InitPgFactory()
}
func InitPgFactory() {
	initKeywordMap()
	funcMap = make(map[defines.SqlType]defines.SqlFunc)
	funcMap[defines.Query] = func(models ...defines.TableModel) []defines.SqlProto {
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
				if model.OrderBys()[i].Type() == defines.Asc {
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
			sql += " LIMIT ? OFFSET ?"
		}
		sql += ";"
		var result []defines.SqlProto
		result = append(result, defines.SqlProto{PreparedSql: pgSql(sql), Data: datas})
		return result
	}
	funcMap[defines.Update] = func(models ...defines.TableModel) []defines.SqlProto {
		if models == nil || len(models) == 0 {
			panic(errors.New("model was nil or empty"))
		}
		var result []defines.SqlProto
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
			conditionSql, dds := factory.ConditionToSql(false, model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, defines.SqlProto{pgSql(sql), datas})
		}

		return result
	}
	funcMap[defines.Insert] = func(models ...defines.TableModel) []defines.SqlProto {
		var result []defines.SqlProto
		for _, model := range models {
			var datas []interface{}

			sql := "INSERT INTO " + wrapperName(model.Table()) + " ("
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
			result = append(result, defines.SqlProto{pgSql(sql), datas})
		}
		return result
	}
	funcMap[defines.Delete] = func(models ...defines.TableModel) []defines.SqlProto {
		var result []defines.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "DELETE FROM "
			sql += " " + wrapperName(model.Table())
			conditionSql, dds := factory.ConditionToSql(false, model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, defines.SqlProto{pgSql(sql), datas})
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
func (m Factory) GetColumns(tableName string, db *sql.DB) ([]defines.Column, error) {

	if cols, ok := dbTableColsCache[tableName]; ok {
		return cols, nil
	}
	colSql := "select column_name as \"columnName\",data_type  as \"dataType\",is_identity as \"columnKey\",coalesce(identity_generation,'NO') as extra from information_schema.columns where table_schema='public' and table_name='%s' order by ordinal_position;\n"
	colSql = fmt.Sprintf(colSql, tableName)
	st, er := db.Prepare(colSql)
	if er != nil {
		return nil, er
	}
	rows, er := st.Query()
	columns := make([]defines.Column, 0)
	for rows.Next() {
		columnName := ""
		columnType := ""
		columnKey := ""
		extra := ""
		er = rows.Scan(&columnName, &columnType, &columnKey, &extra)
		if er == nil {
			columns = append(columns, defines.Column{ColumnName: columnName, ColumnType: columnType, Primary: columnKey == "YES", PrimaryAuto: columnKey == "YES" && extra == "ALWAYS"})
		} else {
			return nil, er
		}
	}
	dbTableColsCache[tableName] = columns
	return columns, nil

}

func (m Factory) GetSqlFunc(sqlType defines.SqlType) defines.SqlFunc {
	return funcMap[sqlType]
}
func (m Factory) ConditionToSql(preTag bool, cnd defines.Condition) (string, []interface{}) {
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

func cndToMyCndStruct(cnd defines.Condition) MyCndStruct {
	if len(cnd.RawExpression()) > 0 {
		return MyCndStruct{linkerToString(cnd), cnd.RawExpression(), cnd.Values()}
	}
	opers := cnd.Field()
	switch cnd.Operation() {
	case defines.Eq:
		opers += " = ? "
	case defines.NotEq:
		opers += " <> ? "
	case defines.Ge:
		opers += " >= ? "
	case defines.Gt:
		opers += " > ? "
	case defines.Le:
		opers += " <= ? "
	case defines.Lt:
		opers += " < ? "
	case defines.In:
		opers += " IN " + valueSpace(len(cnd.Values()))
	case defines.NotIn:
		opers += " NOT IN " + valueSpace(len(cnd.Values()))
	case defines.Like:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case defines.LikeIgnoreStart:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string)
		cnd.SetValues(vals)
	case defines.LikeIgnoreEnd:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = vals[0].(string) + "%"
		cnd.SetValues(vals)
	case defines.IsNull:
		opers += " IS NULL "
	case defines.IsNotNull:
		opers += " IS NOT NULL "
	}
	return MyCndStruct{linkerToString(cnd), opers, cnd.Values()}
}

func linkerToString(cnd defines.Condition) string {
	switch cnd.Linker() {
	case defines.And:
		return " AND "
	case defines.Or:
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
