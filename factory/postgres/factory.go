package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kmlixh/gom/v4/define"
)

// Factory represents a PostgreSQL query builder
type Factory struct{}

// GetType returns the database type
func (f *Factory) GetType() string {
	return "postgres"
}

func init() {
	RegisterFactory()
}

func RegisterFactory() {
	define.RegisterFactory("postgres", &Factory{})
}

func (f *Factory) Connect(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// getOperator returns the SQL operator for the given operation type
func (f *Factory) getOperator(op define.OpType) string {
	switch op {
	case define.OpEq:
		return "="
	case define.OpNe:
		return "!="
	case define.OpGt:
		return ">"
	case define.OpGe:
		return ">="
	case define.OpLt:
		return "<"
	case define.OpLe:
		return "<="
	case define.OpLike:
		return "LIKE"
	case define.OpNotLike:
		return "NOT LIKE"
	case define.OpIn:
		return "IN"
	case define.OpNotIn:
		return "NOT IN"
	case define.OpIsNull:
		return "IS NULL"
	case define.OpIsNotNull:
		return "IS NOT NULL"
	case define.OpBetween:
		return "BETWEEN"
	case define.OpNotBetween:
		return "NOT BETWEEN"
	default:
		return "="
	}
}

// buildCondition builds a single condition clause
func (f *Factory) buildCondition(cond *define.Condition, startParamIndex int) (string, []interface{}) {
	if cond == nil {
		return "", nil
	}

	var condStrs []string
	var args []interface{}
	currentParamIndex := startParamIndex

	// First build the current condition
	if cond.Field != "" {
		sql, arg := f.buildSimpleCondition(cond, currentParamIndex)
		if sql != "" {
			condStrs = append(condStrs, sql)
			args = append(args, arg...)
			currentParamIndex += len(arg)
		}
	}

	// Then build sub-conditions
	if len(cond.SubConds) > 0 {
		for _, subCond := range cond.SubConds {
			subStr, subArg := f.buildCondition(subCond, currentParamIndex)
			if subStr != "" {
				if len(condStrs) > 0 {
					if subCond.JoinType == define.JoinOr {
						condStrs = append(condStrs, "OR")
					} else {
						condStrs = append(condStrs, "AND")
					}
				}
				if strings.Contains(subStr, " AND ") || strings.Contains(subStr, " OR ") {
					if !strings.HasPrefix(subStr, "(") {
						subStr = "(" + subStr + ")"
					}
				}
				condStrs = append(condStrs, subStr)
				args = append(args, subArg...)
				currentParamIndex += len(subArg)
			}
		}
	}

	if len(condStrs) > 0 {
		if cond.IsSubGroup {
			return "(" + strings.Join(condStrs, " ") + ")", args
		}
		return strings.Join(condStrs, " "), args
	}
	return "", nil
}

var dbTableColsCache = make(map[string][]define.Column)
var colSql = `
SELECT 
    c.column_name AS "columnName",
    c.data_type AS "dataType",
    c.is_identity AS "columnKey",
    COALESCE(c.identity_generation, 'NO') AS extra,
    COALESCE(col_description(t.oid, a.attnum),'') AS comment
FROM 
    information_schema.columns c
JOIN 
    pg_class t ON t.relname = c.table_name
JOIN 
    pg_attribute a ON a.attrelid = t.oid AND a.attname = c.column_name
WHERE 
    c.table_schema = $1  
    AND c.table_name = $2
    AND a.attnum > 0
ORDER BY 
    c.ordinal_position;
`

func (m *Factory) GetColumns(tableName string, db *sql.DB) ([]define.Column, error) {
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
	if cols, ok := dbTableColsCache[tableName]; ok {
		return cols, nil
	}
	rows, er = db.Query(colSql, dbName, tableName)
	columns := make([]define.Column, 0)
	for rows.Next() {
		columnName := ""
		columnType := ""
		columnKey := ""
		extra := ""
		comment := ""
		er = rows.Scan(&columnName, &columnType, &columnKey, &extra)
		if er == nil {
			columns = append(columns, define.Column{ColumnName: columnName, ColumnType: columnType, Primary: columnKey == "YES", PrimaryAuto: columnKey == "YES" && extra == "ALWAYS", Comment: comment})
		} else {
			return nil, er
		}
	}
	dbTableColsCache[tableName] = columns
	return columns, nil

}

// buildSimpleCondition builds a simple condition without sub-conditions
func (f *Factory) buildSimpleCondition(cond *define.Condition, startParamIndex int) (string, []interface{}) {
	if cond == nil || cond.Field == "" {
		return "", nil
	}

	field := cond.Field
	op := f.getOperator(cond.Op)

	switch cond.Op {
	case define.OpIn, define.OpNotIn:
		if values, ok := cond.Value.([]interface{}); ok && len(values) > 0 {
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = fmt.Sprintf("$%d", startParamIndex+i)
			}
			return fmt.Sprintf("%s %v (%s)", field, op, strings.Join(placeholders, ", ")), values
		}
		return "", nil
	case define.OpIsNull, define.OpIsNotNull:
		return fmt.Sprintf("%s %v", field, op), nil
	case define.OpBetween, define.OpNotBetween:
		if values, ok := cond.Value.([]interface{}); ok && len(values) == 2 {
			return fmt.Sprintf("%s %v $%d AND $%d", field, op, startParamIndex, startParamIndex+1), values
		}
		return "", nil
	default:
		return fmt.Sprintf("%s %v $%d", field, op, startParamIndex), []interface{}{cond.Value}
	}
}

// quoteIdentifier properly quotes PostgreSQL identifiers
func (f *Factory) quoteIdentifier(identifier string) string {
	parts := strings.Split(identifier, ".")
	for i, part := range parts {
		if part != "*" {
			parts[i] = fmt.Sprintf(`"%s"`, strings.ReplaceAll(part, `"`, `""`))
		}
	}
	return strings.Join(parts, ".")
}

// BuildSelect builds a SELECT query for PostgreSQL
func (f *Factory) BuildSelect(table string, fields []string, conditions []*define.Condition, orderBy string, limit, offset int) (string, []interface{}) {
	var args []interface{}
	query := "SELECT "

	// Add fields
	if len(fields) > 0 {
		var quotedFields []string
		for _, field := range fields {
			if strings.Contains(field, "(") && strings.Contains(field, ")") {
				// Don't quote aggregate functions
				quotedFields = append(quotedFields, field)
			} else if field == "*" {
				quotedFields = append(quotedFields, field)
			} else {
				quotedFields = append(quotedFields, f.quoteIdentifier(field))
			}
		}
		query += strings.Join(quotedFields, ", ")
	} else {
		query += "*"
	}

	// Add table
	query += fmt.Sprintf(" FROM %s", f.quoteIdentifier(table))

	// Add conditions
	if len(conditions) > 0 && conditions[0] != nil {
		query += " WHERE "
		var condStrings []string
		currentParamIndex := 1
		for i, cond := range conditions {
			if cond == nil {
				continue
			}
			condStr, condArgs := f.buildCondition(cond, currentParamIndex)
			if condStr != "" {
				if i > 0 {
					if cond.JoinType == define.JoinOr {
						condStrings = append(condStrings, "OR")
					} else {
						condStrings = append(condStrings, "AND")
					}
				}
				condStrings = append(condStrings, condStr)
				args = append(args, condArgs...)
				currentParamIndex += len(condArgs)
			}
		}
		if len(condStrings) > 0 {
			query += strings.Join(condStrings, " ")
		}
	}

	// Add order by
	if orderBy != "" {
		if strings.HasPrefix(strings.ToUpper(orderBy), "ORDER BY") {
			query += " " + orderBy
		} else {
			query += " ORDER BY " + orderBy
		}
	}

	// Add limit and offset
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	return query, args
}

// BuildUpdate builds an UPDATE query for PostgreSQL
func (f *Factory) BuildUpdate(table string, fields map[string]interface{}, fieldOrder []string, conditions []*define.Condition) (string, []interface{}) {
	var args []interface{}
	query := fmt.Sprintf("UPDATE %s SET ", f.quoteIdentifier(table))

	// Use fieldOrder to maintain field order
	var fieldStrings []string
	usedFields := make(map[string]bool)

	// First add fields in the specified order
	for _, field := range fieldOrder {
		if value, ok := fields[field]; ok {
			args = append(args, value)
			fieldStrings = append(fieldStrings, fmt.Sprintf("%s = $%d", f.quoteIdentifier(field), len(args)))
			usedFields[field] = true
		}
	}

	// Then add any remaining fields
	for field, value := range fields {
		if !usedFields[field] {
			args = append(args, value)
			fieldStrings = append(fieldStrings, fmt.Sprintf("%s = $%d", f.quoteIdentifier(field), len(args)))
		}
	}

	if len(fieldStrings) == 0 {
		// If no fields to update, return empty query
		return "", nil
	}

	query += strings.Join(fieldStrings, ", ")

	// Add conditions
	if len(conditions) > 0 && conditions[0] != nil {
		query += " WHERE "
		var condStrings []string
		currentParamIndex := len(args) + 1
		for i, cond := range conditions {
			if cond == nil {
				continue
			}
			condStr, condArgs := f.buildCondition(cond, currentParamIndex)
			if condStr != "" {
				if i > 0 {
					if cond.JoinType == define.JoinOr {
						condStrings = append(condStrings, "OR")
					} else {
						condStrings = append(condStrings, "AND")
					}
				}
				condStrings = append(condStrings, condStr)
				args = append(args, condArgs...)
				currentParamIndex += len(condArgs)
			}
		}
		if len(condStrings) > 0 {
			query += strings.Join(condStrings, " ")
		}
	}

	return query, args
}

// BuildInsert builds an INSERT query for PostgreSQL
func (f *Factory) BuildInsert(table string, fields map[string]interface{}, fieldOrder []string) (string, []interface{}) {
	if len(fields) == 0 {
		return "", nil
	}

	// Build field list and value placeholders
	var args []interface{}
	var quotedFields []string
	var placeholders []string
	usedFields := make(map[string]bool)

	// First add fields in the specified order
	for _, field := range fieldOrder {
		if value, ok := fields[field]; ok {
			quotedFields = append(quotedFields, f.quoteIdentifier(field))
			args = append(args, value)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
			usedFields[field] = true
		}
	}

	// Then add any remaining fields
	for field, value := range fields {
		if !usedFields[field] {
			quotedFields = append(quotedFields, f.quoteIdentifier(field))
			args = append(args, value)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		f.quoteIdentifier(table),
		strings.Join(quotedFields, ", "),
		strings.Join(placeholders, ", "))

	return query, args
}

// quoteIdentifiers quotes multiple identifiers
func (f *Factory) quoteIdentifiers(identifiers []string) []string {
	quoted := make([]string, len(identifiers))
	for i, id := range identifiers {
		quoted[i] = f.quoteIdentifier(id)
	}
	return quoted
}

// BuildBatchInsert builds a batch INSERT query for PostgreSQL
func (f *Factory) BuildBatchInsert(table string, batchFields []map[string]interface{}) (string, []interface{}) {
	if len(batchFields) == 0 {
		return "", nil
	}

	// Get all unique field names
	fieldSet := make(map[string]struct{})
	for _, fields := range batchFields {
		for field := range fields {
			fieldSet[field] = struct{}{}
		}
	}

	// Convert to sorted slice for consistent order
	var fieldNames []string
	for field := range fieldSet {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)

	var args []interface{}
	var valueStrings []string

	// Build value lists
	for _, fields := range batchFields {
		var valuePlaceholders []string
		for _, field := range fieldNames {
			if value, ok := fields[field]; ok {
				args = append(args, value)
				valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("$%d", len(args)))
			} else {
				valuePlaceholders = append(valuePlaceholders, "NULL")
			}
		}
		valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(valuePlaceholders, ", ")))
	}

	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`,
		f.quoteIdentifier(table),
		strings.Join(f.quoteIdentifiers(fieldNames), ", "),
		strings.Join(valueStrings, ", "))

	return query, args
}

// BuildDelete builds a DELETE query for PostgreSQL
func (f *Factory) BuildDelete(table string, conditions []*define.Condition) (string, []interface{}) {
	var args []interface{}
	query := fmt.Sprintf("DELETE FROM %s", f.quoteIdentifier(table))

	// Add conditions
	if len(conditions) > 0 && conditions[0] != nil {
		query += " WHERE "
		var condStrings []string
		currentParamIndex := 1
		for i, cond := range conditions {
			if cond == nil {
				continue
			}
			condStr, condArgs := f.buildCondition(cond, currentParamIndex)
			if condStr != "" {
				if i > 0 {
					if cond.JoinType == define.JoinOr {
						condStrings = append(condStrings, "OR")
					} else {
						condStrings = append(condStrings, "AND")
					}
				}
				condStrings = append(condStrings, condStr)
				args = append(args, condArgs...)
				currentParamIndex += len(condArgs)
			}
		}
		if len(condStrings) > 0 {
			query += strings.Join(condStrings, " ")
		}
	}

	return query, args
}

// BuildCreateTable builds a CREATE TABLE query for PostgreSQL
func (f *Factory) BuildCreateTable(table string, modelType reflect.Type) string {
	var fieldDefs []string

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		parts := strings.Split(tag, ",")
		columnName := parts[0]
		columnConstraints := parts[1:]

		var columnType string
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int32:
			columnType = "INTEGER"
		case reflect.Int8, reflect.Int16:
			columnType = "SMALLINT"
		case reflect.Int64:
			if columnName == "id" {
				columnType = "BIGSERIAL"
			} else {
				columnType = "BIGINT"
			}
		case reflect.Uint, reflect.Uint32:
			columnType = "INTEGER"
		case reflect.Uint8, reflect.Uint16:
			columnType = "SMALLINT"
		case reflect.Uint64:
			columnType = "BIGINT"
		case reflect.Float32:
			columnType = "REAL"
		case reflect.Float64:
			columnType = "DOUBLE PRECISION"
		case reflect.Bool:
			columnType = "BOOLEAN"
		case reflect.String:
			columnType = "VARCHAR(255)"
		case reflect.Struct:
			if field.Type == reflect.TypeOf(time.Time{}) {
				columnType = "TIMESTAMP"
			}
		case reflect.Ptr:
			if field.Type.Elem() == reflect.TypeOf(time.Time{}) {
				columnType = "TIMESTAMP"
			}
		}

		columnDef := fmt.Sprintf(`"%s" %s`, columnName, columnType)

		// Add constraints
		isNull := field.Type.Kind() == reflect.Ptr // 指针类型默认可为空
		for _, constraint := range columnConstraints {
			switch constraint {
			case "@":
				if columnName == "id" {
					columnDef += " PRIMARY KEY"
				}
			case "notnull", "!":
				isNull = false
				columnDef += " NOT NULL"
			case "null":
				isNull = true
			case "unique", "~":
				columnDef += " UNIQUE"
			case "default":
				if field.Type == reflect.TypeOf(time.Time{}) {
					columnDef += " DEFAULT CURRENT_TIMESTAMP"
				}
			}
		}

		if !isNull && !strings.Contains(columnDef, "NOT NULL") {
			columnDef += " NOT NULL"
		}

		fieldDefs = append(fieldDefs, columnDef)
	}

	// 将字段定义组合成 SQL 语句
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (`, table)
	query += strings.Join(fieldDefs, ", ")
	query += ")"

	// 去除多余的空白字符
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	query = strings.ReplaceAll(query, "  ", " ")

	return query
}

// GetTableInfo 获取表信息
func (f *Factory) GetTableInfo(db *sql.DB, tableName string) (*define.TableInfo, error) {
	// 获取表基本信息
	var tableInfo define.TableInfo
	tableInfo.TableName = tableName

	// 获取表注释
	query := `SELECT COALESCE(obj_description(c.oid), '') as table_comment
			 FROM pg_class c
			 WHERE c.relname = $1`

	err := db.QueryRow(query, tableName).Scan(&tableInfo.TableComment)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("获取表注释失败: %v", err)
	}

	// 获取列信息
	query = `SELECT 
				a.attname as column_name,
				format_type(a.atttypid, a.atttypmod) as data_type,
				CASE 
					WHEN format_type(a.atttypid, a.atttypmod) LIKE 'character varying%' 
					THEN regexp_replace(format_type(a.atttypid, a.atttypmod), 'character varying\((\d+)\)', '\1')::integer
					ELSE null
				END as character_maximum_length,
				CASE 
					WHEN format_type(a.atttypid, a.atttypmod) LIKE 'numeric%' 
					THEN split_part(regexp_replace(format_type(a.atttypid, a.atttypmod), 'numeric\((\d+),(\d+)\)', '\1'), ',', 1)::integer
					ELSE null
				END as numeric_precision,
				CASE 
					WHEN format_type(a.atttypid, a.atttypmod) LIKE 'numeric%' 
					THEN split_part(regexp_replace(format_type(a.atttypid, a.atttypmod), 'numeric\((\d+),(\d+)\)', '\2'), ',', 1)::integer
					ELSE null
				END as numeric_scale,
				CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
				CASE WHEN pk.contype = 'p' THEN 'PRI' ELSE '' END as column_key,
				CASE WHEN a.attidentity = 'a' THEN 'auto_increment' ELSE '' END as extra,
				COALESCE(col_description(a.attrelid, a.attnum), '') as column_comment
			FROM pg_attribute a
			LEFT JOIN pg_constraint pk 
				ON pk.conrelid = a.attrelid 
				AND pk.contype = 'p' 
				AND a.attnum = ANY(pk.conkey)
			WHERE a.attrelid = $1::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
			ORDER BY a.attnum`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col define.ColumnInfo
		var maxLength sql.NullInt64
		var precision sql.NullInt64
		var scale sql.NullInt64
		var isNullable string
		var columnKey string
		var extra string

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&maxLength,
			&precision,
			&scale,
			&isNullable,
			&columnKey,
			&extra,
			&col.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		col.IsNullable = isNullable == "YES"
		col.IsPrimaryKey = columnKey == "PRI"
		col.IsAutoIncrement = extra == "auto_increment"

		if maxLength.Valid {
			col.Length = maxLength.Int64
		}
		if precision.Valid {
			col.Precision = int(precision.Int64)
		}
		if scale.Valid {
			col.Scale = int(scale.Int64)
		}

		tableInfo.Columns = append(tableInfo.Columns, col)
	}

	return &tableInfo, nil
}

// GetTables 获取符合模式的所有表
func (f *Factory) GetTables(db *sql.DB, pattern string) ([]string, error) {
	var tables []string
	var schema, tablePattern string

	// 解析模式
	parts := strings.Split(pattern, ".")
	if len(parts) == 2 {
		// 格式为 schema.table
		schema = parts[0]
		tablePattern = parts[1]
	} else if pattern == "*" {
		// 默认查询 public schema
		schema = "public"
		tablePattern = "*"
	} else {
		// 只有表名模式，使用 public schema
		schema = "public"
		tablePattern = pattern
	}

	// 将 * 转换为 SQL LIKE 模式
	tablePattern = strings.ReplaceAll(tablePattern, "*", "%")

	query := `
		SELECT schemaname || '.' || tablename
		FROM pg_catalog.pg_tables
		WHERE schemaname = $1
		AND tablename LIKE $2
		ORDER BY schemaname, tablename`

	rows, err := db.Query(query, schema, tablePattern)
	if err != nil {
		return nil, fmt.Errorf("查询表列表失败: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %v", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// BuildOrderBy 构建排序语句
func (f *Factory) BuildOrderBy(orders []define.OrderBy) string {
	if len(orders) == 0 {
		return ""
	}

	var parts []string
	for _, order := range orders {
		part := fmt.Sprintf(`"%s"`, order.Field)
		if order.Type == define.OrderDesc {
			part += " DESC"
		} else {
			part += " ASC"
		}
		parts = append(parts, part)
	}

	return "ORDER BY " + strings.Join(parts, ", ")
}
