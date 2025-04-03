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

// buildCondition builds a SQL condition string and returns the condition string and its arguments
func (f *Factory) buildCondition(cond *define.Condition, paramIndex *int) (string, []interface{}) {
	if cond == nil {
		return "", nil
	}

	// First build the current condition
	if cond.Field != "" {
		sql, arg := f.buildSimpleCondition(cond, paramIndex)
		if sql != "" {
			return sql, arg
		}
	}

	// Then build sub-conditions
	if len(cond.SubConds) > 0 {
		var whereConditions []string
		var args []interface{}
		var hasOr bool
		for _, subCond := range cond.SubConds {
			subStr, subArg := f.buildCondition(subCond, paramIndex)
			if subStr != "" {
				if len(whereConditions) > 0 {
					if subCond.JoinType == define.JoinOr {
						hasOr = true
						whereConditions = append(whereConditions, "OR", subStr)
					} else {
						whereConditions = append(whereConditions, "AND", subStr)
					}
				} else {
					whereConditions = append(whereConditions, subStr)
				}
				args = append(args, subArg...)
			}
		}
		if len(whereConditions) > 0 {
			if hasOr {
				return "(" + strings.Join(whereConditions, " ") + ")", args
			}
			return strings.Join(whereConditions, " "), args
		}
	}

	if cond.IsSubGroup {
		var subConditions []string
		var args []interface{}
		for _, subCond := range cond.SubConds {
			subStr, subArg := f.buildCondition(subCond, paramIndex)
			if subStr != "" {
				subConditions = append(subConditions, subStr)
				args = append(args, subArg...)
			}
		}
		if len(subConditions) > 0 {
			return "(" + strings.Join(subConditions, " AND ") + ")", args
		}
	}
	return "", nil
}

// buildSimpleCondition builds a simple condition without sub-conditions
func (f *Factory) buildSimpleCondition(cond *define.Condition, argCount *int) (string, []interface{}) {
	if cond == nil {
		return "", nil
	}

	if cond.IsRawExpr {
		expr := cond.Field
		var args []interface{}
		if rawArgs, ok := cond.Value.([]interface{}); ok {
			args = make([]interface{}, len(rawArgs))
			copy(args, rawArgs)
			// Replace ? with $n
			count := 0
			for count < len(args) {
				expr = strings.Replace(expr, "?", fmt.Sprintf("$%d", *argCount), 1)
				*argCount++
				count++
			}
		}
		return expr, args
	}

	field := f.quoteIdentifier(cond.Field)
	op := f.getOperator(cond.Op)

	switch cond.Op {
	case define.OpIn, define.OpNotIn:
		if values, ok := cond.Value.([]interface{}); ok && len(values) > 0 {
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = fmt.Sprintf("$%d", *argCount+i)
			}
			*argCount += len(values)
			return fmt.Sprintf("%s %s (%s)", field, op, strings.Join(placeholders, ", ")), values
		}
		return "", nil
	case define.OpIsNull, define.OpIsNotNull:
		return fmt.Sprintf("%s %s", field, op), nil
	case define.OpBetween, define.OpNotBetween:
		if values, ok := cond.Value.([]interface{}); ok && len(values) == 2 {
			sql := fmt.Sprintf("%s %s $%d AND $%d", field, op, *argCount, *argCount+1)
			*argCount += 2
			return sql, values
		}
		return "", nil
	default:
		sql := fmt.Sprintf("%s %s $%d", field, op, *argCount)
		*argCount++
		return sql, []interface{}{cond.Value}
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
func (f *Factory) BuildSelect(table string, fields []string, conditions []*define.Condition, orderBy string, limit, offset int) *define.SqlProto {
	if table == "" {
		return &define.SqlProto{Error: define.ErrEmptyTableName}
	}

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

	// Add WHERE conditions
	if len(conditions) > 0 {
		var whereConditions []string
		var hasOr bool
		paramIndex := 1
		for _, cond := range conditions {
			if cond == nil {
				continue
			}
			condStr, condArgs := f.buildCondition(cond, &paramIndex)
			if condStr != "" {
				if strings.HasPrefix(condStr, "HAVING") {
					// Store HAVING conditions for later
					continue
				}
				if len(whereConditions) > 0 {
					if cond.JoinType == define.JoinOr {
						hasOr = true
						whereConditions = append(whereConditions, "OR", condStr)
					} else {
						whereConditions = append(whereConditions, "AND", condStr)
					}
				} else {
					whereConditions = append(whereConditions, condStr)
				}
				args = append(args, condArgs...)
			}
		}
		if len(whereConditions) > 0 {
			if hasOr {
				query += " WHERE (" + strings.Join(whereConditions, " ") + ")"
			} else {
				query += " WHERE " + strings.Join(whereConditions, " ")
			}
		}
	}

	// Add ORDER BY
	if orderBy != "" {
		if !strings.HasPrefix(strings.ToUpper(orderBy), "ORDER BY") {
			query += " ORDER BY " + orderBy
		} else {
			query += " " + orderBy
		}
	}

	// Add LIMIT and OFFSET
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	return &define.SqlProto{
		SqlType: define.Query,
		Sql:     query,
		Args:    args,
		Error:   nil,
	}
}

// BuildUpdate builds an UPDATE query for PostgreSQL
func (f *Factory) BuildUpdate(table string, fields map[string]interface{}, fieldOrder []string, conditions []*define.Condition) *define.SqlProto {
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
		} else {
			return &define.SqlProto{Error: fmt.Errorf("field %s not found in field map", field)}
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
		return &define.SqlProto{
			Error: fmt.Errorf("no field to update"),
		}
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
			condStr, condArgs := f.buildCondition(cond, &currentParamIndex)
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

	return &define.SqlProto{
		SqlType: define.Query,
		Sql:     query,
		Args:    args,
		Error:   nil,
	}
}

// BuildInsert builds an INSERT query for PostgreSQL
func (f *Factory) BuildInsert(table string, fields map[string]interface{}, fieldOrder []string) *define.SqlProto {

	if len(fields) == 0 {
		return &define.SqlProto{
			Error: fmt.Errorf("no filed to insert"),
		}
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

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING *",
		f.quoteIdentifier(table),
		strings.Join(quotedFields, ", "),
		strings.Join(placeholders, ", "))

	return &define.SqlProto{
		SqlType: define.Query,
		Sql:     query,
		Args:    args,
		Error:   nil,
	}
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
func (f *Factory) BuildBatchInsert(table string, batchFields []map[string]interface{}) *define.SqlProto {
	if len(batchFields) == 0 {
		return &define.SqlProto{
			Error: fmt.Errorf("no values to insert"),
		}
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

	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s RETURNING *`,
		f.quoteIdentifier(table),
		strings.Join(f.quoteIdentifiers(fieldNames), ", "),
		strings.Join(valueStrings, ", "))

	return &define.SqlProto{
		SqlType: define.Query,
		Sql:     query,
		Args:    args,
		Error:   nil,
	}
}

// BuildDelete builds a DELETE query for PostgreSQL
func (f *Factory) BuildDelete(table string, conditions []*define.Condition) *define.SqlProto {
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
			condStr, condArgs := f.buildCondition(cond, &currentParamIndex)
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
	query += " RETURNING *"
	return &define.SqlProto{
		SqlType: define.Query,
		Sql:     query,
		Args:    args,
		Error:   nil,
	}
}

// BuildCreateTable builds a CREATE TABLE query for PostgreSQL
func (f *Factory) BuildCreateTable(table string, modelType reflect.Type) *define.SqlProto {
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

	return &define.SqlProto{
		SqlType: define.Exec,
		Sql:     query,
		Args:    nil,
	}
}

// GetTableInfo 获取表信息
func (f *Factory) GetTableInfo(db *sql.DB, tableName string) (*define.TableInfo, error) {
	if db == nil {
		return nil, errors.New("database connection is nil")
	}

	// Get current schema
	var schema string
	row := db.QueryRow("SELECT CURRENT_SCHEMA")
	if err := row.Scan(&schema); err != nil {
		return nil, fmt.Errorf("failed to get current schema: %v", err)
	}

	// Get table comment
	var tableComment string
	row = db.QueryRow(`
		SELECT COALESCE(obj_description(c.oid), '') 
		FROM pg_class c 
		JOIN pg_namespace n ON n.oid = c.relnamespace 
		WHERE n.nspname = $1 
		AND c.relname = $2
	`, schema, tableName)
	if err := row.Scan(&tableComment); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get table comment: %v", err)
	}

	// Get column information
	rows, err := db.Query(`
		SELECT 
			a.attname AS column_name,
			t.typname AS data_type,
			a.attlen AS max_length,
			a.atttypmod AS type_modifier,
			a.attnotnull AS not_null,
			pg_get_expr(d.adbin, d.adrelid) AS default_value,
			col_description(c.oid, a.attnum) AS column_comment,
			a.attidentity AS identity,
			CASE WHEN pk.contype = 'p' THEN true ELSE false END AS is_primary
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
		LEFT JOIN pg_constraint pk ON pk.conrelid = c.oid AND pk.contype = 'p' AND a.attnum = ANY(pk.conkey)
		WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum
	`, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get column information: %v", err)
	}
	defer rows.Close()

	var columns []define.ColumnInfo
	var primaryKeys []string
	hasDecimal := false
	hasUUID := false
	hasIP := false

	for rows.Next() {
		var col define.ColumnInfo
		var maxLength, typeModifier int
		var notNull bool
		var defaultValue, comment, identity sql.NullString
		var isPrimary bool

		err := rows.Scan(
			&col.Name,
			&col.TypeName,
			&maxLength,
			&typeModifier,
			&notNull,
			&defaultValue,
			&comment,
			&identity,
			&isPrimary,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %v", err)
		}

		// Set standard SQL data type
		col.DataType = getSQLDataType(col.TypeName)

		// Set other fields
		if typeModifier > 4 {
			col.Length = int64(typeModifier - 4)
		} else if maxLength > 0 {
			col.Length = int64(maxLength)
		}

		// Handle numeric precision and scale
		if strings.HasPrefix(col.TypeName, "numeric") || strings.HasPrefix(col.TypeName, "decimal") {
			if typeModifier > 4 {
				precision := (typeModifier - 4) >> 16
				scale := (typeModifier - 4) & 0xFFFF
				col.Precision = int(precision)
				col.Scale = int(scale)
			}
		}

		col.IsNullable = !notNull
		col.IsPrimaryKey = isPrimary
		col.IsAutoIncrement = identity.Valid && (identity.String == "a" || identity.String == "d")
		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}
		if comment.Valid {
			col.Comment = comment.String
		}

		// Check special types
		switch col.TypeName {
		case "numeric", "decimal":
			hasDecimal = true
		case "uuid":
			hasUUID = true
		case "inet":
			hasIP = true
		}

		if col.IsPrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}

		columns = append(columns, col)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return &define.TableInfo{
		TableName:    tableName,
		TableComment: tableComment,
		PrimaryKeys:  primaryKeys,
		Columns:      columns,
		HasDecimal:   hasDecimal,
		HasUUID:      hasUUID,
		HasIP:        hasIP,
	}, nil
}

// getSQLDataType returns the standard SQL data type for a given PostgreSQL type
func getSQLDataType(pgType string) string {
	switch strings.ToLower(pgType) {
	case "int2", "smallint":
		return "int32"
	case "int4", "integer":
		return "int32"
	case "int8", "bigint":
		return "int64"
	case "decimal", "numeric":
		return "float64"
	case "real", "float4":
		return "float32"
	case "double precision", "float8":
		return "float64"
	case "char", "varchar", "text", "name":
		return "string"
	case "bool", "boolean":
		return "bool"
	case "date", "timestamp", "timestamptz", "time", "timetz":
		return "time.Time"
	case "bytea":
		return "[]byte"
	case "json", "jsonb":
		return "json.RawMessage"
	case "uuid":
		return "uuid.UUID"
	case "inet", "cidr":
		return "net.IP"
	default:
		return "string"
	}
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
