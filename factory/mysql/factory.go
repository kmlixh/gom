package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
)

// Factory implements the SQLFactory interface for MySQL
type Factory struct{}

func init() {
	RegisterFactory()
}
func RegisterFactory() {
	define.RegisterFactory("mysql", &Factory{})
}

func (f *Factory) Connect(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// buildCondition builds a single condition clause
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

func (f *Factory) buildCondition(cond *define.Condition) (string, []interface{}) {
	if cond == nil {
		return "", nil
	}

	if cond.IsSubGroup {
		var subConditions []string
		var args []interface{}
		for _, subCond := range cond.SubConds {
			if subCond == nil {
				continue
			}
			subStr, subArgs := f.buildCondition(subCond)
			if subStr != "" {
				subConditions = append(subConditions, subStr)
				args = append(args, subArgs...)
			}
		}
		if len(subConditions) > 0 {
			return fmt.Sprintf("(%s)", strings.Join(subConditions, " AND ")), args
		}
		return "", nil
	}

	if cond.Field == "" {
		return "", nil
	}

	// Handle custom conditions (like HAVING)
	if cond.Op == define.OpCustom {
		if values, ok := cond.Value.([]interface{}); ok {
			return cond.Field, values
		}
		return cond.Field, []interface{}{cond.Value}
	}

	op := f.getOperator(cond.Op)
	var condStr string
	var args []interface{}

	switch cond.Op {
	case define.OpIn, define.OpNotIn:
		if values, ok := cond.Value.([]interface{}); ok {
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = "?"
			}
			condStr = fmt.Sprintf("`%s` %v (%s)", cond.Field, op, strings.Join(placeholders, ","))
			args = values
		}
	case define.OpIsNull, define.OpIsNotNull:
		condStr = fmt.Sprintf("`%s` %v", cond.Field, op)
	case define.OpBetween, define.OpNotBetween:
		if values, ok := cond.Value.([]interface{}); ok && len(values) == 2 {
			condStr = fmt.Sprintf("`%s` %v ? AND ?", cond.Field, op)
			args = values
		}
	default:
		condStr = fmt.Sprintf("`%s` %v ?", cond.Field, op)
		args = []interface{}{cond.Value}
	}

	if condStr == "" {
		return "", nil
	}

	// Handle AND/OR conditions
	if len(cond.SubConds) > 0 {
		var subConditions []string
		subConditions = append(subConditions, condStr)
		var hasOr bool
		for _, subCond := range cond.SubConds {
			if subCond == nil {
				continue
			}
			subStr, subArgs := f.buildCondition(subCond)
			if subStr != "" {
				if subCond.Join == define.JoinOr {
					hasOr = true
					subConditions = append(subConditions, "OR", subStr)
				} else {
					subConditions = append(subConditions, "AND", subStr)
				}
				args = append(args, subArgs...)
			}
		}
		if len(subConditions) > 1 {
			if hasOr {
				return fmt.Sprintf("(%s)", strings.Join(subConditions, " ")), args
			}
			return strings.Join(subConditions, " "), args
		}
	}

	return condStr, args
}

// BuildSelect builds a SELECT query for MySQL
func (f *Factory) BuildSelect(table string, fields []string, conditions []*define.Condition, orderBy string, limit, offset int) (string, []interface{}) {
	var args []interface{}
	query := "SELECT "

	// Add fields
	if len(fields) > 0 {
		var quotedFields []string
		for _, field := range fields {
			if field == "*" {
				quotedFields = append(quotedFields, "*")
			} else if strings.Contains(field, "(") && strings.Contains(field, ")") {
				// Don't quote function calls
				quotedFields = append(quotedFields, field)
			} else if strings.HasPrefix(field, "GROUP BY") || strings.HasPrefix(field, "HAVING") {
				// Don't modify GROUP BY and HAVING clauses
				continue
			} else {
				quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
			}
		}
		query += strings.Join(quotedFields, ", ")
	} else {
		query += "*"
	}

	// Add table
	query += fmt.Sprintf(" FROM `%s`", table)

	// Add WHERE conditions
	if len(conditions) > 0 {
		var whereConditions []string
		var hasOr bool
		for _, cond := range conditions {
			if cond == nil {
				continue
			}
			condStr, condArgs := f.buildCondition(cond)
			if condStr != "" {
				if strings.HasPrefix(condStr, "HAVING") {
					// Store HAVING conditions for later
					continue
				}
				if len(whereConditions) > 0 {
					if cond.Join == define.JoinOr {
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

	// Add GROUP BY and HAVING
	var groupByClause string
	var havingClause string
	for _, field := range fields {
		if strings.HasPrefix(field, "GROUP BY") {
			groupByClause = field
		}
	}
	for _, cond := range conditions {
		if cond != nil {
			condStr, condArgs := f.buildCondition(cond)
			if strings.HasPrefix(condStr, "HAVING") {
				havingClause = condStr
				args = append(args, condArgs...)
			}
		}
	}
	if groupByClause != "" {
		query += " " + groupByClause
	}
	if havingClause != "" {
		query += " " + havingClause
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

	return query, args
}

// BuildUpdate builds an UPDATE query for MySQL
func (f *Factory) BuildUpdate(table string, fields map[string]interface{}, fieldOrder []string, conditions []*define.Condition) (string, []interface{}) {
	var args []interface{}
	query := fmt.Sprintf("UPDATE `%s` SET ", table)

	// Use fieldOrder to maintain field order
	var fieldStrings []string
	usedFields := make(map[string]bool)

	// First add fields in the specified order
	for _, field := range fieldOrder {
		if value, ok := fields[field]; ok {
			fieldStrings = append(fieldStrings, fmt.Sprintf("`%s` = ?", field))
			args = append(args, value)
			usedFields[field] = true
		}
	}

	// Then add any remaining fields
	for field, value := range fields {
		if !usedFields[field] {
			fieldStrings = append(fieldStrings, fmt.Sprintf("`%s` = ?", field))
			args = append(args, value)
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
		for i, cond := range conditions {
			if cond == nil {
				continue
			}
			condStr, condArgs := f.buildCondition(cond)
			if condStr != "" {
				if i > 0 {
					if cond.Join == define.JoinOr {
						condStrings = append(condStrings, "OR")
					} else {
						condStrings = append(condStrings, "AND")
					}
				}
				condStrings = append(condStrings, condStr)
				args = append(args, condArgs...)
			}
		}
		if len(condStrings) > 0 {
			query += strings.Join(condStrings, " ")
		}
	}

	return query, args
}

// BuildInsert builds an INSERT query for MySQL
func (f *Factory) BuildInsert(table string, fields map[string]interface{}, fieldOrder []string) (string, []interface{}) {
	if len(fields) == 0 {
		return "", nil
	}

	var args []interface{}
	var quotedFields []string
	var placeholders []string
	usedFields := make(map[string]bool)

	// First add fields in the specified order
	for _, field := range fieldOrder {
		if value, ok := fields[field]; ok {
			quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
			args = append(args, value)
			placeholders = append(placeholders, "?")
			usedFields[field] = true
		}
	}

	// Then add any remaining fields
	for field, value := range fields {
		if !usedFields[field] {
			quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
			args = append(args, value)
			placeholders = append(placeholders, "?")
		}
	}

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)",
		table,
		strings.Join(quotedFields, ", "),
		strings.Join(placeholders, ", "))

	return query, args
}

// BuildBatchInsert builds a batch INSERT query for MySQL
func (f *Factory) BuildBatchInsert(table string, values []map[string]interface{}) (string, []interface{}) {
	if len(values) == 0 {
		return "", nil
	}

	// Get field names from the first row
	var fieldNames []string
	for field := range values[0] {
		fieldNames = append(fieldNames, fmt.Sprintf("`%s`", field))
	}

	var args []interface{}
	var valuePlaceholders []string

	// Build placeholders and collect args
	for _, row := range values {
		var rowPlaceholders []string
		for _, field := range fieldNames {
			rowPlaceholders = append(rowPlaceholders, "?")
			args = append(args, row[strings.Trim(field, "`")])
		}
		valuePlaceholders = append(valuePlaceholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s",
		table,
		strings.Join(fieldNames, ", "),
		strings.Join(valuePlaceholders, ", "),
	)

	return query, args
}

// BuildDelete builds a DELETE query for MySQL
func (f *Factory) BuildDelete(table string, conditions []*define.Condition) (string, []interface{}) {
	query := fmt.Sprintf("DELETE FROM `%s`", table)
	var args []interface{}

	if len(conditions) > 0 {
		query += " WHERE "
		var condStrings []string
		for i, cond := range conditions {
			condStr, condArgs := f.buildCondition(cond)
			if condStr != "" {
				if cond.Join == define.JoinOr && i > 0 {
					condStrings = append(condStrings, "OR", condStr)
				} else if i > 0 {
					condStrings = append(condStrings, "AND", condStr)
				} else {
					condStrings = append(condStrings, condStr)
				}
				args = append(args, condArgs...)
			}
		}
		query += strings.Join(condStrings, " ")
	}

	return query, args
}

// BuildCreateTable builds a CREATE TABLE query for MySQL
func (f *Factory) BuildCreateTable(table string, modelType reflect.Type) string {
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	var fields []string
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		sqlTag := field.Tag.Get("sql")
		if sqlTag == "-" {
			continue
		}

		// Get field name from tag or use field name
		fieldName := field.Tag.Get("gom")
		if fieldName == "" {
			fieldName = field.Name
		}
		if strings.Contains(fieldName, ",") {
			fieldName = strings.Split(fieldName, ",")[0]
		}
		if fieldName == "-" {
			continue
		}

		fieldDef := fmt.Sprintf("`%s`", fieldName)

		// Handle special fields
		switch fieldName {
		case "id":
			fields = append(fields, "`id` BIGINT AUTO_INCREMENT PRIMARY KEY")
			continue
		case "created_at":
			fields = append(fields, "`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP")
			continue
		case "updated_at":
			fields = append(fields, "`updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
			continue
		case "deleted_at":
			fields = append(fields, "`deleted_at` TIMESTAMP")
			continue
		}

		// Handle normal fields
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			fieldDef += " INTEGER"
		case reflect.Int64:
			fieldDef += " BIGINT"
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			fieldDef += " INTEGER UNSIGNED"
		case reflect.Uint64:
			fieldDef += " BIGINT UNSIGNED"
		case reflect.Float32:
			fieldDef += " FLOAT"
		case reflect.Float64:
			fieldDef += " DOUBLE"
		case reflect.Bool:
			fieldDef += " BOOLEAN"
		case reflect.String:
			fieldDef += " VARCHAR(255)"
		default:
			if field.Type == reflect.TypeOf(time.Time{}) {
				fieldDef += " TIMESTAMP"
			} else {
				fieldDef += " TEXT"
			}
		}

		// Add NOT NULL if field is not a pointer
		if field.Type.Kind() != reflect.Ptr {
			fieldDef += " NOT NULL"
		}

		// Add default value if specified in tag
		if sqlTag != "" {
			fieldDef += " " + sqlTag
		}

		fields = append(fields, fieldDef)
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n", table)
	query += strings.Join(fields, ",\n")
	query += "\n)"

	return query
}

// GetTableInfo retrieves table information from MySQL
func (f *Factory) GetTableInfo(db *sql.DB, tableName string) (*define.TableInfo, error) {
	// Get table schema
	var tableSchema string
	err := db.QueryRow("SELECT DATABASE()").Scan(&tableSchema)
	if err != nil {
		return nil, fmt.Errorf("获取数据库名失败: %v", err)
	}

	// Get table comment
	var tableComment string
	err = db.QueryRow(`
		SELECT table_comment 
		FROM information_schema.tables 
		WHERE table_schema = ? AND table_name = ?
	`, tableSchema, tableName).Scan(&tableComment)
	if err != nil {
		return nil, fmt.Errorf("获取表注释失败: %v", err)
	}

	// Get column information
	rows, err := db.Query(`
		SELECT 
			column_name,
			data_type,
			column_type,
			is_nullable,
			column_key,
			column_default,
			extra,
			column_comment,
			numeric_precision,
			numeric_scale,
			character_maximum_length
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`, tableSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []define.ColumnInfo
	var primaryKeys []string
	for rows.Next() {
		var (
			columnName    string
			dataType      string
			columnType    string
			isNullable    string
			columnKey     string
			columnDefault sql.NullString
			extra         string
			comment       string
			precision     sql.NullInt64
			scale         sql.NullInt64
			length        sql.NullInt64
		)

		err := rows.Scan(
			&columnName,
			&dataType,
			&columnType,
			&isNullable,
			&columnKey,
			&columnDefault,
			&extra,
			&comment,
			&precision,
			&scale,
			&length,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		column := define.ColumnInfo{
			Name:            columnName,
			Type:            dataType,
			IsNullable:      isNullable == "YES",
			IsPrimaryKey:    columnKey == "PRI",
			IsAutoIncrement: strings.Contains(strings.ToLower(extra), "auto_increment"),
			Comment:         comment,
		}

		if columnDefault.Valid {
			column.DefaultValue = columnDefault.String
		}
		if length.Valid {
			column.Length = length.Int64
		}
		if precision.Valid {
			column.Precision = int(precision.Int64)
		}
		if scale.Valid {
			column.Scale = int(scale.Int64)
		}

		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, columnName)
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列信息失败: %v", err)
	}

	return &define.TableInfo{
		TableName:    tableName,
		TableComment: tableComment,
		PrimaryKeys:  primaryKeys,
		Columns:      columns,
	}, nil
}

// GetTables 获取符合模式的所有表
func (f *Factory) GetTables(db *sql.DB, pattern string) ([]string, error) {
	var tables []string
	var query string

	if pattern == "*" {
		// 查询所有表
		query = `SELECT TABLE_NAME 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = DATABASE()
				ORDER BY TABLE_NAME`
	} else {
		// 将 * 转换为 SQL LIKE 模式
		pattern = strings.ReplaceAll(pattern, "*", "%")
		query = `SELECT TABLE_NAME 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = DATABASE()
				AND TABLE_NAME LIKE ?
				ORDER BY TABLE_NAME`
	}

	var rows *sql.Rows
	var err error
	if pattern == "*" {
		rows, err = db.Query(query)
	} else {
		rows, err = db.Query(query, pattern)
	}
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

// BuildOrderBy builds the ORDER BY clause for MySQL
func (f *Factory) BuildOrderBy(orders []define.OrderBy) string {
	if len(orders) == 0 {
		return ""
	}

	var parts []string
	for _, order := range orders {
		part := fmt.Sprintf("`%s`", order.Field)
		if order.Type == define.OrderDesc {
			part += " DESC"
		} else {
			part += " ASC"
		}
		parts = append(parts, part)
	}

	return "ORDER BY " + strings.Join(parts, ", ")
}
