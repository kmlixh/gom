package mysql

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
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
	return sql.Open("mysql", dsn)
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

	if cond.IsSubGroup && len(cond.SubConds) > 0 {
		var subCondStrs []string
		var subArgs []interface{}

		for _, subCond := range cond.SubConds {
			subStr, subArg := f.buildCondition(subCond)
			if subStr != "" {
				if subCond.Join == define.JoinOr && len(subCondStrs) > 0 {
					subCondStrs = append(subCondStrs, "OR", subStr)
				} else if len(subCondStrs) > 0 {
					subCondStrs = append(subCondStrs, "AND", subStr)
				} else {
					subCondStrs = append(subCondStrs, subStr)
				}
				subArgs = append(subArgs, subArg...)
			}
		}

		if len(subCondStrs) > 0 {
			return "(" + strings.Join(subCondStrs, " ") + ")", subArgs
		}
		return "", nil
	}

	if cond.Field == "" {
		return "", nil
	}

	op := f.getOperator(cond.Op)
	switch cond.Op {
	case define.OpIn, define.OpNotIn:
		if values, ok := cond.Value.([]interface{}); ok {
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = "?"
			}
			return fmt.Sprintf("`%s` %v (%s)", cond.Field, op, strings.Join(placeholders, ",")), values
		}
		return "", nil
	case define.OpIsNull, define.OpIsNotNull:
		return fmt.Sprintf("`%s` %v", cond.Field, op), nil
	case define.OpBetween, define.OpNotBetween:
		if values, ok := cond.Value.([]interface{}); ok && len(values) == 2 {
			return fmt.Sprintf("`%s` %v ? AND ?", cond.Field, op), values
		}
		return "", nil
	default:
		return fmt.Sprintf("`%s` %v ?", cond.Field, op), []interface{}{cond.Value}
	}
}

// BuildSelect builds a SELECT query for MySQL
func (f *Factory) BuildSelect(table string, fields []string, conditions []*define.Condition, orderBy string, limit, offset int) (string, []interface{}) {
	var args []interface{}
	query := "SELECT "

	// Add fields
	if len(fields) > 0 {
		var quotedFields []string
		for _, field := range fields {
			quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
		}
		query += strings.Join(quotedFields, ", ")
	} else {
		query += "*"
	}

	// Add table
	query += fmt.Sprintf(" FROM `%s`", table)

	// Add conditions
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

	// Add order by
	if orderBy != "" {
		query += " ORDER BY " + orderBy
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

// BuildUpdate builds an UPDATE query for MySQL
func (f *Factory) BuildUpdate(table string, fields map[string]interface{}, conditions []*define.Condition) (string, []interface{}) {
	var args []interface{}
	query := fmt.Sprintf("UPDATE `%s` SET ", table)

	// Sort field names to ensure consistent order
	var fieldNames []string
	for field := range fields {
		if field != "id" { // Skip id field
			fieldNames = append(fieldNames, field)
		}
	}
	sort.Strings(fieldNames)

	// Add fields
	var fieldStrings []string
	for _, field := range fieldNames {
		fieldStrings = append(fieldStrings, fmt.Sprintf("`%s` = ?", field))
		args = append(args, fields[field])
	}

	if len(fieldStrings) == 0 {
		// If no fields to update, return empty query
		return "", nil
	}

	query += strings.Join(fieldStrings, ", ")

	// Add conditions
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

// BuildInsert builds an INSERT query for MySQL
func (f *Factory) BuildInsert(table string, fields map[string]interface{}) (string, []interface{}) {
	if len(fields) == 0 {
		return "", nil
	}

	// Sort field names to ensure consistent order
	var fieldNames []string
	for field := range fields {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)

	var args []interface{}
	var quotedFields []string
	var placeholders []string

	for _, field := range fieldNames {
		quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
		placeholders = append(placeholders, "?")
		args = append(args, fields[field])
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
	var columns []string

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		// Parse tag
		parts := strings.Split(tag, ",")
		columnName := parts[0]
		var columnConstraints []string
		if len(parts) > 1 {
			columnConstraints = parts[1:]
		}

		// Start building column definition
		columnDef := fmt.Sprintf("`%s`", columnName)

		// Add data type based on field type
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			columnDef += " INT"
		case reflect.Int64:
			// Check if it's a primary key with auto increment
			isPrimaryAuto := false
			for _, constraint := range columnConstraints {
				if constraint == "primaryAuto" || constraint == "@" {
					isPrimaryAuto = true
					break
				}
			}
			if isPrimaryAuto {
				columnDef += " BIGINT PRIMARY KEY AUTO_INCREMENT"
			} else {
				columnDef += " BIGINT"
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			columnDef += " INT UNSIGNED"
		case reflect.Uint64:
			columnDef += " BIGINT UNSIGNED"
		case reflect.Float32:
			columnDef += " FLOAT"
		case reflect.Float64:
			columnDef += " DOUBLE"
		case reflect.String:
			size := field.Tag.Get("size")
			if size == "" {
				size = "255"
			}
			columnDef += fmt.Sprintf(" VARCHAR(%s)", size)
		case reflect.Bool:
			columnDef += " BOOLEAN"
		case reflect.Struct:
			if field.Type == reflect.TypeOf(time.Time{}) {
				if columnName == "created_at" {
					columnDef += " TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
				} else if columnName == "updated_at" {
					columnDef += " TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
				} else {
					columnDef += " TIMESTAMP"
				}
			}
		}

		columns = append(columns, columnDef)
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s)", table, strings.Join(columns, ", "))
}

// GetTableInfo 获取表信息
func (f *Factory) GetTableInfo(db *sql.DB, tableName string) (*define.TableInfo, error) {
	// 获取表基本信息
	var tableInfo define.TableInfo
	tableInfo.TableName = tableName

	// 获取表注释
	query := `SELECT TABLE_COMMENT 
			 FROM INFORMATION_SCHEMA.TABLES 
			 WHERE TABLE_SCHEMA = DATABASE() 
			 AND TABLE_NAME = ?`

	err := db.QueryRow(query, tableName).Scan(&tableInfo.TableComment)
	if err != nil {
		return nil, fmt.Errorf("获取表注释失败: %v", err)
	}

	// 获取列信息
	query = `SELECT 
				COLUMN_NAME,
				DATA_TYPE,
				IFNULL(CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION),
				NUMERIC_SCALE,
				IS_NULLABLE,
				COLUMN_KEY,
				EXTRA,
				COLUMN_DEFAULT,
				COLUMN_COMMENT
			FROM INFORMATION_SCHEMA.COLUMNS 
			WHERE TABLE_SCHEMA = DATABASE() 
			AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col define.ColumnInfo
		var length sql.NullInt64
		var scale sql.NullInt64
		var isNullable string
		var columnKey string
		var extra string
		var defaultValue sql.NullString

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&length,
			&scale,
			&isNullable,
			&columnKey,
			&extra,
			&defaultValue,
			&col.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描信息失败: %v", err)
		}

		// 设置列属性
		col.Length = length.Int64
		col.Scale = int(scale.Int64)
		col.IsNullable = isNullable == "YES"
		col.IsPrimaryKey = columnKey == "PRI"
		col.IsAutoIncrement = strings.Contains(extra, "auto_increment")
		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}

		// 如果是主键，添加到主键列表
		if col.IsPrimaryKey {
			tableInfo.PrimaryKeys = append(tableInfo.PrimaryKeys, col.Name)
		}

		tableInfo.Columns = append(tableInfo.Columns, col)
	}

	if len(tableInfo.Columns) == 0 {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}

	return &tableInfo, nil
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

// BuildOrderBy builds the ORDER BY clause
func (f *Factory) BuildOrderBy(orders []define.OrderBy) string {
	if len(orders) == 0 {
		return ""
	}

	var orderClauses []string
	for _, order := range orders {
		if order.Type == define.OrderDesc {
			orderClauses = append(orderClauses, "`"+order.Field+"` DESC")
		} else {
			orderClauses = append(orderClauses, "`"+order.Field+"`")
		}
	}
	return strings.Join(orderClauses, ", ")
}
