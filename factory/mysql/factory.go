package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
)

// Factory represents a MySQL query builder
type Factory struct{}

// GetType returns the database type
func (f *Factory) GetType() string {
	return "mysql"
}

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

var dbTableColsCache = make(map[string][]define.Column)
var columnSql = "select COLUMN_NAME as columnName,DATA_TYPE as dataType,COLUMN_KEY as columnKey,EXTRA as extra, IFNULL(COLUMN_COMMENT,'') as comment from information_schema.columns  where table_schema=?  and table_name= ? order by ordinal_position;"

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
		er = rows.Scan(&columnName, &columnType, &columnKey, &extra)
		if er == nil {
			columns = append(columns, define.Column{ColumnName: columnName, ColumnType: columnType, Primary: columnKey == "PRI", PrimaryAuto: columnKey == "PRI" && extra == "auto_increment"})
		} else {
			return nil, er
		}
	}
	dbTableColsCache[dbName+"-"+tableName] = columns
	return columns, nil

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
				if subCond.JoinType == define.JoinOr {
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
func (f *Factory) BuildSelect(table string, fields []string, conditions []*define.Condition, orderBy string, limit, offset int) (string, []interface{}, error) {
	if table == "" {
		return "", nil, define.ErrEmptyTableName
	}

	var args []interface{}
	var where []string

	// Build SELECT clause
	var selectFields string
	if len(fields) == 0 {
		selectFields = "*"
	} else {
		quotedFields := make([]string, len(fields))
		for i, field := range fields {
			if strings.Contains(field, " ") || strings.Contains(field, "(") || strings.Contains(field, "GROUP BY") || strings.Contains(field, "HAVING") {
				quotedFields[i] = field
			} else {
				quotedFields[i] = "`" + field + "`"
			}
		}
		selectFields = strings.Join(quotedFields, ", ")
	}

	// Build WHERE clause
	if len(conditions) > 0 {
		for _, cond := range conditions {
			if cond != nil {
				sql, condArgs := f.buildCondition(cond)
				if sql != "" {
					if strings.HasPrefix(strings.ToUpper(sql), "HAVING") {
						continue
					}
					where = append(where, sql)
					args = append(args, condArgs...)
				}
			}
		}
	}

	// Build query
	query := fmt.Sprintf("SELECT %s FROM `%s`", selectFields, table)
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}

	// Extract GROUP BY and HAVING clauses from fields
	var groupByClause string
	var havingClause string
	for _, field := range fields {
		if strings.HasPrefix(strings.ToUpper(field), "GROUP BY") {
			groupByClause = field
		} else if strings.HasPrefix(strings.ToUpper(field), "HAVING") {
			havingClause = field
		}
	}

	// Add GROUP BY if specified
	if groupByClause != "" {
		query += " " + groupByClause
	}

	// Add HAVING if specified
	if havingClause != "" {
		query += " " + havingClause
	}

	// Add ORDER BY if specified
	if orderBy != "" {
		if !strings.HasPrefix(strings.ToUpper(orderBy), "ORDER BY") {
			query += " ORDER BY " + orderBy
		} else {
			query += " " + orderBy
		}
	}

	// Add LIMIT and OFFSET if specified
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	return query, args, nil
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
					if cond.JoinType == define.JoinOr {
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

	// Get field names from the first row and sort them for consistent order
	var fieldNames []string
	for field := range values[0] {
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)

	// Build quoted field names
	var quotedFields []string
	for _, field := range fieldNames {
		quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
	}

	var args []interface{}
	var valuePlaceholders []string

	// Build placeholders and collect args
	for _, row := range values {
		var rowPlaceholders []string
		for _, field := range fieldNames {
			rowPlaceholders = append(rowPlaceholders, "?")
			args = append(args, row[field])
		}
		valuePlaceholders = append(valuePlaceholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s",
		table,
		strings.Join(quotedFields, ", "),
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
				if cond.JoinType == define.JoinOr && i > 0 {
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
	if db == nil {
		return nil, errors.New("database connection is nil")
	}

	// Get table comment
	var tableComment string
	row := db.QueryRow(`
		SELECT table_comment 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
		AND table_name = ?
	`, tableName)
	if err := row.Scan(&tableComment); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get table comment: %v", err)
	}

	// Get column information
	rows, err := db.Query(`
		SELECT 
			column_name,
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_key,
			extra,
			column_default,
			column_comment
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		AND table_name = ?
		ORDER BY ordinal_position
	`, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get column information: %v", err)
	}
	defer rows.Close()

	var columns []define.ColumnInfo
	var primaryKeys []string
	hasDecimal := false
	hasUUID := false
	hasIP := false
	hasTime := false

	for rows.Next() {
		var col define.ColumnInfo
		var isNullable, columnKey, extra, comment string
		var maxLength, numericPrecision, numericScale sql.NullInt64
		var defaultValue sql.NullString

		err := rows.Scan(
			&col.Name,
			&col.TypeName,
			&maxLength,
			&numericPrecision,
			&numericScale,
			&isNullable,
			&columnKey,
			&extra,
			&defaultValue,
			&comment,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %v", err)
		}

		// Set standard SQL data type
		col.DataType = getSQLDataType(col.TypeName)

		// Set other fields
		col.Length = maxLength.Int64
		if numericPrecision.Valid {
			col.Precision = int(numericPrecision.Int64)
		}
		if numericScale.Valid {
			col.Scale = int(numericScale.Int64)
		}
		col.IsNullable = isNullable == "YES"
		col.IsPrimaryKey = columnKey == "PRI"
		col.IsAutoIncrement = extra == "auto_increment"
		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}
		col.Comment = comment

		// Check special types
		switch strings.ToLower(col.TypeName) {
		case "decimal", "numeric":
			hasDecimal = true
		case "uuid":
			hasUUID = true
		case "inet":
			hasIP = true
		case "date", "datetime", "timestamp":
			hasTime = true
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
		HasTime:      hasTime,
	}, nil
}

// getSQLDataType returns the standard SQL data type for a given MySQL type
func getSQLDataType(mysqlType string) reflect.Type {
	switch strings.ToLower(mysqlType) {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		return reflect.TypeOf(sql.NullInt32{})
	case "bigint":
		return reflect.TypeOf(sql.NullInt64{})
	case "decimal", "numeric", "float", "double":
		return reflect.TypeOf(sql.NullFloat64{})
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return reflect.TypeOf(sql.NullString{})
	case "date", "datetime", "timestamp":
		return reflect.TypeOf(sql.NullTime{})
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return reflect.TypeOf(sql.RawBytes{})
	case "bit", "bool", "boolean":
		return reflect.TypeOf(sql.NullBool{})
	default:
		return reflect.TypeOf(sql.NullString{})
	}
}

// GetTables 获取符合模式的所有表
func (f *Factory) GetTables(db *sql.DB, pattern string) ([]string, error) {
	if pattern == "" {
		pattern = "%"
	}
	query := "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"
	if pattern != "%" {
		query += fmt.Sprintf(" AND TABLE_NAME LIKE '%s'", pattern)
	}
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}
	return tables, rows.Err()
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

// GenerateStruct generates Go struct code from MySQL table schema
func (f *Factory) GenerateStruct(db *sql.DB, tableName string, outputDir string, packageName string) error {
	// Get table info
	tableInfo, err := f.GetTableInfo(db, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table info: %v", err)
	}

	// Generate struct code
	var code strings.Builder
	code.WriteString(fmt.Sprintf("package %s\n\n", packageName))
	code.WriteString("import (\n")
	if tableInfo.HasDecimal {
		code.WriteString("\t\"database/sql\"\n")
	}
	if tableInfo.HasTime {
		code.WriteString("\t\"time\"\n")
	}
	code.WriteString(")\n\n")

	// Generate struct definition
	structName := strings.Title(tableName)
	code.WriteString(fmt.Sprintf("// %s represents the %s table\n", structName, tableName))
	if tableInfo.TableComment != "" {
		code.WriteString(fmt.Sprintf("// %s\n", tableInfo.TableComment))
	}
	code.WriteString(fmt.Sprintf("type %s struct {\n", structName))

	// Generate struct fields
	for _, col := range tableInfo.Columns {
		// Generate field comment
		if col.Comment != "" {
			code.WriteString(fmt.Sprintf("\t// %s\n", col.Comment))
		}

		// Convert MySQL type to Go type
		goType := f.mysqlTypeToGoType(col)
		fieldName := strings.Title(col.Name)

		// Generate field definition with gom tag
		code.WriteString(fmt.Sprintf("\t%s %s `gom:\"%s\"`\n", fieldName, goType, col.Name))
	}
	code.WriteString("}\n")

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Write to file
	filename := filepath.Join(outputDir, strings.ToLower(tableName)+".go")
	if err := os.WriteFile(filename, []byte(code.String()), 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}

// mysqlTypeToGoType converts MySQL column type to Go type
func (f *Factory) mysqlTypeToGoType(col define.ColumnInfo) string {
	switch strings.ToLower(col.TypeName) {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		if col.IsNullable {
			return "sql.NullInt32"
		}
		return "int32"
	case "bigint":
		if col.IsNullable {
			return "sql.NullInt64"
		}
		return "int64"
	case "decimal", "numeric", "float", "double":
		if col.IsNullable {
			return "sql.NullFloat64"
		}
		return "float64"
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		if col.IsNullable {
			return "sql.NullString"
		}
		return "string"
	case "date", "datetime", "timestamp":
		if col.IsNullable {
			return "*time.Time"
		}
		return "time.Time"
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return "[]byte"
	case "bit", "bool", "boolean":
		if col.IsNullable {
			return "sql.NullBool"
		}
		return "bool"
	default:
		if col.IsNullable {
			return "sql.NullString"
		}
		return "string"
	}
}
