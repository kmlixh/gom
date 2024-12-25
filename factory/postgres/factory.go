package postgres

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kmlixh/gom/v4/define"
)

// Factory implements the SQLFactory interface for PostgreSQL
type Factory struct{}

func (f *Factory) Connect(dsn string) (*sql.DB, error) {
	return sql.Open("pgx", dsn)
}

func init() {
	define.RegisterFactory("postgres", &Factory{})
}

// BuildSelect builds a SELECT query for PostgreSQL
func (f *Factory) BuildSelect(table string, fields []string, conditions []*define.Condition, orderBy string, limit, offset int) (string, []interface{}) {
	var args []interface{}
	var paramCount int
	query := "SELECT "

	// Add fields
	if len(fields) > 0 {
		query += strings.Join(fields, ", ")
	} else {
		query += "*"
	}

	// Add table
	query += " FROM " + table

	// Add conditions
	if len(conditions) > 0 {
		query += " WHERE "
		var condStrings []string
		for _, cond := range conditions {
			paramCount++
			condStrings = append(condStrings, fmt.Sprintf("%s %s $%d", cond.Field, cond.Op, paramCount))
			args = append(args, cond.Value)
		}
		query += strings.Join(condStrings, " AND ")
	}

	// Add order by
	if orderBy != "" {
		query += " ORDER BY " + orderBy
	}

	// Add limit and offset
	if limit > 0 {

		paramCount++
		query += fmt.Sprintf(" LIMIT $%d", paramCount)
		args = append(args, limit)
		if offset > 0 {
			paramCount++
			query += fmt.Sprintf(" OFFSET $%d", paramCount)
			args = append(args, offset)
		}
	}

	return query, args
}

// BuildUpdate builds an UPDATE query for PostgreSQL
func (f *Factory) BuildUpdate(table string, fields map[string]interface{}, conditions []*define.Condition) (string, []interface{}) {
	var args []interface{}
	var paramCount int
	query := "UPDATE " + table + " SET "

	// Add fields
	var fieldStrings []string
	for field, value := range fields {
		paramCount++
		fieldStrings = append(fieldStrings, fmt.Sprintf("%s = $%d", field, paramCount))
		args = append(args, value)
	}
	query += strings.Join(fieldStrings, ", ")

	// Add conditions
	if len(conditions) > 0 {
		query += " WHERE "
		var condStrings []string
		for _, cond := range conditions {
			paramCount++
			condStrings = append(condStrings, fmt.Sprintf("%s %s $%d", cond.Field, cond.Op, paramCount))
			args = append(args, cond.Value)
		}
		query += strings.Join(condStrings, " AND ")
	}

	return query, args
}

// BuildInsert builds an INSERT query for PostgreSQL
func (f *Factory) BuildInsert(table string, fields map[string]interface{}) (string, []interface{}) {
	var args []interface{}
	var fieldNames []string
	var placeholders []string
	argCount := 1

	for field, value := range fields {
		fieldNames = append(fieldNames, field)
		placeholders = append(placeholders, fmt.Sprintf("$%d", argCount))
		args = append(args, value)
		argCount++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) RETURNING id",
		table,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	)

	return query, args
}

// BuildBatchInsert builds a batch INSERT query for PostgreSQL
func (f *Factory) BuildBatchInsert(table string, values []map[string]interface{}) (string, []interface{}) {
	if len(values) == 0 {
		return "", nil
	}

	// Get field names from the first row
	var fieldNames []string
	for field := range values[0] {
		fieldNames = append(fieldNames, field)
	}

	var args []interface{}
	var valuePlaceholders []string
	var paramCount int

	// Build placeholders and collect args
	for _, row := range values {
		var rowPlaceholders []string
		for _, field := range fieldNames {
			paramCount++
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", paramCount))
			args = append(args, row[field])
		}
		valuePlaceholders = append(valuePlaceholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(fieldNames, ", "),
		strings.Join(valuePlaceholders, ", "),
	)

	return query, args
}

// BuildDelete builds a DELETE query for PostgreSQL
func (f *Factory) BuildDelete(table string, conditions []*define.Condition) (string, []interface{}) {
	var args []interface{}
	var paramCount int
	query := "DELETE FROM " + table

	// Add conditions
	if len(conditions) > 0 {
		query += " WHERE "
		var condStrings []string
		for _, cond := range conditions {
			paramCount++
			condStrings = append(condStrings, fmt.Sprintf("%s %s $%d", cond.Field, cond.Op, paramCount))
			args = append(args, cond.Value)
		}
		query += strings.Join(condStrings, " AND ")
	}

	return query, args
}

// BuildCreateTable builds a CREATE TABLE query for PostgreSQL
func (f *Factory) BuildCreateTable(table string, modelType reflect.Type) string {
	var columns []string
	var constraints []string

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
		columnDef := columnName

		// Add data type based on field type
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			columnDef += " INTEGER"
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
				columnDef += " BIGSERIAL"
			} else {
				columnDef += " BIGINT"
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			columnDef += " INTEGER"
		case reflect.Uint64:
			columnDef += " BIGINT"
		case reflect.Float32:
			columnDef += " REAL"
		case reflect.Float64:
			columnDef += " DOUBLE PRECISION"
		case reflect.String:
			size := field.Tag.Get("size")
			if size == "" {
				size = "255"
			}
			columnDef += " VARCHAR(" + size + ")"
		case reflect.Bool:
			columnDef += " BOOLEAN"
		case reflect.Struct:
			if field.Type == reflect.TypeOf(time.Time{}) {
				columnDef += " TIMESTAMP"
			}
		}

		// Add constraints
		for _, constraint := range columnConstraints {
			switch constraint {
			case "primary", "!":
				columnDef += " PRIMARY KEY"
			case "primaryAuto", "@":
				// BIGSERIAL type already set above
				columnDef += " PRIMARY KEY"
			case "notnull":
				columnDef += " NOT NULL"
			case "unique":
				constraintName := fmt.Sprintf("uq_%s_%s", table, columnName)
				constraints = append(constraints, fmt.Sprintf("CONSTRAINT %s UNIQUE (%s)", constraintName, columnName))
			case "index":
				// Indexes will be created after table creation
				constraints = append(constraints, fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s (%s);",
					table, columnName, table, columnName))
			default:
				if strings.HasPrefix(constraint, "default:") {
					defaultValue := strings.TrimPrefix(constraint, "default:")
					columnDef += " DEFAULT " + defaultValue
				} else if strings.HasPrefix(constraint, "foreignkey:") {
					// Format: foreignkey:table.column
					fkInfo := strings.TrimPrefix(constraint, "foreignkey:")
					parts := strings.Split(fkInfo, ".")
					if len(parts) == 2 {
						fkName := fmt.Sprintf("fk_%s_%s", table, columnName)
						constraints = append(constraints, fmt.Sprintf("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
							fkName, columnName, parts[0], parts[1]))
					}
				}
			}
		}

		// Special handling for created_at and updated_at
		if columnName == "created_at" {
			columnDef += " NOT NULL DEFAULT CURRENT_TIMESTAMP"
		} else if columnName == "updated_at" {
			columnDef += " NOT NULL DEFAULT CURRENT_TIMESTAMP"
		}

		columns = append(columns, columnDef)
	}

	// Start building the complete SQL
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s", table, strings.Join(columns, ",\n  "))

	// Add table-level constraints
	if len(constraints) > 0 {
		var tableConstraints []string
		var afterTableConstraints []string

		for _, constraint := range constraints {
			if strings.HasPrefix(constraint, "CREATE INDEX") {
				afterTableConstraints = append(afterTableConstraints, constraint)
			} else {
				tableConstraints = append(tableConstraints, constraint)
			}
		}

		if len(tableConstraints) > 0 {
			query += ",\n  " + strings.Join(tableConstraints, ",\n  ")
		}
		query += "\n);\n"

		// Add post-table creation statements
		if len(afterTableConstraints) > 0 {
			query += "\n" + strings.Join(afterTableConstraints, "\n")
		}
	} else {
		query += "\n);\n"
	}

	// Add trigger for updated_at if the column exists
	hasUpdatedAt := false
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get("gom")
		if strings.Split(tag, ",")[0] == "updated_at" {
			hasUpdatedAt = true
			break
		}
	}

	if hasUpdatedAt {
		query += fmt.Sprintf(`
CREATE OR REPLACE FUNCTION update_%s_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_%s_updated_at ON %s;

CREATE TRIGGER update_%s_updated_at
    BEFORE UPDATE ON %s
    FOR EACH ROW
    EXECUTE FUNCTION update_%s_updated_at();
`, table, table, table, table, table, table)
	}

	return query
}

// GetTableInfo 获取表信息
func (f *Factory) GetTableInfo(db *sql.DB, tableName string) (*define.TableInfo, error) {
	// 解析 schema 和表名
	var schema, table string
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	} else {
		// 如果没有指定 schema，默认使用 public
		schema = "public"
		table = tableName
	}

	// 获取表基本信息
	var tableInfo define.TableInfo
	tableInfo.TableName = schema + "." + table

	// 获取表注释
	query := `SELECT obj_description(($1::text)::regclass, 'pg_class')`

	var comment sql.NullString
	err := db.QueryRow(query, tableInfo.TableName).Scan(&comment)
	if err != nil {
		return nil, fmt.Errorf("获取表注释失败: %v", err)
	}
	tableInfo.TableComment = comment.String

	// 获取列信息
	query = `SELECT 
				a.attname as column_name,
				format_type(a.atttypid, a.atttypmod) as data_type,
				CASE 
					WHEN a.atttypmod > 0 THEN a.atttypmod - 4
					ELSE a.attlen
				END as length,
				CASE 
					WHEN t.typtype = 'd' THEN t.typtypmod
					ELSE NULL
				END as precision,
				information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) as char_maxlen,
				information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) as num_precision,
				information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)) as num_scale,
				not a.attnotnull as is_nullable,
				coalesce(i.indisprimary, false) as is_primary_key,
				pg_get_serial_sequence($1::regclass::text, a.attname) IS NOT NULL as is_auto_increment,
				pg_get_expr(ad.adbin, ad.adrelid) as column_default,
				col_description(a.attrelid, a.attnum) as column_comment
			FROM pg_attribute a
			LEFT JOIN pg_index i ON
				i.indrelid = a.attrelid AND
				a.attnum = ANY(i.indkey)
			LEFT JOIN pg_type t ON
				a.atttypid = t.oid
			LEFT JOIN pg_attrdef ad ON
				ad.adrelid = a.attrelid AND
				ad.adnum = a.attnum
			WHERE a.attrelid = $1::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
			ORDER BY a.attnum`

	rows, err := db.Query(query, tableInfo.TableName)
	if err != nil {
		return nil, fmt.Errorf("获取列信息失败: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col define.ColumnInfo
		var length, charMaxLen, numPrecision, numScale sql.NullInt64
		var precision sql.NullInt64
		var defaultValue, comment sql.NullString

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&length,
			&precision,
			&charMaxLen,
			&numPrecision,
			&numScale,
			&col.IsNullable,
			&col.IsPrimaryKey,
			&col.IsAutoIncrement,
			&defaultValue,
			&comment,
		)
		if err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// 设置列属性
		if charMaxLen.Valid {
			col.Length = charMaxLen.Int64
		} else if length.Valid {
			col.Length = length.Int64
		}

		if numPrecision.Valid {
			col.Precision = int(numPrecision.Int64)
		} else if precision.Valid {
			col.Precision = int(precision.Int64)
		}

		if numScale.Valid {
			col.Scale = int(numScale.Int64)
		}

		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}

		if comment.Valid {
			col.Comment = comment.String
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
