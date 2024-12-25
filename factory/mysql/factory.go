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
	define.RegisterFactory("mysql", &Factory{})
}

func (f *Factory) Connect(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
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
		for _, cond := range conditions {
			condStrings = append(condStrings, fmt.Sprintf("`%s` %s ?", cond.Field, cond.Op))
			args = append(args, cond.Value)
		}
		query += strings.Join(condStrings, " AND ")
	}

	// Add order by
	if orderBy != "" {
		var orderParts []string
		for _, part := range strings.Split(orderBy, ",") {
			part = strings.TrimSpace(part)
			if strings.Contains(strings.ToUpper(part), "DESC") {
				field := strings.TrimSpace(strings.TrimSuffix(strings.ToUpper(part), "DESC"))
				orderParts = append(orderParts, fmt.Sprintf("`%s` DESC", field))
			} else if strings.Contains(strings.ToUpper(part), "ASC") {
				field := strings.TrimSpace(strings.TrimSuffix(strings.ToUpper(part), "ASC"))
				orderParts = append(orderParts, fmt.Sprintf("`%s` ASC", field))
			} else {
				orderParts = append(orderParts, fmt.Sprintf("`%s`", part))
			}
		}
		query += " ORDER BY " + strings.Join(orderParts, ", ")
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

	// Add fields
	var fieldStrings []string
	for field, value := range fields {
		if field != "id" { // Skip id field
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
	if len(conditions) > 0 {
		query += " WHERE "
		var condStrings []string
		for _, cond := range conditions {
			condStrings = append(condStrings, fmt.Sprintf("`%s` %s ?", cond.Field, cond.Op))
			args = append(args, cond.Value)
		}
		query += strings.Join(condStrings, " AND ")
	}

	return query, args
}

// BuildInsert builds an INSERT query for MySQL
func (f *Factory) BuildInsert(table string, fields map[string]interface{}) (string, []interface{}) {
	var args []interface{}
	var fieldNames []string
	var placeholders []string

	for field, value := range fields {
		fieldNames = append(fieldNames, fmt.Sprintf("`%s`", field))
		placeholders = append(placeholders, "?")
		args = append(args, value)
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s)",
		table,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	)

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
	var args []interface{}
	query := fmt.Sprintf("DELETE FROM `%s`", table)

	// Add conditions
	if len(conditions) > 0 {
		query += " WHERE "
		var condStrings []string
		for _, cond := range conditions {
			condStrings = append(condStrings, fmt.Sprintf("`%s` %s ?", cond.Field, cond.Op))
			args = append(args, cond.Value)
		}
		query += strings.Join(condStrings, " AND ")
	}

	return query, args
}

// BuildCreateTable builds a CREATE TABLE query for MySQL
func (f *Factory) BuildCreateTable(table string, modelType reflect.Type) string {
	var columns []string
	var indexes []string

	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" || tag == "-" {
			continue
		}

		// Parse tag
		parts := strings.Split(tag, ",")
		columnName := parts[0]
		var constraints []string
		if len(parts) > 1 {
			constraints = parts[1:]
		}

		// Start building column definition
		columnDef := columnName

		// Add data type based on field type
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			columnDef += " INT"
		case reflect.Int64:
			columnDef += " BIGINT"
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
			columnDef += " VARCHAR(" + size + ")"
		case reflect.Bool:
			columnDef += " BOOLEAN"
		case reflect.Struct:
			if field.Type == reflect.TypeOf(time.Time{}) {
				columnDef += " TIMESTAMP"
			}
		}

		// Add constraints
		for _, constraint := range constraints {
			switch constraint {
			case "primary", "!":
				columnDef += " PRIMARY KEY"
			case "primaryAuto", "@":
				columnDef += " PRIMARY KEY AUTO_INCREMENT"
			case "notnull":
				columnDef += " NOT NULL"
			case "unique":
				indexName := fmt.Sprintf("idx_%s_%s", table, columnName)
				indexes = append(indexes, fmt.Sprintf("UNIQUE KEY `%s` (`%s`)", indexName, columnName))
			case "index":
				indexName := fmt.Sprintf("idx_%s_%s", table, columnName)
				indexes = append(indexes, fmt.Sprintf("KEY `%s` (`%s`)", indexName, columnName))
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
						indexes = append(indexes, fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)",
							fkName, columnName, parts[0], parts[1]))
					}
				}
			}
		}

		// Special handling for created_at and updated_at
		if columnName == "created_at" {
			columnDef += " NOT NULL DEFAULT CURRENT_TIMESTAMP"
		} else if columnName == "updated_at" {
			columnDef += " NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
		}

		columns = append(columns, columnDef)
	}

	// Combine columns and indexes
	allDefinitions := append(columns, indexes...)

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
		table, strings.Join(allDefinitions, ",\n  "))
}
