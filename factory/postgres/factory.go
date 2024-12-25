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
