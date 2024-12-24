package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kmlixh/gom/define"
)

func init() {
	define.RegisterFactory("postgres", New())
}

// PostgresFactory implements SQLFactory interface for PostgreSQL
type PostgresFactory struct{}

// New creates a new PostgresFactory instance
func New() define.SQLFactory {
	return &PostgresFactory{}
}

// Connect creates a new DB connection and returns a DB instance
func (f *PostgresFactory) Connect(dsn string) (*define.DB, error) {
	// Use pgx driver name
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	return define.NewDB(db, f), nil
}

// GenerateSelectSQL generates a SELECT statement
func (f *PostgresFactory) GenerateSelectSQL(table string, fields []string, where string, orderBy string, limit, offset int) string {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), table)

	if where != "" {
		query += " WHERE " + where
	}

	if orderBy != "" {
		query += " ORDER BY " + orderBy
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	return query
}

// GenerateInsertSQL generates an INSERT statement
func (f *PostgresFactory) GenerateInsertSQL(table string, fields []string) string {
	placeholders := make([]string, len(fields))
	for i := range fields {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)
}

// GenerateUpdateSQL generates an UPDATE statement
func (f *PostgresFactory) GenerateUpdateSQL(table string, fields []string, where string) string {
	sets := make([]string, len(fields))
	for i, field := range fields {
		sets[i] = fmt.Sprintf("%s = $%d", field, i+1)
	}

	query := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(sets, ", "))
	if where != "" {
		query += " WHERE " + where
	}

	return query
}

// GenerateDeleteSQL generates a DELETE statement
func (f *PostgresFactory) GenerateDeleteSQL(table string, where string) string {
	query := "DELETE FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	return query
}

// GenerateBatchInsertSQL generates a batch INSERT statement
func (f *PostgresFactory) GenerateBatchInsertSQL(table string, fields []string, valueCount int) string {
	paramCount := len(fields)
	valueGroups := make([]string, valueCount)

	for i := range valueGroups {
		placeholders := make([]string, paramCount)
		for j := range placeholders {
			placeholders[j] = fmt.Sprintf("$%d", i*paramCount+j+1)
		}
		valueGroups[i] = "(" + strings.Join(placeholders, ", ") + ")"
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(fields, ", "),
		strings.Join(valueGroups, ", "),
	)
}

// GenerateBatchUpdateSQL generates a batch UPDATE statement
func (f *PostgresFactory) GenerateBatchUpdateSQL(table string, fields []string, where string, valueCount int) string {
	paramCount := len(fields)
	valueGroups := make([]string, valueCount)

	for i := range valueGroups {
		placeholders := make([]string, paramCount)
		for j := range placeholders {
			placeholders[j] = fmt.Sprintf("$%d", i*paramCount+j+1)
		}
		valueGroups[i] = "(" + strings.Join(placeholders, ", ") + ")"
	}

	query := fmt.Sprintf(`
		WITH updated_values (id, %s) AS (
			VALUES %s
		)
		UPDATE %s SET
	`, strings.Join(fields, ", "), strings.Join(valueGroups, ", "), table)

	sets := make([]string, len(fields))
	for i, field := range fields {
		sets[i] = fmt.Sprintf("%s = updated_values.%s", field, field)
	}

	query += strings.Join(sets, ", ")
	query += " FROM updated_values WHERE " + table + ".id = updated_values.id"
	if where != "" {
		query += " AND " + where
	}

	return query
}

// GenerateBatchDeleteSQL generates a batch DELETE statement
func (f *PostgresFactory) GenerateBatchDeleteSQL(table string, where string, valueCount int) string {
	placeholders := make([]string, valueCount)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		table,
		where,
		strings.Join(placeholders, ", "),
	)
}
