package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v4/define"
)

func init() {
	define.RegisterFactory("mysql", New())
}

// MySQLFactory implements SQLFactory interface for MySQL
type MySQLFactory struct{}

// New creates a new MySQLFactory instance
func New() define.SQLFactory {
	return &MySQLFactory{}
}

// Connect creates a new DB connection and returns a DB instance
func (f *MySQLFactory) Connect(dsn string) (*define.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return define.NewDB(db, f), nil
}

// GenerateSelectSQL generates a SELECT statement
func (f *MySQLFactory) GenerateSelectSQL(table string, fields []string, where string, orderBy string, limit, offset int) string {
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
func (f *MySQLFactory) GenerateInsertSQL(table string, fields []string) string {
	placeholders := make([]string, len(fields))
	for i := range fields {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)
}

// GenerateUpdateSQL generates an UPDATE statement
func (f *MySQLFactory) GenerateUpdateSQL(table string, fields []string, where string) string {
	sets := make([]string, len(fields))
	for i, field := range fields {
		sets[i] = field + " = ?"
	}

	query := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(sets, ", "))
	if where != "" {
		query += " WHERE " + where
	}

	return query
}

// GenerateDeleteSQL generates a DELETE statement
func (f *MySQLFactory) GenerateDeleteSQL(table string, where string) string {
	query := "DELETE FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	return query
}

// GenerateBatchInsertSQL generates a batch INSERT statement
func (f *MySQLFactory) GenerateBatchInsertSQL(table string, fields []string, valueCount int) string {
	placeholders := make([]string, len(fields))
	for i := range fields {
		placeholders[i] = "?"
	}

	valueGroups := make([]string, valueCount)
	valueStr := "(" + strings.Join(placeholders, ", ") + ")"
	for i := range valueGroups {
		valueGroups[i] = valueStr
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(fields, ", "),
		strings.Join(valueGroups, ", "),
	)
}

// GenerateBatchUpdateSQL generates a batch UPDATE statement
func (f *MySQLFactory) GenerateBatchUpdateSQL(table string, fields []string, where string, valueCount int) string {
	cases := make([]string, len(fields))
	for i, field := range fields {
		whenClauses := make([]string, valueCount)
		paramIndex := i
		for j := range whenClauses {
			whenClauses[j] = fmt.Sprintf("WHEN ? THEN ?")
			paramIndex += len(fields)
		}
		cases[i] = fmt.Sprintf("%s = CASE id %s END", field, strings.Join(whenClauses, " "))
	}

	query := fmt.Sprintf("UPDATE %s SET %s", table, strings.Join(cases, ", "))
	if where != "" {
		query += " WHERE " + where
	}

	return query
}

// GenerateBatchDeleteSQL generates a batch DELETE statement
func (f *MySQLFactory) GenerateBatchDeleteSQL(table string, where string, valueCount int) string {
	placeholders := make([]string, valueCount)
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		table,
		where,
		strings.Join(placeholders, ", "),
	)
}
