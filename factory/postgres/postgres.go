package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/kmlixh/gom/v4/define"
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
func (f *PostgresFactory) Connect(dsn string) (*sql.DB, error) {
	// Use pgx driver name
	return sql.Open("pgx", dsn)

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
	if define.Debug {
		fmt.Println("sql:", query)
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

// BuildCondition builds a SQL condition from a Condition object
func (f *PostgresFactory) BuildCondition(cond *define.Condition) (string, []interface{}) {
	if cond.Raw != "" {
		return cond.Raw, cond.Args
	}

	switch cond.Type {
	case define.TypeAnd, define.TypeOr:
		var subclauses []string
		var args []interface{}
		for _, sub := range cond.SubConds {
			subclause, subargs := f.BuildCondition(sub)
			if subclause != "" {
				subclauses = append(subclauses, "("+subclause+")")
				args = append(args, subargs...)
			}
		}
		if len(subclauses) == 0 {
			return "", nil
		}
		op := " AND "
		if cond.Type == define.TypeOr {
			op = " OR "
		}
		return strings.Join(subclauses, op), args
	}

	switch cond.Operator {
	case define.Eq:
		return cond.Field + " = $1", []interface{}{cond.Value}
	case define.Ne:
		return cond.Field + " != $1", []interface{}{cond.Value}
	case define.Gt:
		return cond.Field + " > $1", []interface{}{cond.Value}
	case define.Lt:
		return cond.Field + " < $1", []interface{}{cond.Value}
	case define.Gte:
		return cond.Field + " >= $1", []interface{}{cond.Value}
	case define.Lte:
		return cond.Field + " <= $1", []interface{}{cond.Value}
	case define.In:
		placeholders := make([]string, len(cond.Values))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		return cond.Field + " IN (" + strings.Join(placeholders, ", ") + ")", cond.Values
	case define.NotIn:
		placeholders := make([]string, len(cond.Values))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		return cond.Field + " NOT IN (" + strings.Join(placeholders, ", ") + ")", cond.Values
	case define.Like:
		return cond.Field + " LIKE '%' || $1 || '%'", []interface{}{cond.Value}
	case define.NotLike:
		return cond.Field + " NOT LIKE '%' || $1 || '%'", []interface{}{cond.Value}
	case define.LikeLeft:
		return cond.Field + " LIKE '%' || $1", []interface{}{cond.Value}
	case define.LikeRight:
		return cond.Field + " LIKE $1 || '%'", []interface{}{cond.Value}
	case define.IsNull:
		return cond.Field + " IS NULL", nil
	case define.IsNotNull:
		return cond.Field + " IS NOT NULL", nil
	case define.Between:
		return cond.Field + " BETWEEN $1 AND $2", cond.Values
	}

	return "", nil
}
