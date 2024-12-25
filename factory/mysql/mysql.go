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

func (f *MySQLFactory) Connect(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
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

// BuildCondition builds a SQL condition from a Condition object
func (f *MySQLFactory) BuildCondition(cond *define.Condition) (string, []interface{}) {
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
		return cond.Field + " = ?", []interface{}{cond.Value}
	case define.Ne:
		return cond.Field + " != ?", []interface{}{cond.Value}
	case define.Gt:
		return cond.Field + " > ?", []interface{}{cond.Value}
	case define.Lt:
		return cond.Field + " < ?", []interface{}{cond.Value}
	case define.Gte:
		return cond.Field + " >= ?", []interface{}{cond.Value}
	case define.Lte:
		return cond.Field + " <= ?", []interface{}{cond.Value}
	case define.In:
		placeholders := make([]string, len(cond.Values))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		return cond.Field + " IN (" + strings.Join(placeholders, ", ") + ")", cond.Values
	case define.NotIn:
		placeholders := make([]string, len(cond.Values))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		return cond.Field + " NOT IN (" + strings.Join(placeholders, ", ") + ")", cond.Values
	case define.Like:
		return cond.Field + " LIKE CONCAT('%', ?, '%')", []interface{}{cond.Value}
	case define.NotLike:
		return cond.Field + " NOT LIKE CONCAT('%', ?, '%')", []interface{}{cond.Value}
	case define.LikeLeft:
		return cond.Field + " LIKE CONCAT('%', ?)", []interface{}{cond.Value}
	case define.LikeRight:
		return cond.Field + " LIKE CONCAT(?, '%')", []interface{}{cond.Value}
	case define.IsNull:
		return cond.Field + " IS NULL", nil
	case define.IsNotNull:
		return cond.Field + " IS NOT NULL", nil
	case define.Between:
		return cond.Field + " BETWEEN ? AND ?", cond.Values
	}

	return "", nil
}
