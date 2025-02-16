package define

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrEmptyTableName is returned when a table name is empty
var ErrEmptyTableName = errors.New("empty table name")

// MockSQLFactory is a mock implementation of SQLFactory for testing
type MockSQLFactory struct{}

func (f *MockSQLFactory) Connect(dsn string) (*sql.DB, error) {
	return nil, nil
}

func (f *MockSQLFactory) GetType() string {
	return "mock"
}

func (f *MockSQLFactory) BuildSelect(table string, fields []string, conditions []*Condition, orderBy string, limit, offset int) (string, []interface{}, error) {
	if table == "" {
		return "", nil, ErrEmptyTableName
	}

	var args []interface{}
	var where []string

	// Validate and build conditions
	if len(conditions) > 0 {
		for _, cond := range conditions {
			if cond != nil {
				// Check for nil value
				if cond.Value == nil && cond.Op != OpIsNull && cond.Op != OpIsNotNull {
					return "", nil, fmt.Errorf("invalid condition: nil value not allowed")
				}
				// Check for invalid operator
				if cond.Op > OpCustom {
					return "", nil, fmt.Errorf("invalid operator")
				}
				// Add condition
				where = append(where, fmt.Sprintf("%s = ?", cond.Field))
				args = append(args, cond.Value)
			}
		}
	}

	// Build query
	query := "SELECT * FROM mock_table"
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}

	return query, args, nil
}

func (f *MockSQLFactory) BuildUpdate(table string, fields map[string]interface{}, fieldOrder []string, conditions []*Condition) (string, []interface{}) {
	return "", nil
}

func (f *MockSQLFactory) BuildInsert(table string, fields map[string]interface{}, fieldOrder []string) (string, []interface{}) {
	return "", nil
}

func (f *MockSQLFactory) BuildBatchInsert(table string, values []map[string]interface{}) (string, []interface{}) {
	if len(values) == 0 {
		return "", nil
	}

	// Get all unique field names from the first row
	var fieldOrder []string
	for field := range values[0] {
		fieldOrder = append(fieldOrder, field)
	}

	var args []interface{}
	var placeholders []string

	// Build field list
	fields := strings.Join(fieldOrder, ", ")

	// Build placeholders and collect args
	for _, value := range values {
		var rowPlaceholders []string
		for _, field := range fieldOrder {
			rowPlaceholders = append(rowPlaceholders, "?")
			args = append(args, value[field])
		}
		placeholders = append(placeholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, fields, strings.Join(placeholders, ", "))
	return sql, args
}

func (f *MockSQLFactory) BuildDelete(table string, conditions []*Condition) (string, []interface{}) {
	return "", nil
}

func (f *MockSQLFactory) BuildCreateTable(table string, modelType reflect.Type) string {
	return ""
}

func (f *MockSQLFactory) GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error) {
	return nil, fmt.Errorf("table %s not found", tableName)
}

func (f *MockSQLFactory) GetTables(db *sql.DB, pattern string) ([]string, error) {
	return nil, nil
}

func (f *MockSQLFactory) BuildOrderBy(orders []OrderBy) string {
	return ""
}

func (f *MockSQLFactory) BuildRawQuery(query string, args []interface{}) (*SQLQuery, error) {
	return &SQLQuery{Query: query, Args: args}, nil
}
