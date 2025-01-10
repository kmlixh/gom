package define

import (
	"database/sql"
	"errors"
	"reflect"
	"sort"
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

func (f *MockSQLFactory) BuildSelect(table string, fields []string, conditions []*Condition, orderBy string, limit, offset int) (string, []interface{}) {
	return "", nil
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

	// Get all field names
	fieldSet := make(map[string]struct{})
	for _, row := range values {
		for field := range row {
			fieldSet[field] = struct{}{}
		}
	}

	// Convert to sorted slice
	var fields []string
	for field := range fieldSet {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	// Build placeholders and args
	var args []interface{}
	for _, row := range values {
		for _, field := range fields {
			if value, ok := row[field]; ok {
				args = append(args, value)
			} else {
				args = append(args, nil)
			}
		}
	}

	// Return mock SQL and args
	return "INSERT INTO mock_table VALUES (?)", args
}

func (f *MockSQLFactory) BuildDelete(table string, conditions []*Condition) (string, []interface{}) {
	return "", nil
}

func (f *MockSQLFactory) BuildCreateTable(table string, modelType reflect.Type) string {
	return ""
}

func (f *MockSQLFactory) GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error) {
	return nil, nil
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
