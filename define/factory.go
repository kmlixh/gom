package define

import (
	"database/sql"
	"reflect"
)

// SQLFactory defines the interface for SQL generation
type SQLFactory interface {
	// Connect creates a new database connection
	Connect(dsn string) (*sql.DB, error)

	// BuildSelect builds a SELECT query
	BuildSelect(table string, fields []string, conditions []*Condition, orderBy string, limit, offset int) (string, []interface{})

	// BuildUpdate builds an UPDATE query
	BuildUpdate(table string, fields map[string]interface{}, conditions []*Condition) (string, []interface{})

	// BuildInsert builds an INSERT query
	BuildInsert(table string, fields map[string]interface{}) (string, []interface{})

	// BuildBatchInsert builds a batch INSERT query
	BuildBatchInsert(table string, values []map[string]interface{}) (string, []interface{})

	// BuildDelete builds a DELETE query
	BuildDelete(table string, conditions []*Condition) (string, []interface{})

	// BuildCreateTable builds a CREATE TABLE query
	BuildCreateTable(table string, modelType reflect.Type) string
}

// Debug flag for enabling debug mode
var Debug bool

// ITableModel defines the interface for custom table models
type ITableModel interface {
	// TableName returns the custom table name
	TableName() string
	// CreateSql returns the custom CREATE TABLE SQL statement
	CreateSql() string
}
