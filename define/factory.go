package define

import (
	"database/sql"
	"reflect"
)

// OrderType represents the type of ordering
type OrderType int

const (
	OrderAsc  OrderType = iota // Ascending order
	OrderDesc                  // Descending order
)

// String returns the string representation of OrderType
func (o OrderType) String() string {
	switch o {
	case OrderAsc:
		return "ASC"
	case OrderDesc:
		return "DESC"
	default:
		return "ASC"
	}
}

// OrderBy represents an order by clause
type OrderBy struct {
	Field string
	Type  OrderType
}

// TableInfo 表信息
type TableInfo struct {
	TableName    string       `json:"table_name"`    // 表名
	TableComment string       `json:"table_comment"` // 表注释
	PrimaryKeys  []string     `json:"primary_keys"`  // 主键列表
	Columns      []ColumnInfo `json:"columns"`       // 列信息
	HasDecimal   bool         `json:"has_decimal"`   // 是否包含 Decimal 类型
	HasUUID      bool         `json:"has_uuid"`      // 是否包含 UUID 类型
	HasIP        bool         `json:"has_ip"`        // 是否包含 IP 类型
	HasTime      bool         `json:"has_time"`      // 是否包含时间类型
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name            string `json:"name"`              // 列名
	TypeName        string `json:"type_name"`         // 数据库类型名称
	DataType        string `json:"data_type"`         // 标准SQL数据类型
	Length          int64  `json:"length"`            // 长度
	Precision       int    `json:"precision"`         // 精度
	Scale           int    `json:"scale"`             // 小数位数
	IsNullable      bool   `json:"is_nullable"`       // 是否可空
	IsPrimaryKey    bool   `json:"is_primary_key"`    // 是否主键
	IsAutoIncrement bool   `json:"is_auto_increment"` // 是否自增
	DefaultValue    string `json:"default_value"`     // 默认值
	Comment         string `json:"comment"`           // 注释
}

type TableStruct struct {
	TableInfo     TableInfo         `json:"table_info"`
	FieldToColMap map[string]string `json:"field_to_col_map"`
	ColToFieldMap map[string]string `json:"col_to_field_map"`
}

type ExecuteType string

const (
	Query ExecuteType = "query"
	Exec  ExecuteType = "exec"
)

type SqlProto struct {
	SqlType ExecuteType
	Sql     string
	Args    []any
	Error   error
}

// SQLFactory defines the interface for SQL query builders
type SQLFactory interface {
	// Connect creates a new database connection
	Connect(dsn string) (*sql.DB, error)

	// GetType returns the database type (e.g., "mysql", "postgres")
	GetType() string

	// BuildSelect builds a SELECT query
	BuildSelect(table string, fields []string, conditions []*Condition, orderBy string, limit, offset int) *SqlProto

	// BuildUpdate builds an UPDATE query
	BuildUpdate(table string, fields map[string]interface{}, fieldOrder []string, conditions []*Condition) *SqlProto

	// BuildInsert builds an INSERT query
	BuildInsert(table string, fields map[string]interface{}, fieldOrder []string) *SqlProto

	// BuildBatchInsert builds a batch INSERT query
	BuildBatchInsert(table string, values []map[string]interface{}) *SqlProto

	// BuildDelete builds a DELETE query
	BuildDelete(table string, conditions []*Condition) *SqlProto

	// BuildCreateTable builds a CREATE TABLE query
	BuildCreateTable(table string, modelType reflect.Type) *SqlProto

	// GetTableInfo 获取表信息
	GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error)

	// GetTables 获取符合模式的所有表
	// pattern: 表名匹配模式，支持 * 通配符
	// 对于 PostgreSQL，pattern 可以是 schema.table 格式
	GetTables(db *sql.DB, pattern string) ([]string, error)

	// BuildOrderBy builds the ORDER BY clause
	BuildOrderBy(orders []OrderBy) string
}

// ITableModel defines the interface for custom table models
type ITableModel interface {
	// TableName returns the custom table name
	TableName() string
	// CreateSql returns the custom CREATE TABLE SQL statement
	CreateSql() string
}
