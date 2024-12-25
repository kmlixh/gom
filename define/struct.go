package define

import (
	"database/sql"
)

var Debug bool

// FactoryMap stores registered SQL factories
var FactoryMap = make(map[string]SQLFactory)

// RegisterFactory registers a SQL factory with the given name
func RegisterFactory(name string, factory SQLFactory) {
	FactoryMap[name] = factory
}

// GetFactory returns a registered SQL factory by name
func GetFactory(name string) (SQLFactory, bool) {
	factory, ok := FactoryMap[name]
	return factory, ok
}

type SQLFactory interface {
	Connect(dsn string) (*sql.DB, error)
	GenerateSelectSQL(table string, fields []string, where string, orderBy string, limit, offset int) string
	GenerateInsertSQL(table string, fields []string) string
	GenerateUpdateSQL(table string, fields []string, where string) string
	GenerateDeleteSQL(table string, where string) string
	GenerateBatchInsertSQL(table string, fields []string, valueCount int) string
	GenerateBatchUpdateSQL(table string, fields []string, where string, valueCount int) string
	GenerateBatchDeleteSQL(table string, where string, valueCount int) string
	BuildCondition(cond *Condition) (string, []interface{})
}

// ConditionType represents the type of condition (AND/OR)
type ConditionType int

const (
	TypeAnd ConditionType = iota
	TypeOr
)

// Operator represents a comparison operator
type Operator int16

const (
	Eq Operator = iota
	Ne
	Gt
	Lt
	Gte
	Lte
	In
	NotIn
	Like
	NotLike
	LikeLeft
	LikeRight
	IsNull
	IsNotNull
	Between
)

// Condition represents a query condition
type Condition struct {
	Type     ConditionType // 条件类型: AND, OR
	Field    string        // 字段名
	Operator Operator      // 操作符: =, !=, >, <, etc.
	Value    interface{}   // 值
	Values   []interface{} // 多个值(用于IN, BETWEEN等)
	SubConds []*Condition  // 子条件(用于AND, OR)
	Raw      string        // 原始条件语句
	Args     []interface{} // 参数
}

// NewAndCondition creates a new AND condition group
func NewAndCondition(conditions ...*Condition) *Condition {
	return &Condition{
		Type:     TypeAnd,
		SubConds: conditions,
	}
}

// NewOrCondition creates a new OR condition group
func NewOrCondition(conditions ...*Condition) *Condition {
	return &Condition{
		Type:     TypeOr,
		SubConds: conditions,
	}
}
