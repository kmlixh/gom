package define

import (
	"database/sql"
	"reflect"
)

type Linker int

const (
	_ Linker = iota
	And
	Or
)

type Operation int

const (
	_ Operation = iota
	Eq
	NotEq
	Ge
	Gt
	Le
	Lt
	Like
	LikeIgnoreStart
	LikeIgnoreEnd
	NotLike
	In
	NotIn
	IsNull
	IsNotNull
	RawOperation
)

type OrderType int

type SqlType int
type Condition interface {
	PayLoads() int64 //有效荷载，说明当前条件及其链式条件共有多少有多少个条件
	Linker() Linker
	Field() string
	Operation() Operation
	Values() []interface{}
	SetValues([]interface{})
	Items() []Condition
	HasSubConditions() bool
	RawExpression() string
	Eq(field string, values interface{}) Condition
	EqBool(b bool, field string, value interface{}) Condition
	OrEq(field string, value interface{}) Condition
	OrEqBool(b bool, field string, value interface{}) Condition
	Ge(field string, value interface{}) Condition
	GeBool(b bool, field string, value interface{}) Condition
	OrGe(field string, value interface{}) Condition
	OrGeBool(b bool, field string, value interface{}) Condition
	Gt(field string, values interface{}) Condition
	GtBool(b bool, field string, values interface{}) Condition
	OrGt(field string, values interface{}) Condition
	OrGtBool(b bool, field string, values interface{}) Condition
	Le(field string, values interface{}) Condition
	LeBool(b bool, field string, values interface{}) Condition
	OrLe(field string, values interface{}) Condition
	OrLeBool(b bool, field string, values interface{}) Condition
	Lt(field string, values interface{}) Condition
	LtBool(b bool, field string, values interface{}) Condition
	OrLt(field string, values interface{}) Condition
	OrLtBool(b bool, field string, values interface{}) Condition
	NotEq(field string, values interface{}) Condition
	NotEqBool(b bool, field string, values interface{}) Condition
	OrNotEq(field string, values interface{}) Condition
	OrNotEqBool(b bool, field string, values interface{}) Condition
	In(field string, values ...interface{}) Condition
	InBool(b bool, field string, values ...interface{}) Condition
	OrIn(field string, values ...interface{}) Condition
	OrInBool(b bool, field string, values ...interface{}) Condition
	NotIn(field string, values ...interface{}) Condition
	NotInBool(b bool, field string, values ...interface{}) Condition
	OrNotIn(field string, values ...interface{}) Condition
	OrNotInBool(b bool, field string, values ...interface{}) Condition
	Like(field string, values interface{}) Condition
	LikeBool(b bool, field string, values interface{}) Condition
	OrLike(field string, values interface{}) Condition
	OrLikeBool(b bool, field string, values interface{}) Condition
	NotLike(field string, values interface{}) Condition
	NotLikeBool(b bool, field string, values interface{}) Condition
	OrNotLike(field string, values interface{}) Condition
	OrNotLikeBool(b bool, field string, values interface{}) Condition
	LikeIgnoreStart(field string, values interface{}) Condition
	LikeIgnoreStartBool(b bool, field string, values interface{}) Condition
	OrLikeIgnoreStart(field string, values interface{}) Condition
	OrLikeIgnoreStartBool(b bool, field string, values interface{}) Condition
	LikeIgnoreEnd(field string, values interface{}) Condition
	LikeIgnoreEndBool(b bool, field string, values interface{}) Condition
	OrLikeIgnoreEnd(field string, values interface{}) Condition
	OrLikeIgnoreEndBool(b bool, field string, values interface{}) Condition
	IsNull(filed string) Condition
	IsNullBool(b bool, field string) Condition
	IsNotNull(field string) Condition
	IsNotNullBool(b bool, field string) Condition
	OrIsNull(filed string) Condition
	OrIsNullBool(b bool, field string) Condition
	OrIsNotNull(field string) Condition
	OrIsNotNullBool(b bool, field string) Condition
	And(field string, operation Operation, value ...interface{}) Condition
	AndBool(b bool, field string, operation Operation, value ...interface{}) Condition
	And2(condition Condition) Condition
	And3(rawExpresssion string, values ...interface{}) Condition
	And3Bool(b bool, rawExpresssion string, values ...interface{}) Condition
	Or(field string, operation Operation, value ...interface{}) Condition
	OrBool(b bool, field string, operation Operation, value ...interface{}) Condition
	Or2(condition Condition) Condition
	Or3(rawExpresssion string, values ...interface{}) Condition
	Or3Bool(b bool, rawExpresssion string, values ...interface{}) Condition
}

const (
	_ SqlType = iota
	Query
	Insert
	Update
	Delete
)

const (
	_ OrderType = iota
	Asc
	Desc
)

type SqlProto struct {
	PreparedSql string
	Data        []interface{}
}
type OrderBy interface {
	Name() string
	Type() OrderType
}
type PageInfo interface {
	Page() (int64, int64)
}
type TableModel interface {
	Table() string
	PrimaryKeys() []string
	Columns() []string
	ColumnDataMap() map[string]interface{}
	Condition() Condition
	OrderBys() []OrderBy
	Page() PageInfo
}
type SqlFunc func(model ...TableModel) []SqlProto
type SqlFactory interface {
	OpenDb(dsn string) (*sql.DB, error)
	GetTables(db *sql.DB) ([]string, error)
	GetCurrentSchema(db *sql.DB) (string, error)
	GetColumns(tableName string, db *sql.DB) ([]Column, error)
	GetSqlFunc(sqlType SqlType) SqlFunc
	ConditionToSql(preTag bool, condition Condition) (string, []interface{})
	GetTableStruct(tableName string, db *sql.DB) (ITableStruct, error)
	GetSqlTypeDefaultValue(sqlType string) any
}
type ITableStruct interface {
	GetTableName() string
	GetTableComment() string
	GetColumns() ([]Column, error)
}
type IRowScanner interface {
	Scan(rows *sql.Rows) (interface{}, error)
}

type TableStruct struct {
	TableName    string
	TableComment string
	Columns      []Column
}

func (t TableStruct) GetTableName() string {
	return t.TableName
}

func (t TableStruct) GetTableComment() string {
	return t.TableComment
}

func (t TableStruct) GetColumns() ([]Column, error) {
	return t.Columns, nil
}

type Column struct {
	QueryName     string       `json:"queryName"`
	ColumnName    string       `json:"ColumnName"`
	IsPrimary     bool         `json:"isPrimary"`
	IsPrimaryAuto bool         `json:"isPrimaryAuto"` //If IsPrimary Key Auto Generate Or2 Not
	TypeName      string       `json:"type"`
	Type          reflect.Type `json:"-"`
	ColumnValue   any          `json:"value"`
	Comment       string       `json:"comment"`
}
