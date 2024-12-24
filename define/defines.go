package define

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"
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

var Debug bool

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
	ToTableModel() (TableModel, IRowScanner, error)
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

func (t TableStruct) ToTableModel() (TableModel, IRowScanner, error) {
	cols, err := t.GetColumns()
	if err != nil {
		return nil, nil, err
	}

	colNames := make([]string, len(cols))
	colMap := make(map[string]interface{})
	primaryKeys := make([]string, 0)
	scanDest := make([]interface{}, len(cols))

	for i, col := range cols {
		colNames[i] = col.ColumnName
		colMap[col.ColumnName] = col.ColumnValue
		if col.IsPrimary {
			primaryKeys = append(primaryKeys, col.ColumnName)
		}
		// 根据列类型创建对应的扫描目标
		scanDest[i] = getScanDest(col.Type)
	}

	model := &DefaultModel{
		table:         t.TableName,
		primaryKeys:   primaryKeys,
		columns:       colNames,
		columnDataMap: colMap,
		condition:     nil,
		orderBys:      nil,
		page:          nil,
	}

	scanner := &TableScanner{
		dest:    scanDest,
		columns: cols,
	}

	return model, scanner, nil
}

// TableScanner implements IRowScanner interface
type TableScanner struct {
	dest    []interface{}
	columns []Column
}

// ScanResult wraps scan results with conversion methods
type ScanResult struct {
	Data []map[string]interface{}
}

// Into converts scan results into the provided struct slice
func (r *ScanResult) Into(dest interface{}) error {
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be a pointer")
	}

	sliceValue := destValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	// Get the type of slice elements
	elemType := sliceValue.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("slice elements must be structs")
	}

	// Create a new slice with the correct capacity
	newSlice := reflect.MakeSlice(sliceValue.Type(), 0, len(r.Data))

	// Iterate through each map and create struct instances
	for _, item := range r.Data {
		// Create a new struct instance
		structPtr := reflect.New(elemType)
		structVal := structPtr.Elem()

		// Fill the struct fields
		for i := 0; i < elemType.NumField(); i++ {
			field := elemType.Field(i)
			fieldVal := structVal.Field(i)

			// Get the column name from gom tag
			tag := field.Tag.Get("gom")
			if tag == "" || tag == "-" {
				continue
			}

			// Get the value from map
			if value, ok := item[tag]; ok {
				if err := setFieldValue(fieldVal, value); err != nil {
					return fmt.Errorf("failed to set field %s: %v", field.Name, err)
				}
			}
		}

		// Append the struct to slice
		if isPtr {
			newSlice = reflect.Append(newSlice, structPtr)
		} else {
			newSlice = reflect.Append(newSlice, structVal)
		}
	}

	// Set the result back to the destination slice
	sliceValue.Set(newSlice)
	return nil
}

// setFieldValue sets the appropriate value to the struct field
func setFieldValue(field reflect.Value, value interface{}) error {
	if value == nil {
		return nil
	}

	// Handle the case where value is a pointer
	valueVal := reflect.ValueOf(value)
	if valueVal.Kind() == reflect.Ptr {
		if valueVal.IsNil() {
			return nil
		}
		valueVal = valueVal.Elem()
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := valueVal.Interface().(type) {
		case int64:
			field.SetInt(v)
		case int:
			field.SetInt(int64(v))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := valueVal.Interface().(type) {
		case uint64:
			field.SetUint(v)
		case uint:
			field.SetUint(uint64(v))
		}
	case reflect.Float32, reflect.Float64:
		switch v := valueVal.Interface().(type) {
		case float64:
			field.SetFloat(v)
		case float32:
			field.SetFloat(float64(v))
		}
	case reflect.String:
		if s, ok := valueVal.Interface().(string); ok {
			field.SetString(s)
		}
	case reflect.Bool:
		if b, ok := valueVal.Interface().(bool); ok {
			field.SetBool(b)
		}
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			if t, ok := valueVal.Interface().(time.Time); ok {
				field.Set(reflect.ValueOf(t))
			}
		}
	}
	return nil
}

func (s *TableScanner) Scan(rows *sql.Rows) (interface{}, error) {
	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(s.dest...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range s.columns {
			// Get the actual value from the pointer
			value := reflect.ValueOf(s.dest[i]).Elem().Interface()
			row[col.ColumnName] = value
		}
		results = append(results, row)
	}
	return &ScanResult{Data: results}, nil
}

// getScanDest returns appropriate scan destination based on type
func getScanDest(t reflect.Type) interface{} {
	if t == nil {
		return new(interface{})
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(int64)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return new(uint64)
	case reflect.Float32, reflect.Float64:
		return new(float64)
	case reflect.Bool:
		return new(bool)
	case reflect.String:
		return new(string)
	case reflect.Struct:
		if t.String() == "time.Time" {
			return new(time.Time)
		}
		fallthrough
	default:
		return new(interface{})
	}
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

// DefaultModel implements TableModel interface
type DefaultModel struct {
	table         string
	primaryKeys   []string
	columns       []string
	columnDataMap map[string]interface{}
	condition     Condition
	orderBys      []OrderBy
	page          PageInfo
}

func (m *DefaultModel) Table() string {
	return m.table
}

func (m *DefaultModel) PrimaryKeys() []string {
	return m.primaryKeys
}

func (m *DefaultModel) Columns() []string {
	return m.columns
}

func (m *DefaultModel) ColumnDataMap() map[string]interface{} {
	return m.columnDataMap
}

func (m *DefaultModel) Condition() Condition {
	return m.condition
}

func (m *DefaultModel) OrderBys() []OrderBy {
	return m.orderBys
}

func (m *DefaultModel) Page() PageInfo {
	return m.page
}
