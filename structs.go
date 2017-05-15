package gom

import (
	"reflect"
)

type SqlFactory interface {
	Insert(TableModel) (string, []interface{})
	Update(TableModel) (string, []interface{})
	Replace(TableModel) (string, []interface{})
	Delete(TableModel) (string, []interface{})
	Query(TableModel) (string, []interface{})
}
type RowChooser interface {
	Scan(dest ...interface{}) error
}

type TableModel struct {
	ModelType  reflect.Type
	ModelValue reflect.Value
	TableName  string
	Columns    []Column
	Primary    Column
	Cnd        Condition
}
type Column struct {
	ColumnType reflect.Type
	ColumnName string
	FieldName  string
	Auto       bool
}
type Condition interface {
	State() string
	Value() []interface{}
}
type Conditions struct {
	States string
	Values []interface{}
}

func (c Conditions) State() string {
	return c.States
}
func (c Conditions) Value() []interface{} {
	return c.Values
}

func (mo TableModel) InsertValues() []interface{} {
	var interfaces []interface{}
	results := reflect.Indirect(reflect.ValueOf(&interfaces))
	for _, column := range mo.Columns {
		vars := reflect.ValueOf(mo.ModelValue.FieldByName(column.FieldName).Interface())
		if results.Kind() == reflect.Ptr {
			results.Set(reflect.Append(results, vars.Addr()))
		} else {
			results.Set(reflect.Append(results, vars))
		}
	}
	return interfaces
}
func (m TableModel) GetPrimary() interface{} {
	return m.ModelValue.FieldByName(m.Primary.FieldName).Interface()
}
func (m TableModel) GetPrimaryCondition() Condition {
	if IsEmpty(m.GetPrimary()) {
		return nil
	} else {
		return Conditions{m.Primary.ColumnName + " = ?", []interface{}{m.GetPrimary()}}
	}
}
