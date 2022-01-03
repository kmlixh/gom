package gom

import (
	"reflect"
	"strings"
)

type Table interface {
	TableName() string
}
type TableModel struct {
	Table   string
	Columns []string
	Data    map[string]interface{}
	Condition
	OrderBys []OrderBy
	GroupBys []string
	Page
}

type StructModel struct {
	Type        reflect.Type
	Value       reflect.Value
	TableName   string
	ColumnNames []string
	Columns     map[string]Column
	Primary     Column
}

func (this StructModel) Clone(value reflect.Value, columnFilters ...string) StructModel {
	var names []string
	if len(columnFilters) > 0 {
		names = columnFilters
	} else {
		names = this.ColumnNames
	}
	return StructModel{this.Type, value, this.TableName, names, this.Columns, this.Primary}
}
func (model StructModel) ColumnsValues() []interface{} {
	var datas []interface{}
	for _, name := range model.ColumnNames {
		column := model.Columns[name]
		var data interface{}
		value := model.Value.FieldByName(column.FieldName)
		if !column.Auto {
			scanner, ok := value.Interface().(IScanner)
			if ok {
				data, _ = scanner.Value()
			} else {
				data = value.Interface()
			}
			datas = append(datas, data)
		}
	}
	return datas
}
func (model StructModel) StructToMap() map[string]interface{} {
	var maps map[string]interface{}
	for _, name := range model.ColumnNames {
		column := model.Columns[name]
		value := model.Value.FieldByName(column.FieldName)
		maps[name] = value.Interface()
	}
	return maps
}

type Column struct {
	reflect.Type
	ColumnName string
	FieldName  string
	QueryField string
	IsPrimary  bool
	Auto       bool
}

func (this Column) Clone() Column {
	return Column{this.Type, this.ColumnName, this.FieldName, this.QueryField, this.IsPrimary, this.Auto}
}

func (mo StructModel) InsertValues() []interface{} {
	var interfaces []interface{}
	results := reflect.Indirect(reflect.ValueOf(&interfaces))
	for _, name := range mo.ColumnNames {
		if !mo.Primary.Auto || !strings.EqualFold(mo.Primary.ColumnName, name) {
			vars := reflect.ValueOf(mo.Value.FieldByName(mo.Columns[name].FieldName).Interface())
			if results.Kind() == reflect.Ptr {
				results.Set(reflect.Append(results, vars.Addr()))
			} else {
				results.Set(reflect.Append(results, vars))
			}
		}

	}
	return interfaces
}
func (m StructModel) GetPrimary() interface{} {
	return m.Value.FieldByName(m.Primary.FieldName).Interface()
}
func (m StructModel) GetPrimaryCondition() Condition {
	if m.Type.Kind() != reflect.Struct {
		return nil
	}
	if IsEmpty(m.GetPrimary()) || m.Primary.IsPrimary == false {
		return nil
	} else {
		t := m.Primary.Type
		switch t.Kind() {
		case reflect.Int64, reflect.Int32, reflect.Int, reflect.Int8, reflect.Int16:
			if m.GetPrimary().(int64) == 0 {
				return nil
			}
		}
		switch t.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			if m.GetPrimary().(int64) == 0 {
				return nil
			}
		}
		switch t.Kind() {
		case reflect.Float32, reflect.Float64:
			if m.GetPrimary().(float64) == 0 {
				return nil
			}
		}
		if t.Kind() == reflect.String {
			if len(m.GetPrimary().(string)) == 0 {
				return nil
			}
		}
		return &ConditionImpl{field: m.Primary.ColumnName}
	}
}
