package structs

import (
	"reflect"
	"strings"
)

var Debug bool

type SqlProto struct {
	Sql  string
	Data []interface{}
}
type DefaultStruct struct {
	Defaults string
}

type TableModel struct {
	Table   string
	Columns []string
	Data    map[string]interface{}
	Condition
	OrderBys []OrderBy
	Page
}

type GenerateSQLFunc func(model ...TableModel) []SqlProto
type SqlFactory interface {
	GetSqlFunc(sqlType SqlType) GenerateSQLFunc
	ConditionToSql(condition Condition) (string, []interface{})
}

type OrderType int

type SqlType int

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

type OrderBy interface {
	Name() string
	Type() OrderType
}

type OrderByImpl struct {
	name      string
	orderType OrderType
}

func MakeOrderBy(name string, orderType OrderType) OrderBy {
	return OrderByImpl{name, orderType}
}
func (o OrderByImpl) Name() string {
	return o.name
}
func (o OrderByImpl) Type() OrderType {
	return o.orderType
}

type Page interface {
	Page() (int, int)
}

type PageImpl struct {
	index int
	size  int
}

func MakePage(index int, size int) Page {
	return PageImpl{index, size}
}

func (p PageImpl) Page() (int, int) {
	return p.index, p.size
}

type CountResult struct {
	Count int64
	Error error
}
type Table interface {
	TableName() string
}

type StructModel struct {
	Type            reflect.Type
	Value           reflect.Value
	TableName       string
	ColumnNames     []string
	ColumnMap       map[string]Column
	Primary         Column
	HasColumnFilter bool
	DataMap         map[string]interface{}
}

func (this StructModel) Clone(value reflect.Value, columnFilters ...string) StructModel {
	var names []string
	var hasColumnFilter = false
	if len(columnFilters) > 0 {
		hasColumnFilter = true
		for _, col := range columnFilters {
			_, ok := this.ColumnMap[col]
			if !ok {
				col = CamelToSnakeString(col)
				_, ok = this.ColumnMap[col]
			}
			if ok {
				names = append(names, col)
			}
		}
	} else {
		names = this.ColumnNames
	}
	return StructModel{this.Type, value, this.TableName, names, this.ColumnMap, this.Primary, hasColumnFilter, nil}
}
func (model StructModel) ColumnsValues() []interface{} {
	var datas []interface{}
	for _, name := range model.ColumnNames {
		column := model.ColumnMap[name]
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

type Column struct {
	reflect.Type
	ColumnName string
	FieldName  string
	IsPrimary  bool
	Auto       bool
}

func (this Column) Clone() Column {
	return Column{this.Type, this.ColumnName, this.FieldName, this.IsPrimary, this.Auto}
}

func (mo StructModel) InsertValues() []interface{} {
	var interfaces []interface{}
	results := reflect.Indirect(reflect.ValueOf(&interfaces))
	for _, name := range mo.ColumnNames {
		if !mo.Primary.Auto || !strings.EqualFold(mo.Primary.ColumnName, name) {
			vars := reflect.ValueOf(mo.Value.FieldByName(mo.ColumnMap[name].FieldName).Interface())
			if results.Kind() == reflect.Ptr {
				results.Set(reflect.Append(results, vars.Addr()))
			} else {
				results.Set(reflect.Append(results, vars))
			}
		}

	}
	return interfaces
}
func (m StructModel) GetPrimary() reflect.Value {
	return m.Value.FieldByName(m.Primary.FieldName)
}
func (m StructModel) GetPrimaryCondition() Condition {
	if m.Value.Kind() != reflect.Struct {
		return nil
	}
	if IsEmpty(m.GetPrimary()) || m.Primary.IsPrimary == false {
		return nil
	} else {
		v := m.GetPrimary()
		return Cnd(m.Primary.ColumnName, Eq, v.Interface())
	}
}
