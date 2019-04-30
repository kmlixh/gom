package gom

import (
	"fmt"
	"reflect"
	"strings"
)

type CreateSql func(TableModel, Condition) (string, []interface{})
type TransactionWork func(databaseTx *Db) (int, error)
type SqlFactory interface {
	Insert(TableModel, Condition) (string, []interface{})
	InsertIgnore(TableModel, Condition) (string, []interface{})
	Replace(TableModel, Condition) (string, []interface{})
	Update(TableModel, Condition) (string, []interface{})
	Delete(TableModel, Condition) (string, []interface{})
	Query(TableModel, Condition) (string, []interface{})
}
type RowChooser interface {
	Scan(dest ...interface{}) error
}

type TableModel struct {
	Type        reflect.Type
	Value       reflect.Value
	TableName   string
	ColumnNames []string
	Columns     map[string]Column
	Primary     Column
}

func (this TableModel) Clone(value reflect.Value, columnFilters ...string) TableModel {
	var names []string
	if len(columnFilters) > 0 {
		names = columnFilters
	} else {
		names = this.ColumnNames
	}
	return TableModel{this.Type, value, this.TableName, names, this.Columns, this.Primary}
}
func (model TableModel) ColumnsValues() []interface{} {
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

type OrderType int

const (
	_ OrderType = iota
	Asc
	Desc
)

type LinkType int

const (
	_ LinkType = iota
	And
	Or
)

type Condition interface {
	Items() []_ConditionItem
	Values() []interface{}
	Pager() Pager
	Order() Order
	NotNull() bool
	Or(sql string, values ...interface{}) Condition
	And(sql string, values ...interface{}) Condition
	AndIn(name string, values ...interface{}) Condition
	OrIn(name string, values ...interface{}) Condition
	Page(index int, size int) Condition
	Limit(index int, size int) Condition
	OrderBy(name string, tp OrderType) Condition
}
type Order interface {
	Name() string
	Type() OrderType
}
type Orders struct {
	MName string
	OrderType
}

func (o Orders) Name() string {
	return o.MName
}
func (o Orders) Type() OrderType {
	return o.OrderType
}

type Pager interface {
	Page() (int, int)
}

type Pagers struct {
	MIndex int
	MSize  int
}

func (p Pagers) Page() (int, int) {
	return p.MIndex, p.MSize
}

type _Condition struct {
	MItems []_ConditionItem
	MOrder Order
	MPager Pager
}
type _ConditionItem struct {
	LinkType LinkType
	States   string
	Values   []interface{}
}

func Cnd(sql string, values ...interface{}) Condition {
	if sql == "" {
		return &_Condition{}
	} else {
		return &_Condition{MItems: []_ConditionItem{{LinkType: And, States: sql, Values: splitArrays(values)}}}
	}
}

func (c *_Condition) Items() []_ConditionItem {
	return c.MItems
}
func (c *_Condition) Values() []interface{} {
	results := []interface{}{}
	length := len(c.MItems)
	if length > 0 {
		for i := 0; i < length; i++ {

			results = append(results, c.MItems[i].Values...)
		}
	}
	return results
}
func (c *_Condition) Pager() Pager {
	return c.MPager
}
func (c *_Condition) Order() Order {
	return c.MOrder
}
func (c *_Condition) NotNull() bool {
	return len(c.MItems) > 0 || c.MOrder != nil || c.MPager != nil
}
func (c *_Condition) And(sql string, values ...interface{}) Condition {
	c.MItems = append(c.MItems, _ConditionItem{LinkType: And, States: sql, Values: splitArrays(values)})
	return c
}
func (c *_Condition) Or(sql string, values ...interface{}) Condition {
	c.MItems = append(c.MItems, _ConditionItem{LinkType: Or, States: sql, Values: splitArrays(values)})
	return c
}
func (c *_Condition) AndIn(name string, values ...interface{}) Condition {
	if len(values) > 0 {
		sql, datas := makeInSql(name, values...)
		c.MItems = append(c.MItems, _ConditionItem{LinkType: And, States: sql, Values: splitArrays(datas)})
	}
	return c
}

func (c *_Condition) OrIn(name string, values ...interface{}) Condition {
	if len(values) > 0 {
		sql, datas := makeInSql(name, values...)
		c.MItems = append(c.MItems, _ConditionItem{LinkType: Or, States: sql, Values: splitArrays(datas)})
	}
	return c
}
func (c *_Condition) Page(index int, size int) Condition {
	c.MPager = Pagers{index * size, size}
	return c
}
func (c *_Condition) Limit(index int, size int) Condition {
	c.MPager = Pagers{index, size}
	return c
}

func (c *_Condition) OrderBy(name string, tp OrderType) Condition {
	c.MOrder = Orders{name, tp}
	return c
}
func splitArrays(values interface{}) []interface{} {
	var results []interface{}
	val := reflect.ValueOf(values)
	_, ptr, sli := getType(values)
	if ptr {
		val = val.Elem()
	}
	if sli {
		lens := val.Len()
		if lens > 0 {
			for i := 0; i < lens; i++ {
				datas := splitArrays(val.Index(i).Interface())
				results = append(results, datas...)
			}
		}
	} else {
		results = append(results, val.Interface())
	}
	return results

}

func makeInSql(name string, values ...interface{}) (string, []interface{}) {
	sql := name + " in ("
	var datas []interface{}
	if len(values) == 0 {
		return "", datas
	}
	datas = splitArrays(values)
	for i := 0; i < len(datas); i++ {
		if i == 0 {
			sql += " ? "
		} else {
			sql += ", ? "
		}
	}
	sql += ")"
	if debug {
		fmt.Println("make in sql was :", sql, datas)
	}
	return sql, datas
}

func (mo TableModel) InsertValues() []interface{} {
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
func (m TableModel) GetPrimary() interface{} {
	return m.Value.FieldByName(m.Primary.FieldName).Interface()
}
func (m TableModel) GetPrimaryCondition() Condition {
	if IsEmpty(m.GetPrimary()) || m.Primary.IsPrimary == false {
		return nil
	} else {
		return &_Condition{MItems: []_ConditionItem{{And, "`" + m.Primary.ColumnName + "` = ?", []interface{}{m.GetPrimary()}}}}
	}
}
