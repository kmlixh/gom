package gom

import (
	"fmt"
	"reflect"
	"database/sql/driver"
)

type CreateSql func(TableModel) (string, []interface{})

type BinaryUnmarshaler interface {
	UnmarshalBinary(dbytes []byte) (interface{}, error)
}

type CustomScanner interface{
    Value() (driver.Value, error)
	Scan(src interface{}) error
}
type SqlGenerator struct {
	createSql   CreateSql
	tableModels []TableModel
}
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
	reflect.Type
	ColumnName string
	FieldName  string
	QueryField string
	IsPrimary  bool
	Auto       bool
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
	Items() []ConditionItem
	Values() []interface{}
	Pager() Pager
	Order() Order
	NotNull() bool
	Or(sql string, values ...interface{}) Condition
	And(sql string, values ...interface{}) Condition
	AndIn(name string, values ...interface{}) Condition
	OrIn(name string, values ...interface{}) Condition
	Page(index int, size int) Condition
	OrderBy(name string, tp OrderType) Condition
}
type Order interface {
	Name() string
	Type() OrderType
}
type Orders struct {
	MName string
	MType OrderType
}

func (o Orders) Name() string {
	return o.MName
}
func (o Orders) Type() OrderType {
	return o.MType
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

type Conditions struct {
	MItems []ConditionItem
	MOrder Order
	MPager Pager
}
type ConditionItem struct {
	LinkType LinkType
	States   string
	Values   []interface{}
}

func Cnd(sql string, values ...interface{}) Condition {
	if sql == "" {
		return &Conditions{}
	} else {
		return &Conditions{MItems: []ConditionItem{{LinkType: And, States: sql, Values: splitArrays(values)}}}
	}
}

func (c *Conditions) Items() []ConditionItem {
	return c.MItems
}
func (c *Conditions) Values() []interface{} {
	results := []interface{}{}
	length := len(c.MItems)
	if length > 0 {
		for i := 0; i < length; i++ {

			results = append(results, c.MItems[i].Values...)
		}
	}
	return results
}
func (c *Conditions) Pager() Pager {
	return c.MPager
}
func (c *Conditions) Order() Order {
	return c.MOrder
}
func (c *Conditions) NotNull() bool {
	return len(c.MItems) > 0 || c.MOrder != nil || c.MPager != nil
}
func (c *Conditions) And(sql string, values ...interface{}) Condition {
	c.MItems = append(c.MItems, ConditionItem{LinkType: And, States: sql, Values: splitArrays(values)})
	return c
}
func (c *Conditions) Or(sql string, values ...interface{}) Condition {
	c.MItems = append(c.MItems, ConditionItem{LinkType: Or, States: sql, Values: splitArrays(values)})
	return c
}
func (c *Conditions) AndIn(name string, values ...interface{}) Condition {
	if len(values) > 0 {
		sql, datas := makeInSql(name, values...)
		c.MItems = append(c.MItems, ConditionItem{LinkType: And, States: sql, Values: splitArrays(datas)})
	}
	return c
}

func (c *Conditions) OrIn(name string, values ...interface{}) Condition {
	if len(values) > 0 {
		sql, datas := makeInSql(name, values...)
		c.MItems = append(c.MItems, ConditionItem{LinkType: Or, States: sql, Values: splitArrays(datas)})
	}
	return c
}
func (c *Conditions) Page(index int, size int) Condition {
	c.MPager = Pagers{index, size}
	return c
}

func (c *Conditions) OrderBy(name string, tp OrderType) Condition {
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
	if IsEmpty(m.GetPrimary()) || m.Primary.IsPrimary == false {
		return nil
	} else {
		return &Conditions{MItems: []ConditionItem{{And, "`" + m.Primary.ColumnName + "` = ?", []interface{}{m.GetPrimary()}}}}
	}
}
