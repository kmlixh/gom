package gom

import (
	"fmt"
	"reflect"
)

type CreateSql func(TableModel) (string, []interface{})

type BinaryUnmarshaler interface {
	UnmarshalBinary(dbytes []byte) (interface{}, error)
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
	ColumnType reflect.Type
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

type linkType int

const (
	_ linkType = iota
	And
	Or
)

type Condition interface {
	State() string
	Value() []interface{}
	Or(sql string, values ...interface{}) Condition
	And(sql string, values ...interface{}) Condition
	AndIn(name string, values ...interface{}) Condition
	OrIn(name string, values ...interface{}) Condition
	Pager(index int, size int) Condition
	OrderBy(name string, tp OrderType) Condition
}
type Order interface {
	Order() string
}
type Orders struct {
	orderName string
	orderType OrderType
}

func (o Orders) Order() string {
	result := " ORDER BY `" + o.orderName + "` "
	if o.orderType == Asc {
		result += "ASC"
	} else {
		result += "DESC"
	}
	return result
}

type Pager interface {
	Pager() (int, int)
}
type Pagers struct {
	pageIndex int
	pageSize  int
}

func (p Pagers) Pager() (int, int) {
	return p.pageIndex * p.pageSize, p.pageSize
}

type Conditions struct {
	conditionItems []ConditionItem
	order          Order
	pager          Pager
}
type ConditionItem struct {
	linkType linkType
	states   string
	values   []interface{}
}

func Cnd(sql string, values ...interface{}) Condition {
	if sql == "" {
		return &Conditions{}
	} else {
		return &Conditions{conditionItems: []ConditionItem{{linkType: And, states: sql, values: splitArrays(values)}}}
	}
}
func (c *Conditions) State() string {
	results := ""
	length := len(c.conditionItems)
	if length > 0 {
		if debug {
			fmt.Println("ConditionItem=====", length, c.conditionItems)
		}
		for i := 0; i < length; i++ {
			if i == 0 {
				results += " WHERE "
			} else {
				if c.conditionItems[i].linkType == And {
					results += " AND "
				} else {
					results += " OR "
				}
			}
			results += c.conditionItems[i].states
		}
	}
	if c.order != nil {
		results += c.order.Order()
	}
	if c.pager != nil {
		results += " LIMIT ?,?;"
	}
	return results
}
func (c *Conditions) Value() []interface{} {
	results := []interface{}{}
	length := len(c.conditionItems)
	if length > 0 {
		for i := 0; i < length; i++ {

			results = append(results, c.conditionItems[i].values...)
		}
	}
	if c.pager != nil {
		page, index := c.pager.Pager()
		results = append(results, page, index)
	}
	return results
}
func (c *Conditions) And(sql string, values ...interface{}) Condition {
	c.conditionItems = append(c.conditionItems, ConditionItem{linkType: And, states: sql, values: splitArrays(values)})
	return c
}
func (c *Conditions) Or(sql string, values ...interface{}) Condition {
	c.conditionItems = append(c.conditionItems, ConditionItem{linkType: Or, states: sql, values: splitArrays(values)})
	return c
}
func (c *Conditions) AndIn(name string, values ...interface{}) Condition {
	if len(values) > 0 {
		sql, datas := makeInSql(name, values...)
		c.conditionItems = append(c.conditionItems, ConditionItem{linkType: And, states: sql, values: splitArrays(datas)})
	}
	return c
}

func (c *Conditions) OrIn(name string, values ...interface{}) Condition {
	if len(values) > 0 {
		sql, datas := makeInSql(name, values...)
		c.conditionItems = append(c.conditionItems, ConditionItem{linkType: Or, states: sql, values: splitArrays(datas)})
	}
	return c
}
func (c *Conditions) Pager(index int, size int) Condition {
	c.pager = Pagers{index, size}
	return c
}

func (c *Conditions) OrderBy(name string, tp OrderType) Condition {
	c.order = Orders{name, tp}
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
		return &Conditions{conditionItems: []ConditionItem{{And, "`" + m.Primary.ColumnName + "` = ?", []interface{}{m.GetPrimary()}}}}
	}
}
