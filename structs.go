package gom

import (
	"fmt"
	"reflect"
)

type CreateSql func(TableModel) (string, []interface{})

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
	IsPrimary  bool
	Auto       bool
}
type Condition interface {
	State() string
	Value() []interface{}
}
type Conditions struct {
	states string
	values []interface{}
}

func Cnd(sql string, values ...interface{}) Conditions {
	return Conditions{sql, values}
}
func (c Conditions) State() string {
	return c.states
}
func (c Conditions) Value() []interface{} {
	return c.values
}
func (c Conditions) And(sql string, values ...interface{}) Conditions {
	if c.states != "" {
		c.states += " and "
	}
	c.states += sql
	c.values = append(c.values, values)
	return c
}
func (c Conditions) Or(sql string, values ...interface{}) Conditions {
	if c.states != "" {
		c.states += " or "
	}
	c.states += sql
	c.values = append(c.values, values)
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
func (c Conditions) AndIn(name string, values ...interface{}) Conditions {
	if len(values) > 0 {
		if c.states != "" {
			c.states += " and "
		}
		sql, datas := makeInSql(name, values...)
		c.states += sql
		c.values = append(c.values, datas...)
	}
	return c
}

func (c Conditions) OrIn(name string, values ...interface{}) Conditions {
	if len(values) > 0 {
		if c.states != "" {
			c.states += " or "
		}
		sql, datas := makeInSql(name, values...)
		c.states += sql
		c.values = append(c.values, datas...)
	}
	return c
}
func (c Conditions) Sql(sql string, values ...interface{}) Conditions {
	c.states += sql
	c.values = append(c.values, values)
	return c
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
		return Conditions{"`" + m.Primary.ColumnName + "` = ?", []interface{}{m.GetPrimary()}}
	}
}
