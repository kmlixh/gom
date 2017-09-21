package gom

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/janyees/gom"
	"strings"
)

func init() {
	gom.Register("mysql", &MySqlFactory{})
}

type MySqlFactory struct {
}

func (MySqlFactory) Insert(model gom.TableModel) (string, []interface{}) {
	var datas []interface{}
	ccs := model.Columns
	sql := "insert into " + "`" + model.TableName + "` ("
	values := ""
	for i, v := range ccs {
		value := model.ModelValue.FieldByName(v.FieldName).Interface()
		if (!v.Auto) && value != nil {
			if i > 1 {
				sql += ","
				values += ","
			}
			datas = append(datas, value)
			values += " ? "
			sql += "`" + v.ColumnName + "`"
		}

	}
	sql += ") VALUES (" + values + ")"
	return sql, datas
}
func (fac MySqlFactory) Replace(model gom.TableModel) (string, []interface{}) {
	sql, datas := fac.Insert(model)
	sql = strings.Replace(sql, "insert", "replace", 0)
	return sql, datas
}
func (MySqlFactory) Delete(model gom.TableModel) (string, []interface{}) {
	sql := "delete from " + "`" + model.TableName + "` "
	if model.Cnd != nil {
		sql += cnd(model.Cnd) + ";"
		return sql, model.Cnd.Values()
	} else if model.GetPrimaryCondition() != nil {
		sql += cnd(model.GetPrimaryCondition()) + " ;"
		return sql, model.GetPrimaryCondition().Values()
	} else {
		return sql, []interface{}{}
	}

}
func (MySqlFactory) Update(model gom.TableModel) (string, []interface{}) {
	var datas []interface{}
	sql := "update " + "`" + model.TableName + "` set "
	for i, v := range model.Columns {
		value := model.ModelValue.FieldByName(v.FieldName).Interface()
		if (!v.Auto) && value != nil {
			if i > 1 {
				sql += ","
			}
			sql += "`" + v.ColumnName + "` = ? "
			datas = append(datas, value)
		}
	}
	if model.Cnd != nil {
		sql += cnd(model.Cnd) + ";"
		datas = append(datas, model.Cnd.Values()...)
	} else if model.GetPrimaryCondition() != nil {
		sql += cnd(model.GetPrimaryCondition()) + ";"
		datas = append(datas, model.GetPrimaryCondition().Values()...)
	} else {
		sql += ";"
	}
	return sql, datas
}
func (MySqlFactory) Query(model gom.TableModel) (string, []interface{}) {
	sql := "select "
	for i, v := range model.Columns {
		if i > 0 {
			sql += ","
		}
		if v.QueryField == "" {
			sql += "`" + v.ColumnName + "`"
		} else {
			sql += v.QueryField
		}

	}
	sql += " from " + "`" + model.TableName + "`"
	if model.Cnd != nil {
		if model.Cnd.NotNull() {
			sql += cnd(model.Cnd) + ";"
		} else {
			sql += ";"
		}
		return sql, model.Cnd.Values()
	} else if model.GetPrimaryCondition() != nil {
		sql += cnd(model.GetPrimaryCondition()) + ";"
		return sql, model.GetPrimaryCondition().Values()
	} else {
		return sql, []interface{}{}
	}
}
func cnd(c gom.Condition) string {
	results := ""
	items := c.Items()
	length := len(items)
	if length > 0 {

		for i := 0; i < length; i++ {
			if i == 0 {
				results += " WHERE "
			} else {
				if items[i].LinkType == gom.And {
					results += " AND "
				} else {
					results += " OR "
				}
			}
			results += items[i].States
		}
	}
	if c.Order() != nil {
		results += " ORDER BY " + c.Order().Name()
		if c.Order().Type() == gom.Asc {
			results += " ASC "
		} else {
			results += " DESC "
		}
	}
	if c.Pager() != nil {
		results += " LIMIT ?,?;"
	}
	return results
}
