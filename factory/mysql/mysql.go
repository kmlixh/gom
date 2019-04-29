package mysql

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/janyees/gom"
	"strings"
)

func init() {
	gom.Register("mysql", &MySqlFactory{})
}
func MysqlRegister() {
	gom.Register("mysql", &MySqlFactory{})
}

type MySqlFactory struct {
}

func (MySqlFactory) Insert(model gom.TableModel, c gom.Condition) (string, []interface{}) {
	var datas []interface{}
	sql := "INSERT INTO " + "`" + model.TableName + "` ("
	values := ""
	for _, name := range model.ColumnNames {
		v := model.Columns[name]
		value := model.Value.FieldByName(v.FieldName).Interface()
		if (!v.Auto) && value != nil {

			if len(datas) > 0 {
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
func (self MySqlFactory) InsertIgnore(model gom.TableModel, c gom.Condition) (string, []interface{}) {
	sql, datas := self.Insert(model, c)
	sql = strings.Replace(sql, "INSERT INTO ", "INSERT IGNORE INTO ", 1)
	return sql, datas
}
func (fac MySqlFactory) Replace(model gom.TableModel, c gom.Condition) (string, []interface{}) {
	sql, datas := fac.Insert(model, c)
	sql = strings.Replace(sql, "INSERT", "REPLACE", 1)
	return sql, datas
}
func (MySqlFactory) Delete(model gom.TableModel, cnd gom.Condition) (string, []interface{}) {
	sql := "DELETE FROM " + "`" + model.TableName + "` "
	if cnd != nil {
		sql += cndSql(cnd)
		return sql, cndValue(cnd)
	} else if model.GetPrimaryCondition() != nil {
		sql += cndSql(model.GetPrimaryCondition())
		return sql, model.GetPrimaryCondition().Values()
	} else {
		return sql, []interface{}{}
	}

}
func (MySqlFactory) Update(model gom.TableModel, cnd gom.Condition) (string, []interface{}) {
	var datas []interface{}
	sql := "UPDATE " + "`" + model.TableName + "` SET "
	for _, name := range model.ColumnNames {
		v := model.Columns[name]
		value := model.Value.FieldByName(v.FieldName).Interface()
		if (!v.Auto) && value != nil {
			if len(datas) > 0 {
				sql += ","
			}
			sql += "`" + v.ColumnName + "` = ? "
			datas = append(datas, value)
		}
	}
	if cnd != nil {
		sql += cndSql(cnd)
		datas = append(datas, cndValue(cnd)...)
	} else if model.GetPrimaryCondition() != nil {
		sql += cndSql(model.GetPrimaryCondition())
		datas = append(datas, model.GetPrimaryCondition().Values()...)
	} else {
		sql += ";"
	}
	return sql, datas
}
func (MySqlFactory) Query(model gom.TableModel, cnd gom.Condition) (string, []interface{}) {
	sql := "SELECT "
	i := 0
	for _, name := range model.ColumnNames {
		v := model.Columns[name]
		if i > 0 {
			sql += ","
		}
		if v.QueryField == "" {
			sql += "`" + name + "`"
		} else {
			sql += v.QueryField
		}
		i++

	}
	sql += " FROM " + "`" + model.TableName + "`"
	if cnd != nil {
		if cnd.NotNull() {
			sql += cndSql(cnd)
		} else {
			sql += ";"
		}
		return sql, cndValue(cnd)
	} else if model.GetPrimaryCondition() != nil {
		sql += cndSql(model.GetPrimaryCondition())
		return sql, model.GetPrimaryCondition().Values()
	} else {
		return sql, []interface{}{}
	}
}
func cndValue(cnd gom.Condition) []interface{} {
	values := cnd.Values()
	if cnd.Pager() != nil {
		index, size := cnd.Pager().Page()
		if index >= 0 {
			values = append(values, index)
		}
		values = append(values, size)
	}
	return values
}
func cndSql(c gom.Condition) string {
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
		results += " ORDER BY `" + c.Order().Name() + "`"
		if c.Order().Type() == gom.Asc {
			results += " ASC "
		} else {
			results += " DESC "
		}
	}
	if c.Pager() != nil {
		index, _ := c.Pager().Page()
		if index >= 0 {
			results += " LIMIT ?,?;"
		} else {
			results += " LIMIT ?;"
		}
	}
	return results
}
