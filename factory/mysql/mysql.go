package gom

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/janyees/gom"
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
		if value != nil {
			if i > 0 {
				sql += ","
				values += ","
			}
			datas = append(datas, value)
			values += " ? "
			sql += v.ColumnName
		}

	}
	sql += ") VALUES (" + values + ")"
	return sql, datas
}
func (MySqlFactory) Replace(model gom.TableModel) (string, []interface{}) {
	var datas []interface{}
	ccs := model.Columns
	sql := "replace into " + "`" + model.TableName + "` ("
	values := ""
	for i, v := range ccs {
		value := model.ModelValue.FieldByName(v.FieldName).Interface()
		if value != nil {
			if i > 0 {
				sql += ","
				values += ","
			}
			datas = append(datas, value)
			values += " ? "
			sql += v.ColumnName
		}

	}
	sql += ") VALUES (" + values + ")"
	return sql, datas
}
func (MySqlFactory) Delete(model gom.TableModel) (string, []interface{}) {
	sql := "delete from " + "`" + model.TableName + "` "
	if model.Cnd != nil {
		sql += " where " + model.Cnd.State() + ";"
		return sql, model.Cnd.Value()
	} else if model.GetPrimaryCondition() != nil {
		sql += " where " + model.GetPrimaryCondition().State() + " ;"
		return sql, model.GetPrimaryCondition().Value()
	} else {
		return sql + ";", []interface{}{}
	}

}
func (MySqlFactory) Update(model gom.TableModel) (string, []interface{}) {
	var datas []interface{}
	sql := "update " + "`" + model.TableName + "` set "
	for i, v := range model.Columns {
		value := model.ModelValue.FieldByName(v.FieldName).Interface()
		if value != nil {
			if i > 0 {
				sql += ","
			}
			sql += v.ColumnName + " = ? "
			datas = append(datas, value)
		}
	}
	if model.Cnd != nil {
		sql += " where " + model.Cnd.State() + ";"
		datas = append(datas, model.Cnd.Value()...)
	} else if model.GetPrimaryCondition() != nil {
		sql += " where " + model.GetPrimaryCondition().State() + ";"
		datas = append(datas, model.GetPrimaryCondition().Value()...)
	} else {
		sql += ";"
	}
	return sql, datas
}
func (MySqlFactory) Query(model gom.TableModel) (string, []interface{}) {
	sql := "select "
	for i, v := range model.Columns {
		if i == 0 {
			sql += v.ColumnName
		} else {
			sql += "," + v.ColumnName
		}
	}
	sql += " from " + "`" + model.TableName + "`"
	if model.Cnd != nil {
		sql += " where " + model.Cnd.State() + ";"
		return sql, model.Cnd.Value()
	} else if model.GetPrimaryCondition() != nil {
		sql += " where " + model.GetPrimaryCondition().State() + ";"
		return sql, model.GetPrimaryCondition().Value()
	} else {
		return sql + ";", []interface{}{}
	}
}
