package mysql

import (
	"errors"
	"gitee.com/janyees/gom/register"
	"gitee.com/janyees/gom/structs"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type MyCndStruct struct {
	Linker     string
	Expression string
	Data       []interface{}
}

var funcMap map[structs.SqlType]structs.GenerateSQLFunc

type Factory struct {
}

func (m Factory) GetSqlFunc(sqlType structs.SqlType) structs.GenerateSQLFunc {
	return funcMap[sqlType]
}
func (m Factory) ConditionToSql(cnd structs.Condition) (string, []interface{}) {
	if cnd == nil {
		return "", nil
	}
	if !cnd.IsEnalbe() {
		return "", nil
	}
	myCnd := cndToMyCndStruct(cnd)
	var data []interface{}
	data = append(data, cnd.Values()...)
	var sql string
	if cnd.Depth() > 0 {
		sql += myCnd.Linker
	}

	if cnd.HasSubConditions() && cnd.Depth() > 0 {
		sql += " ("
	}
	sql += myCnd.Expression
	if cnd.HasSubConditions() {
		for _, v := range cnd.Items() {
			s, dd := m.ConditionToSql(v)
			sql += s
			data = append(data, dd...)
		}
	}

	if cnd.HasSubConditions() && cnd.Depth() > 0 {
		sql += ")"
	}

	return sql, data

}

func init() {
	m := Factory{}
	register.Register("mysql", &m)
	funcMap = make(map[structs.SqlType]structs.GenerateSQLFunc)
	funcMap[structs.Query] = func(models ...structs.TableModel) []structs.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "SELECT "
		counts := len(model.Columns)
		if counts == 0 {
			panic(errors.New("columns is null or empty"))
		}
		if counts > 1 {
			for i := 0; i < len(model.Columns); i++ {
				if i == 0 {
					sql += wrapperName(model.Columns[i]) + " "
				} else {
					sql += ", " + wrapperName(model.Columns[i]) + " "
				}
			}
		} else {
			sql += " " + wrapperName(model.Columns[0])
		}
		sql += " FROM " + model.Table + " "
		cnds, dds := m.ConditionToSql(model.Condition)
		if len(cnds) > 0 {
			sql += " WHERE " + cnds
		}
		datas = append(datas, dds...)
		if model.OrderBys != nil && len(model.OrderBys) > 0 {
			sql += " ORDER BY"
			for i := 0; i < len(model.OrderBys); i++ {
				if i > 0 {
					sql += ","
				}
				t := ""
				if model.OrderBys[i].Type() == structs.Asc {
					t = " ASC"
				} else {
					t = " DESC"
				}
				sql += " " + wrapperName(model.OrderBys[i].Name()) + t
			}
		}
		if model.Page != nil {
			idx, size := model.Page.Page()
			datas = append(datas, idx, size)
			sql += " LIMIT ?,?"
		}
		sql += ";"
		return []structs.SqlProto{{sql, datas}}
	}
	funcMap[structs.Update] = func(models ...structs.TableModel) []structs.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "UPDATE　"
		sql += " " + model.Table + " SET　"
		for i := 0; i < len(model.Columns); i++ {
			if i == 0 {
				sql += wrapperName(model.Columns[i]) + " = ? "
			} else {
				sql += ", " + wrapperName(model.Columns[i]) + " = ? "
			}
			datas = append(datas, model.Data[wrapperName(model.Columns[i])])
		}
		cnds, dds := m.ConditionToSql(model.Condition)
		if len(cnds) > 0 {
			sql += " WHERE " + cnds + ";"
		}
		datas = append(datas, dds)
		return []structs.SqlProto{{sql, datas}}
	}
	funcMap[structs.Insert] = func(models ...structs.TableModel) []structs.SqlProto {
		model := models[0]
		var datas []interface{}

		sql := "INSERT INTO " + model.Table + " ("
		valuesPattern := "VALUES("
		for i, c := range model.Columns {
			if i > 0 {
				sql += ","
				valuesPattern += ","
			}
			sql += c
			valuesPattern += "?"
			datas = append(datas, model.Data[c])
		}
		sql += ")"
		valuesPattern += ");"
		sql += valuesPattern
		return []structs.SqlProto{{sql, datas}}
	}
	funcMap[structs.Delete] = func(models ...structs.TableModel) []structs.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "DELETE　FROM　"
		sql += " " + model.Table
		cnds, dds := m.ConditionToSql(model.Condition)
		if len(cnds) > 0 {
			sql += " WHERE " + cnds + ";"
		}
		datas = append(datas, dds)
		return []structs.SqlProto{{sql, datas}}
	}
}

func wrapperName(name string) string {
	if strings.Contains(name, " ") {
		return name
	} else {
		return "`" + name + "`"
	}
}

func cndToMyCndStruct(cnd structs.Condition) MyCndStruct {
	if len(cnd.RawExpression()) > 0 {
		return MyCndStruct{linkerToString(cnd), cnd.RawExpression(), cnd.Values()}
	}
	opers := cnd.Field()
	switch cnd.Operation() {
	case structs.Eq:
		opers += " = ? "
	case structs.NotEq:
		opers += " <> ? "
	case structs.Ge:
		opers += " >= ? "
	case structs.Gt:
		opers += " > ? "
	case structs.Le:
		opers += " <= ? "
	case structs.Lt:
		opers += " < ? "
	case structs.In:
		opers += " IN " + valueSpace(len(cnd.Values()))
	case structs.NotIn:
		opers += " NOT IN " + valueSpace(len(cnd.Values()))
	case structs.Like:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case structs.LikeIgnoreStart:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string)
		cnd.SetValues(vals)
	case structs.LikeIgnoreEnd:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = vals[0].(string) + "%"
		cnd.SetValues(vals)
	case structs.IsNull:
		opers += " IS NULL "
	case structs.IsNotNull:
		opers += " IS NOT NULL "
	}
	return MyCndStruct{linkerToString(cnd), opers, cnd.Values()}
}

func linkerToString(cnd structs.Condition) string {
	switch cnd.Linker() {
	case structs.And:
		return " AND "
	case structs.Or:
		return " OR "
	default:
		return " AND "
	}
}

func valueSpace(count int) string {
	if count == 1 {
		return " ? "
	} else {
		str := "("
		for i := 0; i < count-1; i++ {
			str += "?,"
		}
		str += "?)"
		return str
	}
}
