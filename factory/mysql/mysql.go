package mysql

import (
	"errors"
	"gitee.com/janyees/gom"
	_ "github.com/go-sql-driver/mysql"
)

type MyCndStruct struct {
	Linker     string
	Expression string
	Data       []interface{}
}

var funcMap map[gom.SqlType]gom.GenerateSQLFunc

func init() {
	m := Factory{}
	gom.Register("mysql", &m)
	funcMap = make(map[gom.SqlType]gom.GenerateSQLFunc)
	funcMap[gom.Query] = func(models ...gom.TableModel) []gom.SqlProto {
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
					sql += "`" + model.Columns[i] + "` "
				} else {
					sql += ", " + "`" + model.Columns[i] + "` "
				}
			}
		} else {
			sql += " " + "`" + model.Columns[0] + "`"
		}
		sql += " FROM " + model.Table + " "
		cnds, dds := m.ConditionToSql(model.Condition)
		if len(cnds) > 0 {
			sql += " WHERE " + cnds
		}
		datas = append(datas, dds...)
		if model.GroupBys != nil && len(model.GroupBys) > 1 {
			sql += " GROUP BY "
			for i := 0; i < len(model.GroupBys); i++ {
				if i == 0 {
					sql += "`" + model.GroupBys[i] + "` "
				} else {
					sql += ",`" + model.GroupBys[i] + "`"
				}
			}
		}
		if model.OrderBys != nil && len(model.OrderBys) > 0 {
			sql += " ORDER BY"
			for i := 0; i < len(model.OrderBys); i++ {
				if i > 0 {
					sql += ","
				}
				t := ""
				if model.OrderBys[i].Type() == gom.Asc {
					t = " ASC"
				} else {
					t = " DESC"
				}
				sql += " `" + model.OrderBys[i].Name() + "`" + t
			}
		}
		if model.Page != nil {
			idx, size := model.Page.Page()
			datas = append(datas, idx, size)
			sql += " LIMIT ?,?"
		}
		sql += ";"
		return []gom.SqlProto{{sql, datas}}
	}
	funcMap[gom.Update] = func(models ...gom.TableModel) []gom.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "UPDATE　"
		sql += " " + model.Table + " SET　"
		for i := 0; i < len(model.Columns); i++ {
			if i == 0 {
				sql += model.Columns[i] + " = ? "
			} else {
				sql += ", " + model.Columns[i] + " = ? "
			}
			datas = append(datas, model.Data[model.Columns[i]])
		}
		cnds, dds := m.ConditionToSql(model.Condition)
		if len(cnds) > 0 {
			sql += " WHERE " + cnds + ";"
		}
		datas = append(datas, dds)
		return []gom.SqlProto{{sql, datas}}
	}
	funcMap[gom.Insert] = func(models ...gom.TableModel) []gom.SqlProto {
		model := models[0]
		var datas []interface{}

		sql := "INSERT　INTO " + model.Table + "("
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
		return []gom.SqlProto{{sql, datas}}
	}
	funcMap[gom.Delete] = func(models ...gom.TableModel) []gom.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "DELETE　FROM　"
		sql += " " + model.Table
		cnds, dds := m.ConditionToSql(model.Condition)
		if len(cnds) > 0 {
			sql += " WHERE " + cnds + ";"
		}
		datas = append(datas, dds)
		return []gom.SqlProto{{sql, datas}}
	}
}

type Factory struct {
}

func (m Factory) GetSqlFunc(sqlType gom.SqlType) gom.GenerateSQLFunc {
	return funcMap[sqlType]
}
func (m Factory) SupportPatch(sqlType gom.SqlType) bool {
	return false
}
func (m Factory) ConditionToSql(cnd gom.Condition) (string, []interface{}) {
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

func cndToMyCndStruct(cnd gom.Condition) MyCndStruct {
	if len(cnd.RawExpression()) > 0 {
		return MyCndStruct{linkerToString(cnd), cnd.RawExpression(), cnd.Values()}
	}
	opers := cnd.Field()
	switch cnd.Operation() {
	case gom.Eq:
		opers += " = ? "
	case gom.NotEq:
		opers += " <> ? "
	case gom.Ge:
		opers += " >= ? "
	case gom.Gt:
		opers += " > ? "
	case gom.Le:
		opers += " <= ? "
	case gom.Lt:
		opers += " < ? "
	case gom.In:
		opers += " IN " + valueSpace(len(cnd.Values()))
	case gom.NotIn:
		opers += " NOT IN " + valueSpace(len(cnd.Values()))
	case gom.Like:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case gom.LikeIgnoreStart:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string)
		cnd.SetValues(vals)
	case gom.LikeIgnoreEnd:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = vals[0].(string) + "%"
		cnd.SetValues(vals)
	case gom.IsNull:
		opers += " IS NULL "
	case gom.IsNotNull:
		opers += " IS NOT NULL "
	}
	return MyCndStruct{linkerToString(cnd), opers, cnd.Values()}
}

func linkerToString(cnd gom.Condition) string {
	switch cnd.Linker() {
	case gom.And:
		return " AND "
	case gom.Or:
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
