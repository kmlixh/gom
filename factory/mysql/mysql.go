package mysql

import (
	"errors"
	"gitee.com/janyees/gom"
	_ "github.com/go-sql-driver/mysql"
)

var funcMap map[gom.SqlType]gom.GenerateSQLFunc

func init() {
	m := Factory{}
	gom.Register("mysql", &m)
	funcMap = make(map[gom.SqlType]gom.GenerateSQLFunc)
	funcMap[gom.Query] = func(models ...gom.TableModel) []gom.SqlProto {
		model := models[0]
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
		if model.GroupBys != nil && len(model.GroupBys) > 1 {
			sql += " GROUP BY "
			for i := 0; i < len(model.GroupBys); i++ {
				if i == 0 {
					sql += model.GroupBys[i] + " "
				} else {
					sql += ", " + model.GroupBys[i] + " "
				}
			}
		}
		if model.OrderBys != nil && len(model.OrderBys) > 0 {
			sql += " ORDER BY "
			for i := 0; i < len(model.OrderBys); i++ {
				if i > 0 {
					sql += ", "
				}
				t := ""
				if model.OrderBys[i].Type() == gom.Asc {
					t = " ASC "
				} else {
					t = " DESC "
				}
				sql += model.OrderBys[i].Name() + t + " "
			}
		}
		if model.Page != nil {
			idx, size := model.Page.Page()
			dds = append(dds, idx, size)
			sql += " LIMIT ?,? "
		}
		sql += " ;"
		return []gom.SqlProto{{sql, dds}}
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
	var data []interface{}
	data = append(data, cnd.Values())
	var sql string
	if len(cnd.RawExpression()) > 0 {
		sql = cnd.RawExpression()
	} else {
		sql += linkerToString(cnd)
		if cnd.HasSubConditions() {
			sql += "("
		}
		sql += cnd.Field() + operationToString(cnd)
		if cnd.HasSubConditions() {
			for _, v := range cnd.Items() {
				s, dd := m.ConditionToSql(v)
				sql += s
				data = append(data, dd)
			}
		}

		if cnd.HasSubConditions() {
			sql += ")"
		}

	}
	return sql, data

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
func operationToString(cnd gom.Condition) string {
	opers := ""
	switch cnd.Operation() {
	case gom.Eq:
		opers = " = " + valueSpace(len(cnd.Values()))
	case gom.NotEq:
		opers = " <> " + valueSpace(len(cnd.Values()))
	case gom.Ge:
		opers = " >= " + valueSpace(len(cnd.Values()))
	case gom.Gt:
		opers = " > " + valueSpace(len(cnd.Values()))
	case gom.Le:
		opers = " <= " + valueSpace(len(cnd.Values()))
	case gom.Lt:
		opers = " < " + valueSpace(len(cnd.Values()))
	case gom.In:
		opers = " IN " + valueSpace(len(cnd.Values()))
	case gom.NotIn:
		opers = " NOT IN " + valueSpace(len(cnd.Values()))
	case gom.Like:
		opers = " LIKE CONCAT('%',?,'%')"
	case gom.LikeIgnoreStart:
		opers = " LIKE CONCAT('%',?)"
	case gom.LikeIgnoreEnd:
		opers = " LIKE CONCAT(?,'%')"
	case gom.IsNull:
		opers = " IS NULL "
	case gom.IsNotNull:
		opers = " IS NOT NULL "
	}

	return opers
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
