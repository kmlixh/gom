package mysql

import (
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kmlixh/gom/v2"
	"strings"
)

type MyCndStruct struct {
	Linker     string
	Expression string
	Data       []interface{}
}

var funcMap map[gom.SqlType]gom.GenerateSQLFunc

type Factory struct {
}

func (m Factory) GetSqlFunc(sqlType gom.SqlType) gom.GenerateSQLFunc {
	return funcMap[sqlType]
}
func (m Factory) ConditionToSql(preTag bool, cnd gom.Condition) (string, []interface{}) {
	if cnd == nil {
		return "", nil
	}
	myCnd := cndToMyCndStruct(cnd)

	var data []interface{}
	data = append(data, myCnd.Data...)
	sql := ""
	if preTag {
		sql += myCnd.Linker
	}
	if preTag && cnd.PayLoads() > 1 {
		sql += " ("
	}
	curTag := len(myCnd.Expression) > 0
	sql += myCnd.Expression

	if cnd.HasSubConditions() {
		for _, v := range cnd.Items() {
			if v.PayLoads() > 0 {
				s, dd := m.ConditionToSql(curTag || preTag, v)
				if len(s) > 0 {
					curTag = true
				}
				sql += s
				data = append(data, dd...)
			}
		}
	}

	if preTag && cnd.PayLoads() > 1 {
		sql += ")"
	}

	return sql, data

}

func init() {
	m := Factory{}
	gom.Register("mysql", &m)
	funcMap = make(map[gom.SqlType]gom.GenerateSQLFunc)
	funcMap[gom.Query] = func(models ...gom.TableModel) []gom.SqlProto {
		model := models[0]
		var datas []interface{}
		sql := "SELECT "
		counts := len(model.Columns())
		if counts == 0 {
			panic(errors.New("columns is null or empty"))
		} else {
			for i := 0; i < len(model.Columns()); i++ {
				if i == 0 {
					sql += wrapperName(model.Columns()[i]) + " "
				} else {
					sql += ", " + wrapperName(model.Columns()[i]) + " "
				}
			}
		}
		sql += " FROM " + model.Table() + " "
		cndString, cndData := m.ConditionToSql(false, model.Condition())
		if len(cndString) > 0 {
			sql += " WHERE " + cndString
		}
		datas = append(datas, cndData...)
		if len(model.OrderBys()) > 0 {
			sql += " ORDER BY"
			for i := 0; i < len(model.OrderBys()); i++ {
				if i > 0 {
					sql += ","
				}
				t := ""
				if model.OrderBys()[i].Type() == gom.Asc {
					t = " ASC"
				} else {
					t = " DESC"
				}
				sql += " " + wrapperName(model.OrderBys()[i].Name()) + t
			}
		}
		if model.Page() != nil {
			idx, size := model.Page().Page()
			datas = append(datas, idx, size)
			sql += " LIMIT ?,?"
		}
		sql += ";"
		var result []gom.SqlProto
		result = append(result, gom.SqlProto{PreparedSql: sql, Data: datas})
		return result
	}
	funcMap[gom.Update] = func(models ...gom.TableModel) []gom.SqlProto {
		if models == nil || len(models) == 0 {
			panic(errors.New("model was nil or empty"))
		}
		var result []gom.SqlProto
		for _, model := range models {
			if model.ColumnDataMap() == nil {
				panic(errors.New("nothing to update"))
			}
			var datas []interface{}
			sql := "UPDATE "
			sql += " " + model.Table() + " SET "
			i := 0
			for j, k := range model.Columns() {
				if j > 0 { //默认第一个是主键，需要去掉
					if i > 0 {
						sql += ", "
					}
					sql += wrapperName(k) + " = ? "
					datas = append(datas, model.ColumnDataMap()[k])
					i++
				}
			}
			conditionSql, dds := m.ConditionToSql(false, model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, gom.SqlProto{sql, datas})
		}

		return result
	}
	funcMap[gom.Insert] = func(models ...gom.TableModel) []gom.SqlProto {
		var result []gom.SqlProto
		for _, model := range models {
			var datas []interface{}

			sql := "INSERT INTO " + model.Table() + " ("
			valuesPattern := "VALUES("
			i := 0
			for j, c := range model.Columns() {
				if !model.PrimaryAuto() || j > 0 {
					if i > 0 {
						sql += ","
						valuesPattern += ","
					}
					sql += wrapperName(c)
					valuesPattern += "?"
					datas = append(datas, model.ColumnDataMap()[c])
					i++
				}
			}
			sql += ")"
			valuesPattern += ");"
			sql += valuesPattern
			result = append(result, gom.SqlProto{sql, datas})
		}
		return result
	}
	funcMap[gom.Delete] = func(models ...gom.TableModel) []gom.SqlProto {
		var result []gom.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "DELETE FROM "
			sql += " " + model.Table()
			conditionSql, dds := m.ConditionToSql(false, model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, gom.SqlProto{sql, datas})
		}
		return result
	}
}

func wrapperName(name string) string {
	if strings.IndexAny(name, " ") > 0 {
		return name
	} else {
		name = strings.TrimSpace(name)
		return "`" + name + "`"
	}
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
		return " ( ? ) "
	} else {
		str := "("
		for i := 0; i < count-1; i++ {
			str += "?,"
		}
		str += "?)"
		return str
	}
}
