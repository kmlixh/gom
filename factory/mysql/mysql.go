package mysql

import (
	"errors"
	"gitee.com/janyees/gom/cnds"
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
func (m Factory) ConditionToSql(cnd cnds.Condition) (string, []interface{}) {
	if cnd == nil {
		return "", nil
	}
	if !cnd.Valid() {
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
		counts := len(model.Columns())
		if counts == 0 {
			panic(errors.New("columns is null or empty"))
		}
		if counts > 1 {
			for i := 0; i < len(model.Columns()); i++ {
				if i == 0 {
					sql += wrapperName(model.Columns()[i]) + " "
				} else {
					sql += ", " + wrapperName(model.Columns()[i]) + " "
				}
			}
		} else {
			sql += " " + wrapperName(model.Columns()[0])
		}
		sql += " FROM " + model.Table() + " "
		cndString, cndData := m.ConditionToSql(model.Condition())
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
				if model.OrderBys()[i].Type() == structs.Asc {
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
		var result []structs.SqlProto
		result = append(result, structs.SqlProto{PreparedSql: sql, Data: datas})
		return result
	}
	funcMap[structs.Update] = func(models ...structs.TableModel) []structs.SqlProto {
		if models == nil || len(models) == 0 {
			panic(errors.New("model was nil or empty"))
		}
		var result []structs.SqlProto
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
			conditionSql, dds := m.ConditionToSql(model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, structs.SqlProto{sql, datas})
		}

		return result
	}
	funcMap[structs.Insert] = func(models ...structs.TableModel) []structs.SqlProto {
		var result []structs.SqlProto
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
					sql += c
					valuesPattern += "?"
					datas = append(datas, model.ColumnDataMap()[c])
					i++
				}
			}
			sql += ")"
			valuesPattern += ");"
			sql += valuesPattern
			result = append(result, structs.SqlProto{sql, datas})
		}
		return result
	}
	funcMap[structs.Delete] = func(models ...structs.TableModel) []structs.SqlProto {
		var result []structs.SqlProto
		for _, model := range models {
			var datas []interface{}
			sql := "DELETE FROM "
			sql += " " + model.Table()
			conditionSql, dds := m.ConditionToSql(model.Condition())
			if len(conditionSql) > 0 {
				sql += " WHERE " + conditionSql + ";"
			}
			datas = append(datas, dds...)
			result = append(result, structs.SqlProto{sql, datas})
		}
		return result
	}
}

func wrapperName(name string) string {
	if strings.Contains(name, " ") {
		return name
	} else {
		return "`" + name + "`"
	}
}

func cndToMyCndStruct(cnd cnds.Condition) MyCndStruct {
	if len(cnd.RawExpression()) > 0 {
		return MyCndStruct{linkerToString(cnd), cnd.RawExpression(), cnd.Values()}
	}
	opers := cnd.Field()
	switch cnd.Operation() {
	case cnds.Eq:
		opers += " = ? "
	case cnds.NotEq:
		opers += " <> ? "
	case cnds.Ge:
		opers += " >= ? "
	case cnds.Gt:
		opers += " > ? "
	case cnds.Le:
		opers += " <= ? "
	case cnds.Lt:
		opers += " < ? "
	case cnds.In:
		opers += " IN " + valueSpace(len(cnd.Values()))
	case cnds.NotIn:
		opers += " NOT IN " + valueSpace(len(cnd.Values()))
	case cnds.Like:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string) + "%"
		cnd.SetValues(vals)
	case cnds.LikeIgnoreStart:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = "%" + vals[0].(string)
		cnd.SetValues(vals)
	case cnds.LikeIgnoreEnd:
		opers += " LIKE ? "
		vals := cnd.Values()
		vals[0] = vals[0].(string) + "%"
		cnd.SetValues(vals)
	case cnds.IsNull:
		opers += " IS NULL "
	case cnds.IsNotNull:
		opers += " IS NOT NULL "
	}
	return MyCndStruct{linkerToString(cnd), opers, cnd.Values()}
}

func linkerToString(cnd cnds.Condition) string {
	switch cnd.Linker() {
	case cnds.And:
		return " AND "
	case cnds.Or:
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
