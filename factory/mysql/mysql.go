package mysql

import "github.com/gom"
import _ "github.com/go-sql-driver/mysql"

func init()  {
	gom.Register("mysql",&MySqlFactory{})
}

type MySqlFactory struct {

}

func Insert(model gom.TableModel) (string,[]interface{}) {
	var datas []interface{}
	sql:="insert into \\`"+model.TableName+"\\` ("
	values:=""
	for i,v:=range model.Columns{
		if i>0{
			sql+=","
			values+=","
		}

		values+=" ? "
		sql+=v.ColumnName
	}
	sql+=") VALUES ("+values+")"


}
func Delete(model gom.TableModel) (string,[]interface{}) {

}
func Update(model gom.TableModel) (string,[]interface{}) {

}
func Query(model gom.TableModel) (string,[]interface{}) {

}
