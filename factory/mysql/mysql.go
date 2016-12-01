package gom

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/janyees/gom"
	"fmt"
)

func init()  {
	gom.Register("mysql",&MySqlFactory{})
}

type MySqlFactory struct {

}

func (MySqlFactory) Insert(model gom.TableModel) (string,[]interface{}) {
	var datas []interface{}
	sql:="insert into "+"`"+model.TableName+"` ("
	values:=""
	for i,v:=range model.Columns{
		value:=model.ModelValue.FieldByName(v.FieldName).Interface()
		if value !=nil{
			if i>0{
				sql+=","
				values+=","
			}
			datas=append(datas,value)
			values+=" ? "
			sql+=v.ColumnName
		}

	}
	sql+=") VALUES ("+values+")"
	return sql,datas
}
func (MySqlFactory)Delete(model gom.TableModel) (string,[]interface{}) {
	sql:="delete from "+"`"+model.TableName+"` where "
	if model.Cnd != nil{
		sql+=model.Cnd.State()+";"
		return sql,model.Cnd.Value()
	}else{
		sql+=model.GetPrimaryCondition().State()+";"
		return sql,model.GetPrimaryCondition().Value()
	}

}
func (MySqlFactory)Update(model gom.TableModel) (string,[]interface{}) {
	var datas []interface{}
	sql:="update "+"`"+model.TableName+"` set "
	for i,v:=range model.Columns{
		value:=model.ModelValue.FieldByName(v.FieldName).Interface()
		fmt.Println("single value:",value)
		if value !=nil{
			if i>0{
				sql+=","
			}
			sql+=v.ColumnName+" = ? "
			datas=append(datas,value)
		}
	}
	if model.Cnd!=nil{
		sql+=" "+model.Cnd.State()+";"
		datas=append(datas,model.Cnd.Value()...)
	}else{
		sql+=" "+model.GetPrimaryCondition().State()+";"
		datas=append(datas,model.GetPrimaryCondition().Value()...)
	}
	fmt.Println(sql,datas)
	return sql,datas
}
func (MySqlFactory)Query(model gom.TableModel) (string,[]interface{}) {
	sql:="select "
	sql+=model.Primary.ColumnName
	for _,v:=range model.Columns{
		sql+=","+v.ColumnName
	}
	sql+=" from "+"`"+model.TableName+"` "
	if model.Cnd!=nil{
		sql+=model.Cnd.State()+";"
		return sql,model.Cnd.Value()
	}else{
		return sql,nil
	}
}
