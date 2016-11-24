package gom

import (
	"reflect"
	"strings"
	"fmt"
)

type SqlFactory interface {
	Insert(TableModel) (string,[]interface{})
}
type TableModel struct {
	ModelType reflect.Type
	ModelValue reflect.Value
	TableName string
	Columns []Column
	Primary Column
}
type Column struct {
	ColumnType reflect.Type
	ColumnName string
	FieldName string
	Auto bool
}


func typeOf(v interface{}) reflect.Type {
	tt:=reflect.TypeOf(v)
	if(tt.Kind()==reflect.Ptr){
		return  tt.Elem()
	}else {
		return tt
	}
}
func getTableModule(v interface{}) *TableModel {
	tt:=reflect.TypeOf(v)
	var tps reflect.Type
	var vals reflect.Value
	fmt.Println("tt kind:",tt.Kind())
	if tt.Kind()==reflect.Ptr{
		tps=tt.Elem()
		vals=reflect.ValueOf(v).Elem()
	}else{
		tps=tt;
		vals=reflect.ValueOf(v)
	}
	fmt.Println("tests,,,,",vals)
	if vals.NumField()>0 && tps.NumMethod()>0{
		nameMethod:=vals.MethodByName("TableName")
		tableName:=nameMethod.Call(nil)[0].String()
		columns,primary:=getColumns(vals)
		return &TableModel{ModelType:tps,ModelValue:vals,Columns:columns,TableName:tableName,Primary:primary}
	}else{
		return &TableModel{}
	}
}
func getColumns(v reflect.Value) ([]Column,Column){
	var primary Column
	var columns []Column
	results := reflect.Indirect(reflect.ValueOf(&columns))
	oo:=v.Type()
	i:=0
	for;i<oo.NumField();i++{
		field:=oo.Field(i)
		tag,hasTag:=field.Tag.Lookup("gom")
		if hasTag && (!strings.Contains(tag,"-")&&!strings.Contains(tag,"ignore")){
			if strings.HasPrefix(tag,"primary")|| strings.HasPrefix(tag,"auto"){
				if len(primary.ColumnName)>0{
					panic("your struct '"+oo.Name()+"' has dumplicate primary key")
				}else{
					primary=generateColumnFromTag(tag,field)
				}
			}else if strings.HasPrefix(tag,"column"){
				column:=generateColumnFromTag(tag,field)
				n:=reflect.Indirect(reflect.ValueOf(&column))
				if(results.Kind()==reflect.Ptr){
					results.Set(reflect.Append(results,n.Addr()))
				}else{
					results.Set(reflect.Append(results,n))
				}
			}else{
				panic("wrong definations!!!")
			}
		}else{
			column:=Column{field.Type,strings.ToLower(field.Name),field.Name,false}
			n:=reflect.Indirect(reflect.ValueOf(&column))
			if(results.Kind()==reflect.Ptr){
				results.Set(reflect.Append(results,n.Addr()))
			}else{
				results.Set(reflect.Append(results,n))
			}
		}
	}
	return columns,primary
}
func generateColumnFromTag(tag string,filed reflect.StructField) Column{
	columnName:=getTagName(tag)
	isAtuo:=strings.Contains(tag,"auto")
	return Column{ColumnType:filed.Type,ColumnName:columnName,FieldName:filed.Name,Auto:isAtuo}
}
func getTagName(tag string) string {
	datas:= strings.Split(tag,",")
	if len(datas)==2{
		return datas[1]
	}else{
		panic("wrong defination!!!")
	}
}