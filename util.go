package gom

import (
	"strings"
	"reflect"
	"fmt"
	"time"
	"database/sql"
)

func getTypeOf(v interface{}) (reflect.Type,bool) {
	tt:=reflect.TypeOf(v)
	if(tt.Kind()==reflect.Ptr){
		return  tt.Elem(),true
	}else {
		return tt,false
	}
}
func getTalbeModules(vs...interface{}) *[]TableModel{
	tablemodels:=make([]TableModel,len(vs))
	for i,v:=range vs{
		tablemodels[i]=getTableModule(v)
	}
	return tablemodels
}
func getTableModule(v interface{}) *TableModel {
	tt:=reflect.TypeOf(v)
	var tps reflect.Type
	var vals reflect.Value
	if tt.Kind()==reflect.Ptr{
		tps=tt.Elem()
		vals=reflect.ValueOf(v).Elem()
	}else{
		tps=tt;
		vals=reflect.ValueOf(v)
	}
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
func getValueOfTableRow(model TableModel,row sql.Row) reflect.Value{
	maps:=getBytesMap(model,row)
	ccs:=[]Column{model.Primary}
	append(ccs,model.Columns)
	vv:=reflect.New(model.ModelType)
	for _,c:=range ccs{
		var dds interface{}
		dbytes:=maps[c.ColumnName]
		data:=string(dbytes)
		switch c.ColumnType.Kind() {
		case reflect.Uint:
			dds,_=UIntfromString(data)
		case reflect.Uint16:
			dds,_=UInt16fromString(data)
		case reflect.Uint32:
			dds,_=UInt32fromString(data)
		case reflect.Uint64:
			dds,_=UInt64fromString(data)
		case reflect.Float32:
			dds,_=Float32fromString(data)
		case reflect.Float64:
			dds,_=Float64fromString(data)
		case reflect.String:
			dds=data
		case reflect.TypeOf([]byte{}).Kind():
			dds=dbytes
		case reflect.TypeOf(time.Time{}).Kind():
			dds,_=TimeFromString(data)
		default:
			dds=data
		}
		vv.FieldByName(c.ColumnName).Set(reflect.ValueOf(dds))
	}
	return vv;
}
func getBytesMap(model TableModel,row sql.Row) map[string][]byte{
	data:=make(sql.RawBytes,len(model.Columns)+1)
	dest := make([]interface{}, len(model.Columns)+1) // A temporary interface{} slice
	for i,_ := range data {
		dest[i] = &data[i] // Put pointers to each string in the interface slice
	}
	row.Scan(dest...)
	result:=make(map[string][]byte,len(model.Columns)+1)
	ccs:=[]Column{model.Primary}
	append(ccs,model.Columns)
	for i,dd:=range ccs{
		result[dd.ColumnName]=data[i+1]
	}
	return result;

}
