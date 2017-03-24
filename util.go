package gom

import (
	"strings"
	"reflect"
	"time"
	"fmt"
)

func Cnds(sql string,vs...interface{}) Condition  {
	return Conditions{sql,vs}
}
func IsEmpty(v interface{}) bool{
	times:=time.Time{};
	if  times==v{
		return true
	}
	if v==""{
		return true
	}
	if v==0{
		return true
	}
	if v==0.0{
		return true
	}
	if v==nil{
		return true
	}
	return false
}
func getType(v interface{}) (reflect.Type,bool,bool) {
	tt:=reflect.TypeOf(v)
	isPtr :=false
	islice:=false
	if(tt.Kind()==reflect.Ptr){
		tt=tt.Elem()
		isPtr =true
	}
	if(tt.Kind()==reflect.Slice||tt.Kind()==reflect.Array){
		tt=tt.Elem()
		islice=true
	}
	if DB.debug{
		fmt.Println("Test getType, result:",tt,isPtr,islice)
	}
	return tt, isPtr,islice
}
func getTableModels(vs...interface{}) []TableModel{
	tablemodels:=[]TableModel{}
	for _,v:=range vs{
		tablemodels=append(tablemodels,getTableModule(v))
	}
	return tablemodels
}
func getTableModule(v interface{}) TableModel {
	if v!=nil && reflect.TypeOf(v).Kind()!=reflect.Interface{
		tt,_,_:= getType(v)
		vals:=reflect.New(tt).Elem()
		if tt.NumField()>0 && tt.NumMethod()>0{
			nameMethod:=vals.MethodByName("TableName")
			tableName:=nameMethod.Call(nil)[0].String()
			columns,primary:=getColumns(vals)
			return TableModel{ModelType:tt,ModelValue:vals,Columns:columns,TableName:tableName,Primary:primary}
		}else{
			return TableModel{}
		}
	}else{
		return TableModel{}
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
		col,tps:=getColumnFromField(field)
		if tps!=-1{
			if tps==1 || tps==2{
				primary=col
			}else{
				n:=reflect.Indirect(reflect.ValueOf(&col))
				if(results.Kind()==reflect.Ptr){
					results.Set(reflect.Append(results,n.Addr()))
				}else{
					results.Set(reflect.Append(results,n))
				}
			}
		}
	}
	if DB.debug{
		fmt.Println("columns is:",columns,primary)
	}
	return columns,primary
}
func getColumnFromField(filed reflect.StructField) (Column,int) {
	tag,tps:=getTagFromField(filed)
	if DB.debug{
		fmt.Println("Tag is:",tag,"type is:",tps);
	}
	if tps!=-1{
		return Column{ColumnType:filed.Type,ColumnName:tag,FieldName:filed.Name,Auto:tps==2},tps
	}else{
		return Column{},-1
	}

}
func getTagFromField(field reflect.StructField) (string,int) {
	tag,hasTag:=field.Tag.Lookup("gom")
	if hasTag{
		if strings.EqualFold(tag,"-")||len(tag)==0{
			return "",-1
		}else if len(tag)==1{
			tps:=0
			if strings.EqualFold(tag,"@"){
				tps=2
			}
			if strings.EqualFold(tag,"!"){
				tps=1
			}
			return strings.ToLower(field.Name),tps
		}else{
			if strings.Contains(tag,","){
				tags:=strings.Split(tag,",")
				if len(tags)==2{
					if strings.EqualFold(tags[0],"!") || strings.EqualFold(tags[0],"primary"){
						return tags[1],1
					} else if strings.EqualFold(tags[0],"@") || strings.EqualFold(tags[0],"auto"){
						return tags[1],2
					}else if strings.EqualFold(tags[0],"#") || strings.EqualFold(tags[0],"column"){
						return tags[1],0
					}else{
						return "",-1
					}
				}else{
					return "",-1
				}
			}else{
				return tag,0
			}
		}
	}else{
		return "",-1
	}
}
func getValueOfTableRow(model TableModel,row RowChooser) reflect.Value{
	maps:=getBytesMap(model,row)
	ccs:=[]Column{model.Primary}
	ccs=append(ccs,model.Columns...)
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
		case reflect.Int:
			dds,_=IntfromString(data)
		case reflect.Int8:
			dds,_=Int8fromString(data)
		case reflect.Int16:
			dds,_=Int16fromString(data)
		case reflect.Int32:
			dds,_=Int32fromString(data)
		case reflect.Int64:
			dds,_=Int64fromString(data)
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
		vv.Elem().FieldByName(c.FieldName).Set(reflect.ValueOf(dds))
	}
	return vv;
}
func getBytesMap(model TableModel,row RowChooser) map[string][]byte{

	data:=make([][]byte,len(model.Columns)+1)
	dest := make([]interface{}, len(model.Columns)+1) // A temporary interface{} slice
	for i,_ := range data {
		dest[i] = &data[i] // Put pointers to each string in the interface slice
	}
	err:=row.Scan(dest...)
	if err!=nil{
		return map[string][]byte{}
	}
	result:=make(map[string][]byte,len(model.Columns)+1)
	ccs:=[]Column{model.Primary}
	ccs=append(ccs,model.Columns...)
	for i,dd:=range ccs{
		result[dd.ColumnName]=data[i]
	}
	return result;

}
