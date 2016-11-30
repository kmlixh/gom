package gom

import (
	"database/sql"
	"reflect"
)

type DB struct {
	factory SqlFactory
	db * sql.DB
}

func (DB DB) exec(funcs func(TableModel),ms...TableModel)(int,error){
	var results int
	for _,model:=range ms{
		result,err:=DB.db.Exec(funcs(model))
		if(err!=nil){
			return results,err
		}else{
			results+=result
		}
	}
	return results,nil
}
func (DB DB) execTransc(funcs func(TableModel),vs...interface{})(int,error){
	tx,err:=DB.db.Begin()
	if err!=nil{
		return 0,err
	}
	result,errs:=DB.exec(funcs,vs)
	if errs==nil {
		tx.Commit()
	}else{
		tx.Rollback()
	}
	return result,errs;
}
func (DB DB) Insert(vs...interface{})(int,error){
	return DB.exec(DB.factory.Insert,getTalbeModules(vs))
}
func (DB DB) InsertWithTransaction(vs...interface{})(int,error){
	return DB.execTransc(DB.factory.Insert,getTalbeModules(vs))
}
func (DB DB) Delete(vs...interface{})(int,error){
	return DB.exec(DB.factory.Delete,getTalbeModules(vs))
}
func (DB DB) DeleteWithTransaction(vs...interface{})(int,error){
	return DB.execTransc(DB.factory.Delete,getTalbeModules(vs))
}
func (DB DB) DeleteByConditon(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.exec(DB.factory.Delete,tableModel)
}
func (DB DB) DeleteByConditonWithTransaction(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.execTransc(DB.factory.Delete,tableModel)
}
func (DB DB) Update(vs...interface{})(int,error){
	return DB.exec(DB.factory.Update,getTalbeModules(vs))
}
func (DB DB) UpdateWithTransaction(vs...interface{})(int,error) {
	return DB.execTransc(DB.factory.Update,getTalbeModules(vs))
}
func (DB DB) UpdateByCondition(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.exec(DB.factory.Update,tableModel)
}
func (DB DB) UpdateByConditionWithTransaction(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.exec(DB.factory.Update,tableModel)
}
func (DB DB) Query(c Condition,vs interface{}) interface{}{
	typ,isPtr:=getTypeOf(vs)
	if typ.Kind()==reflect.Slice{

	}else if typ.Kind()==reflect.Struct{
		model:=getTableModule(vs)
		model.Cnd=c;
		row:=DB.db.QueryRow(DB.factory.Query(model))
		val:=getValueOfTableRow(model,row)
		vt:=reflect.ValueOf(vs)
		if(isPtr){
			vt.Elem().Set(val)
		}else{
			vt.Set(val)
		}
		return vt.Interface()
	}

}






