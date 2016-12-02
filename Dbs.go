package gom

import (
	"database/sql"
	"reflect"
)

type DB struct {
	factory SqlFactory
	db * sql.DB
}

func (DB DB) exec(funcs func(TableModel)(string,[]interface{}),ms...TableModel)(int,error){
	var results int
	for _,model:=range ms{
		sqls,datas:=funcs(model)
		result,err:=DB.db.Exec(sqls,datas...)
		if(err!=nil){
			return results,err
		}else{
			rows,_:=result.RowsAffected()
			results+=int(rows)
		}
	}
	return results,nil
}
func (DB DB) execTransc(funcs func(TableModel)(string,[]interface{}),vs...TableModel)(int,error){
	tx,err:=DB.db.Begin()
	if err!=nil{
		return 0,err
	}
	result,errs:=DB.exec(funcs,vs...)
	if errs==nil {
		tx.Commit()
	}else{
		tx.Rollback()
	}
	return result,errs;
}
func (DB DB) Insert(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.exec(DB.factory.Insert,tables...)
}
func (DB DB) InsertWithTransaction(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.execTransc(DB.factory.Insert,tables...)
}
func (DB DB) Delete(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.exec(DB.factory.Delete,tables...)
}
func (DB DB) DeleteWithTransaction(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.execTransc(DB.factory.Delete,tables...)
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
	tables:= getTableModels(vs...)
	return DB.exec(DB.factory.Update,tables...)
}
func (DB DB) UpdateWithTransaction(vs...interface{})(int,error) {
	tables:= getTableModels(vs...)
	return DB.execTransc(DB.factory.Update,tables...)
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
func (DB DB) Query(vs interface{},c Condition) interface{}{
	tps,isPtr,islice:=getTypeOf(vs)
	model:=getTableModule(vs)
	if len(model.TableName)>0{
		model.Cnd=c;
		if islice{
			results := reflect.Indirect(reflect.ValueOf(vs))
			sqls,adds:=DB.factory.Query(model)
			rows,err:=DB.db.Query(sqls,adds...)
			if err!=nil{
				return nil
			}
			defer rows.Close()
			for rows.Next() {
				val:=getValueOfTableRow(model,rows)
				if isPtr {
					results.Set(reflect.Append(results,val.Elem()))
				}else{
					results.Set(reflect.Append(results,val))
				}
			}
			return vs

		}else {
			sqls,adds:=DB.factory.Query(model)
			row:=DB.db.QueryRow(sqls,adds...)
			val:=getValueOfTableRow(model,row)
			var vt reflect.Value
			if(isPtr){
				vt=reflect.ValueOf(vs).Elem()
			}else{
				vt=reflect.New(tps).Elem()

			}
			vt.Set(val.Elem())
			return vt.Interface()
		}

	}else{
		return nil
	}
}






