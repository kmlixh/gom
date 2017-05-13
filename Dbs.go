package gom

import (
	"database/sql"
	"reflect"
	"fmt"
)

type DB struct {
	factory SqlFactory
	db      * sql.DB
}
type Executor struct {
	execute Execute
	tms []TableModel
}
type Execute func(TableModel)(string,[]interface{})

type ExecutorType int

const (
	_ ExecutorType =iota
	Insert
	Delete
	Update
)

func GetExecutor(executorType ExecutorType,tableModel []TableModel) Executor {
	var execute Execute
	switch executorType {
	case Insert:
		execute=DB.factory.Insert
	case Delete:
		execute=DB.factory.Delete
	case Update:
		execute=DB.factory.Update
	}
	return Executor{execute,tableModel}
}

func (DB DB) exec(executor Executor)(int,error){
	var results int
	for _,model:=range executor.tms{
		sqls,datas:=executor.execute(model)
		if debug {
			fmt.Println(sqls,datas)
		}
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
func (DB DB) execTransc(executors...Executor)(int,error){
	result:=0
	tx,err:=DB.db.Begin()
	if err!=nil{
		return result,err
	}
	for _,executor:=range executors{
		result,err=DB.exec(executor)
		if err!=nil {
			tx.Rollback()
			return 0,err
		}
	}
	tx.Commit()
	return result,nil;
}
func (DB DB) Insert(vs...interface{})(int,error){
	models:= getTableModels(vs...)
	return DB.exec(Executor{DB.factory.Insert, models})
}
func (DB DB) InsertInTransaction(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.execTransc(DB.factory.Insert,tables...)
}
func (DB DB) Delete(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.exec(Executor{DB.factory.Delete,tables})
}
func (DB DB) DeleteInTransaction(vs...interface{})(int,error){
	tables:= getTableModels(vs...)
	return DB.execTransc(DB.factory.Delete,tables...)
}
func (DB DB) DeleteByConditon(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.exec(Executor{DB.factory.Delete,tableModel})
}
func (DB DB) DeleteByConditonInTransaction(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.execTransc(DB.factory.Delete,tableModel)
}
func (DB DB) Update(vs...interface{})(int,error){
	tms := getTableModels(vs...)
	return DB.exec(Executor{DB.factory.Update, tms})
}
func (DB DB) UpdateInTransaction(vs...interface{})(int,error) {
	tables:= getTableModels(vs...)
	return DB.execTransc(DB.factory.Update,tables...)
}
func (DB DB) UpdateByCondition(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.exec(Executor{DB.factory.Update,tableModel})
}
func (DB DB) UpdateByConditionInTransaction(v interface{},c Condition)(int,error){
	tableModel:=getTableModule(v)
	tableModel.Cnd=c
	return DB.exec(Executor{DB.factory.Update,tableModel})
}

func (DB DB) Query(vs interface{},c Condition) interface{}{
	tps,isPtr,islice:= getType(vs)
	model:=getTableModule(vs)
	if debug{
		fmt.Println("model:",model)
	}
	if len(model.TableName)>0{
		model.Cnd=c;
		if islice{
			results := reflect.Indirect(reflect.ValueOf(vs))
			sqls,adds:=DB.factory.Query(model)
			if debug {
				fmt.Println(sqls,adds)
			}
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
			if debug {
				fmt.Println(sqls,adds)
			}
			row:=DB.db.QueryRow(sqls,adds...)
			if debug{
				fmt.Println("row is",row)
			}
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






