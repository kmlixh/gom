package gom

import "database/sql"

type DB struct {
	factory SqlFactory
	db * sql.DB
}

func (DB DB)Insert(v interface{}) (int,error)  {
	model:=getTableModule(v)
	result,err:=DB.db.Exec(DB.factory.Insert(model))
	if(err!=nil){
		return 0,err
	}else{
		return result.RowsAffected(),nil
	}
}

func (DB DB) Insert(...interface{})(int,error){

}
func (DB DB) Delete(v interface{},c Condition)  {

}
func (DB DB) Delete(vs...interface{}){

}
func (DB DB) Update(v interface{}){

}
func (DB DB) Update(vs ...interface{}){

}
func (DB DB) Query(v interface{}){

}
func (DB DB) Query(vs ...interface{}){

}
