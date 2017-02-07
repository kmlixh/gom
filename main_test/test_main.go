package main

import (
	"fmt"
	_ "github.com/janyees/gom/factory/mysql"
	"time"
	"reflect"
)
type Log struct {
	Id string `json:"id" gom:"!"`
	Level int `gom:"level"`
	Info string `gom:"info"`
	Test string
	Date time.Time `gom:"#"`
}
type User struct {
	Id int `json:"id" gom:"@,id"`
	SessionId string `json:"session_id" gom:"-"`
	Pwd string `json:"pwd" gom:"pwd"`
	Email string `json:"email" gom:"email"`
	Valid int `json:"valid" gom:"valid"`
	NickName string `json:"nicks" gom:"nick_name"`
	RegDate time.Time `json:"reg_date" gom:"reg_date"`
}


func (User) TableName() string {
	return "user"
}
func (Log) TableName() string {
	return "system_log"
}

func main() {
	var users []User
	tt:=reflect.TypeOf(&users)
	ptrs:=false
	islice:=false
	if(tt.Kind()==reflect.Ptr){
		tt=tt.Elem()
		ptrs=true
	}
	if(tt.Kind()==reflect.Slice||tt.Kind()==reflect.Array){
		tt=tt.Elem()
		islice=true
	}
	fmt.Println(tt,ptrs,islice,tt.NumField())

	vals:=reflect.Indirect(reflect.ValueOf(tt).Elem())
	fmt.Println(vals.NumField())
}