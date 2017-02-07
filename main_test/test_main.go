package main

import (
	"fmt"
	_ "github.com/janyees/gom/factory/mysql"
	"github.com/janyees/gom"
	"time"
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
	var user User
	dsn:=`root:Nuatar171Yzy@tcp(120.25.254.189:3306)/reurl`
	db,err:=gom.Open("mysql",dsn,true)
	if err!=nil{
		fmt.Println(err)
	}
	db.Query(&user,gom.Cnds("email=?","lier171@qq.com"))
	fmt.Println(user)
}