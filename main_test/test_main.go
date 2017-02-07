package main

import (
	"fmt"
	_ "github.com/janyees/gom/factory/mysql"
	"time"
	"encoding/json"
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
	b := []byte(`{"email":"lier171@qq.com","pwd":"38c16f4d16eb41aa83808ee965ab8a29","nicks":"test"}`)
	var f User
	err := json.Unmarshal(b, &f)
	fmt.Println(err,f)
}