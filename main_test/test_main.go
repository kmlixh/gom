package main

import (
	"fmt"
	_ "github.com/janyees/gom/factory/mysql"
	"time"
	"crypto/md5"
	"encoding/hex"
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
	md5Ctx := md5.New()
	md5Ctx.Write([]byte("test md5 encrypto"))
	cipherStr := md5Ctx.Sum(nil)
	fmt.Print(cipherStr)
	fmt.Print("\n")
	fmt.Print(hex.EncodeToString(cipherStr))
}