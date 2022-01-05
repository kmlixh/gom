package test

import (
	"fmt"
	"gitee.com/janyees/gom"
	"testing"
	"time"
)
import _ "gitee.com/janyees/gom/factory/mysql"

var dsn = "remote:remote123@tcp(10.0.1.5)/test?charset=utf8&loc=Asia%2FShanghai&parseTime=true"
var db = &gom.DB{}

type UserInfo struct {
	Id          int64     `json:"id" gom:"@"`
	PhoneNumber string    `json:"phone_number" gom:"phone_number"`
	Unionid     string    `json:"unionid" gom:"unionid"`
	NickName    string    `json:"nick_name" gom:"#,nick_name"`
	HeadSrc     string    `json:"head_src" gom:"head_src"`
	Sex         int       `json:"sex" gom:"sex"`
	Score       int64     `json:"score" gom:"-"`
	DonateTag   int64     `json:"donate_tag" gom:"donate_tag"`
	Title       string    `json:"title" gom:"-"`
	CheckIn     bool      `json:"check_in" gom:"-"`
	CreateDate  time.Time `json:"create_date" gom:"create_date"`
}

func init() {
	temp, er := gom.Open("mysql", dsn, false)
	if er != nil {
		panic(er)
	}
	db = temp
	fmt.Println(db)
}

func (UserInfo) TableName() string {
	return "user_info"
}

func TestRawSelect(t *testing.T) {
	users := make([]UserInfo, 0)
	_, ser := db.Raw("select * from user_info limit ?,?", 0, 1000).Select(&users)
	if ser != nil {
		panic(ser)
	}
	fmt.Println(len(users))
}
func TestDefaultTableQuery(t *testing.T) {
	users := make([]UserInfo, 0)
	_, ser := db.Select(&users)
	if ser != nil {
		panic(ser)
	}
	fmt.Println(len(users))
}
func TestDefaultTableQueryLimit(t *testing.T) {
	users := make([]UserInfo, 0)
	_, ser := db.Page(0, 1000).Select(&users)
	if ser != nil {
		panic(ser)
	}
	if len(users) != 1000 {
		t.Fail()
	}
}
func TestCustomTableName(t *testing.T) {
	users := make([]UserInfo, 0)
	_, ser := db.Table("user_info2").Page(0, 1000).Select(&users)
	if ser != nil {
		panic(ser)
	}
	if len(users) != 1000 {
		t.Fail()
	}
	fmt.Println(len(users))
}
func TestOrderByDesc(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.OrderByDesc("id").Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) != 10 {
		t.Fail()
	}
	fmt.Println(users)
}
func TestOrderByAsc(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.OrderByAsc("id").Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) != 10 {
		t.Fail()
	}
	fmt.Println(users)
}
