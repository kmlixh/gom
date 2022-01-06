package test

import (
	"fmt"
	"gitee.com/janyees/gom"
	"testing"
	"time"
)
import _ "gitee.com/janyees/gom/factory/mysql"

var dsn = "remote:remote123@tcp(10.0.1.5)/test?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

//var dsn = "root:123456@tcp(192.168.32.187)/fochan?charset=utf8&loc=Asia%2FShanghai&parseTime=true"
var db *gom.DB

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
type TbRecord struct {
	Id         int64
	Age        int
	Height     int
	Width      int
	Length     int
	CreateDate time.Time
}

func init() {
	fmt.Println("init DB.............")
	temp, er := gom.Open("mysql", dsn, false)
	if er != nil {
		panic(er)
	}
	db = temp
}

type Log struct {
	Id    string `json:"id" gom:"!"`
	Level int    `gom:"level"`
	Info  string `gom:"info"`
	Test  string
	Date  time.Time `gom:"#"`
}
type User struct {
	Id        int       `json:"id" gom:"@,id"`
	SessionId string    `json:"session_id" gom:"-"`
	Pwd       string    `json:"pwd" gom:"pwd"`
	Email     string    `json:"email" gom:"email"`
	Valid     int       `json:"valid" gom:"valid"`
	NickName  string    `json:"nicks" gom:"nick_name"`
	RegDate   time.Time `json:"reg_date" gom:"reg_date"`
}

func (User) TableName() string {
	return "user"
}
func (Log) TableName() string {
	return "system_log"
}

func TestGetTableModel(t *testing.T) {
	var log []Log
	m1, err := gom.GetStructModel(&log)
	t.Log(m1, err)
}
func TestGetTableModelRepeat(t *testing.T) {
	var log []Log
	m1, err := gom.GetStructModel(&log)
	t.Log(m1, err)
	m2, err := gom.GetStructModel(&log)
	t.Log(m2, err)
}

type TestTable struct {
	Id  int `json:"id" gom:"@"`
	Kid int `json:"kid" gom:"#"`
	Vid int `json:"vid" gom:"#"`
}

func (TestTable) TableName() string {
	return "test_table"
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
func TestMultiOrders(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.OrderByAsc("id").OrderBy("nick_name", gom.Desc).OrderByDesc("create_date").Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) != 10 {
		t.Fail()
	}
	fmt.Println(users)
}
func TestRawCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where2("nick_name like ? ", "%淑兰%").Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Fail()
	}
	fmt.Println(users)
}
func TestCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where(gom.Cnd("nick_name", gom.LikeIgnoreStart, "淑兰")).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Fail()
	}
	fmt.Println(users)
}
func TestMultiCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where(gom.Cnd("nick_name", gom.LikeIgnoreStart, "淑兰").Or(gom.Cnd("phone_number", gom.Eq, "13663049871").Eq("nick_name", "吃素是福"))).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Fail()
	}
	fmt.Println(users)
}

func TestStructCondition(t *testing.T) {
	user := UserInfo{PhoneNumber: "13663049871", NickName: "吃素是福"}
	users := make([]UserInfo, 0)
	_, er := db.Where(gom.StructToCondition(user)).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Fail()
	}
	fmt.Println(users)
}

func TestDefaultStruct(t *testing.T) {
	logs := make([]TbRecord, 0)
	_, er := db.Select(&logs)
	if er != nil {
		panic(er)
	}
	if len(logs) == 0 {
		t.Fail()
	}
	fmt.Println(logs)
}

func TestRawQueryWithGroupBy(t *testing.T) {
	logs := make([]TbRecord, 0)
	_, er := db.Raw("select count(id) as id,sum(age) as age,sum(height) as height from tb_record group by create_date").Select(&logs)
	if er != nil {
		panic(er)
	}
	if len(logs) == 0 {
		t.Fail()
	}
	fmt.Println(logs)
}
