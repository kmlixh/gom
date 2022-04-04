package gom

import (
	"fmt"
	_ "gitee.com/janyees/gom/factory/mysql"
	"gitee.com/janyees/gom/structs"
	"testing"
	"time"
)

var dsn = "remote:remote123@tcp(10.0.1.5)/test?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

//var dsn = "remote:Remote171Yzy@tcp(13.236.1.51:3306)/user_centre?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

var db *DB

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
	temp, er := Open("mysql", dsn, false)
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
	Id       int64     `json:"id" gom:"@,id"`
	Pwd      string    `json:"pwd" gom:"pwd"`
	Email    string    `json:"email" gom:"email"`
	Valid    int       `json:"valid" gom:"valid"`
	NickName string    `json:"nicks" gom:"nick_name"`
	RegDate  time.Time `json:"reg_date" gom:"reg_date"`
}

func (User) TableName() string {
	return "user"
}
func (Log) TableName() string {
	return "system_log"
}

func TestGetTableModel(t *testing.T) {
	var log []Log
	_, err := structs.GetStructModel(&log)
	if err != nil {
		t.Error(err)
	}
}

func (UserInfo) TableName() string {
	return "user_info"
}

func TestDefaultTableQuery(t *testing.T) {
	var users []User
	_, ser := db.Select(&users)
	if ser != nil {
		panic(ser)
	}
}
func TestDefaultTableQueryLimit(t *testing.T) {
	users := make([]UserInfo, 0)
	_, ser := db.Page(0, 1000).Select(&users)
	if ser != nil {
		panic(ser)
	}
	if len(users) != 1000 {
		t.Error("counts :", len(users), db)
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
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}

func TestMultiOrders(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.OrderByAsc("id").OrderBy("nick_name", structs.Desc).OrderByDesc("create_date").Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) != 10 {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestRawCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where2("nick_name like ? ", "%淑兰%").Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where(structs.Cnd("nick_name", structs.LikeIgnoreStart, "淑兰")).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestMultiCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where(structs.Cnd("nick_name", structs.LikeIgnoreStart, "淑兰").Or(structs.Cnd("phone_number", structs.Eq, "13663049871").Eq("nick_name", "吃素是福"))).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}

func TestStructCondition(t *testing.T) {
	user := UserInfo{PhoneNumber: "13663049871", NickName: "吃素是福"}
	users := make([]UserInfo, 0)
	_, er := db.Where(structs.StructToCondition(user)).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}

func TestDefaultStruct(t *testing.T) {
	logs := make([]TbRecord, 0)
	_, er := db.Select(&logs)
	if er != nil {
		panic(er)
	}
	if len(logs) == 0 {
		t.Error("counts :", len(logs), db)
		t.Fail()
	}
}

func TestRawQueryWithGroupBy(t *testing.T) {
	logs := make([]TbRecord, 0)
	_, er := db.Raw("select count(id) as id,sum(age) as age,sum(height) as height from tb_record group by create_date").Select(&logs)
	if er != nil {
		panic(er)
	}
	if len(logs) == 0 {
		t.Error("counts :", len(logs), db)
		t.Fail()
	}
}
func TestCount(t *testing.T) {
	cs := db.Table("user_info").Count("id")
	if cs.Error != nil {
		t.Error("counts :", db)
		t.Fail()
	}
}
func TestSum(t *testing.T) {
	cs := db.Table("tb_record").Sum("age")
	if cs.Error != nil {
		t.Error("counts :", db)
		t.Fail()
	}
}
func TestFirst(t *testing.T) {
	var log TbRecord
	_, er := db.First(&log)
	if er != nil {
		t.Error("log :", log, db)
		t.Fail()
	}
}
