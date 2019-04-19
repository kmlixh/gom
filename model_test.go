package gom

import (
	"testing"
	"time"
)

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
	m1, err := getTableModels(&log)
	t.Log(m1, err)
}
func TestGetTableModelRepeat(t *testing.T) {
	var log []Log
	m1, err := getTableModels(&log)
	t.Log(m1, err)
	m2, err := getTableModels(&log)
	t.Log(m2, err)
}
func TestCnd(t *testing.T) {
	cnd := Cnd("")
	cnd.OrderBy("id", Desc)
	cnd.Page(0, 15)
	t.Log(cnd)
}
func BenchmarkTableModel(b *testing.B) {
	var log Log
	m1, err1 := getTableModel(&log)
	var user User
	m2, err2 := getTableModel(&user)
	b.Log(m1, m2, err1, err2)
}
func TestCnds(t *testing.T) {
	cnd := Cnd("name =? and id=?", "nicker", 1)
	t.Log(cnd)
}
func BenchmarkCnds(b *testing.B) {
	cnd := Cnd("name=? and id=? and user_anasf=?", "nide", 2.34, 1, true)
	b.Log(cnd)

}

type TestTable struct {
	Id  int `json:"id" gom:"@"`
	Kid int `json:"kid" gom:"#"`
	Vid int `json:"vid" gom:"#"`
}

func (TestTable) TableName() string {
	return "test_table"
}
