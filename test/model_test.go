package test

import (
	"gitee.com/janyees/gom"
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
