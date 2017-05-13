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

func TestModel(t *testing.T) {
	var log Log
	m1 := getTableModel(&log)
	m2 := getTableModel(log)
	t.Log(m1, m2)
}
func TestCnds(t *testing.T) {
	t.Log("ok")
}
