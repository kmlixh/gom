package gom

import (
	"database/sql"
	"testing"
)

var rawDb *sql.DB

func init() {
	rawDb, _ = sql.Open("mysql", dsn)
}
func TestNothing(t *testing.T) {

}
func BenchmarkBaseSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		selectDataByRawDb()
	}
}
func BenchmarkBaseSelectGom(b *testing.B) {
	for i := 0; i < b.N; i++ {
		selectDataByGom()
	}
}
func selectDataByRawDb() {
	var users []User
	st, er := rawDb.Prepare("select * from user limit 0,1000")
	if er != nil {
		panic(er)
	}
	rows, er := st.Query()
	if er != nil {
		panic(er)
	}
	for rows.Next() {
		var user User
		rows.Scan(&user.Id, &user.Pwd, &user.Email, &user.Valid, &user.NickName, &user.RegDate)
		users = append(users, user)
	}
	rows.Close()
}
func selectDataByGom() {
	var users []User
	db.Raw("select * from user limit 0,1000").Select(&users)
}
