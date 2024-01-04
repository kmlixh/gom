package tests

import (
	"database/sql"
	"github.com/google/uuid"
	"strconv"
	"testing"
	"time"
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
func BenchmarkDB_InsertSingle(b *testing.B) {
	uid := uuid.New().String()

	for i := 0; i < b.N; i++ {
		user := User2{
			Id:         uid + strconv.Itoa(i),
			Name:       uid + "_" + strconv.Itoa(i),
			Age:        20,
			Height:     120.23,
			Width:      123.11,
			BinData:    []byte{12, 43, 54, 122, 127},
			CreateDate: time.Now(),
		}
		db.Insert(user)
	}
}
func BenchmarkRaw_InsertSingle(b *testing.B) {
	for i := 0; i < b.N; i++ {

		uid := uuid.New().String()
		name := uid + strconv.Itoa(i)
		sql := "INSERT INTO `test`.`user2` (`id`, `name`, `age`, `height`, `width`, `bin_data`, `create_date`) VALUES (?, ?, 20, 120.23, 123.11, 0x0C2B367A7F, now());"

		st, er := rawDb.Prepare(sql)
		if er != nil {
			b.Error(er)
		}
		rs, er := st.Exec(uid, name)
		if er != nil {
			b.Error(er)
		}
		c, er := rs.RowsAffected()
		if c != 1 || er != nil {
			b.Error(c, er)
		}
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
	db.RawSql("select * from user limit 0,1000").Select(&users)
}
