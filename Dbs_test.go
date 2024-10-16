package gom

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/kmlixh/gom/v3/define"
	"github.com/kmlixh/gom/v3/factory"
	"github.com/kmlixh/gom/v3/factory/mysql"
	"github.com/kmlixh/gom/v3/factory/postgres"
	"reflect"
	"strconv"
	"testing"
	"time"
)

var mysqlDsn = "root:123456@tcp(10.0.1.5:3306)/auth_centre?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

// var pgDsn = "postgres://postgres:yzy123@192.168.110.249:5432/db_dict?sslmode=disable"
var pgDsn = "postgres://postgres:123456@10.0.1.5:5432/auth_centre?sslmode=disable"

func TestDB_CleanOrders(t *testing.T) {
	db1 := DB{}
	db2 := DB{}
	db3 := DB{}
	db2.OrderBy("name", define.Desc)
	db2.OrderBy("name", define.Desc).OrderByDesc("use")
	tests := []struct {
		name string
		raw  DB
		want []define.OrderBy
	}{
		{"empty orders clean", db1, []define.OrderBy{}},
		{"有一个时除去", db2, []define.OrderBy{}},
		{"有多个时清空", db3, []define.OrderBy{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := tt.raw
			if got := this.CleanOrders().GetOrderBys(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CleanOrders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Count(t *testing.T) {

	type args struct {
		tableName  string
		columnName string
	}
	tests := []struct {
		name string
		db   *DB
		args args
		want int64
	}{
		// TODO: Add test cases.
		{"Count测试", db, args{"user_info", "id"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := tt.db
			if got, _ := this.Table(tt.args.tableName).Count(tt.args.columnName); got < tt.want {
				t.Errorf("Count() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_UnzipSlice(t *testing.T) {
	tests := []struct {
		name  string
		args  []interface{}
		wants []interface{}
	}{
		{"slice为空的情况", []interface{}{}, []interface{}{}},
		{name: "测试单层是否会展开", args: []interface{}{"dsfadsf", 23, "sdfadsf", ""}, wants: []interface{}{"dsfadsf", 23, "sdfadsf", ""}},
		{name: "测试头部第一层有嵌套", args: []interface{}{[]interface{}{"3w4", 23, "sdfsd"}, "name", "lest", "234", 123}, wants: []interface{}{"3w4", 23, "sdfsd", "name", "lest", "234", 123}},
		{"存在多层嵌套的情况", []interface{}{[]interface{}{12, 2, 43, 324, "sdfa", []interface{}{"dfadsf", 234, []interface{}{4, "sdfasd", "34343", "sdfadsf"}, 34, 2}, 3, 343, "sdf"}, 12, 2, 43, 324, "sdfa"}, []interface{}{12, 2, 43, 324, "sdfa", "dfadsf", 234, 4, "sdfasd", "34343", "sdfadsf", 34, 2, 3, 343, "sdf", 12, 2, 43, 324, "sdfa"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gots := UnZipSlice(tt.args)
			if !reflect.DeepEqual(gots, tt.wants) {
				t.Errorf("Test_UnzipSlice resource was: = %v, want: %v", gots, tt.wants)
			}
		})
	}
}

type U1 struct {
	name   string
	age    int
	height float32
	desc   string
}
type Car struct {
	name   string
	width  int
	height int
	weight int
}
type U2 struct {
	name    string
	types   int
	keyword string
}

func Test_UnzipSliceToMapSlice(t *testing.T) {
	tests := []struct {
		name  string
		args  []interface{}
		wants map[string][]interface{}
	}{
		{"测试单类型模式⚡️", []interface{}{U1{"u1", 1, 1, "dsf"}, U1{"fasd", 23, 434, "234"}}, map[string][]interface{}{"U1": {U1{"u1", 1, 1, "dsf"}, U1{"fasd", 23, 434, "234"}}}},
		{"测试多类型嵌套", []interface{}{U1{"u1", 1, 1, "dsf"}, U1{"fasd", 23, 434, "234"}, Car{"哈弗", 180, 430, 2}, []interface{}{U2{"u2", 2, "sdfdsf"}, U1{"sdfdsf", 12, 3, "sdfsdf"}}}, map[string][]interface{}{"U1": {U1{"u1", 1, 1, "dsf"}, U1{"fasd", 23, 434, "234"}, U1{"sdfdsf", 12, 3, "sdfsdf"}}, "Car": {Car{"哈弗", 180, 430, 2}}, "U2": {U2{"u2", 2, "sdfdsf"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gots := SliceToGroupSlice(tt.args)
			if !reflect.DeepEqual(gots, tt.wants) {
				t.Errorf("Test_UnzipSlice resource was: = %v, want: %v", gots, tt.wants)
			} else {
				if Debug {
					t.Logf("Test_UnzipSlice ok resource was: = %v, want: %v", gots, tt.wants)
				}

			}
		})
	}
}

func Test_StructToMap(t *testing.T) {
	tt := time.Now()
	type Results struct {
		result map[string]interface{}
		err    bool
	}
	tests := []struct {
		name  string
		args  interface{}
		wants Results
	}{
		{
			name: "测试简单类型转换逻辑",
			args: User{
				Id:       int64(1),
				Email:    "kmlixh@gqq.com",
				NickName: "dsfasdf",
				RegDate:  tt,
			},
			wants: Results{map[string]interface{}{"id": int64(1), "pwd": "dsfds", "email": "kmlixh@gqq.com", "valid": 1, "nick_name": "dsfasdf", "reg_date": tt}, false},
		},
		{
			name:  "测试基础类型string是否会报错",
			args:  "sdfasdf",
			wants: Results{make(map[string]interface{}), true},
		},
		//{
		//	name:  "测试基础类型time是否会报错",
		//	args:  time.Now(),
		//	wants: Results{make(map[string]interface{}), true},
		//},
		{
			name:  "测试Slice是否会报错",
			args:  []interface{}{1, 2, 3, 4},
			wants: Results{make(map[string]interface{}), true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gots, er := StructToMap(tt.args)
			if !reflect.DeepEqual(gots, tt.wants.result) && tt.wants.err && er == nil {
				t.Errorf("Test_StructToMap Fail, resource was: = %v, want: %v,er result:%v,er wants:%v", gots, tt.wants.result, er.Error(), tt.wants.err)
			}
		})
	}
}

func TestDB_Insert(t *testing.T) {

	tests := []struct {
		name string
		t    func(t *testing.T)
	}{
		{"测试单个插入", func(t *testing.T) {
			nck := uuid.New().String()
			user := User{NickName: nck, Pwd: "aaa", Valid: 111, Email: nck + "@nck.com", RegDate: time.Now()}
			c, er := db.Insert(user)
			if c == nil && er != nil {
				t.Error("插入异常：", er.Error())
			}
			cc, er := c.RowsAffected()
			if cc != 1 && er != nil {
				t.Error("插入异常：", er.Error())
			}
			var tmp User
			r, err := db.Where2("nick_name=?", nck).Select(&tmp)
			if err != nil {
				t.Error(err, r)
			}
			if tmp.Id == 0 {
				t.Error("插入成功但查询失败")
			}

		}},
		{
			"批量插入操作", func(t *testing.T) {
				var users []User
				var ncks []interface{}
				for i := 0; i < 100; i++ {
					nck := uuid.New().String()
					ncks = append(ncks, nck)
					user := User{NickName: nck, Pwd: "pwd" + strconv.Itoa(i), Email: nck + "@nck.com", RegDate: time.Now()}
					users = append(users, user)
				}
				c, er := db.Insert(users)
				if er != nil {
					t.Error("批量插入报错", c, er)
				}
				var tempUsers []User
				_, err := db.Where(CndRaw("id > ?", 0).In("nick_name", ncks...)).Select(&tempUsers)

				if err != nil {
					t.Error("查询出错")
				}
				if len(tempUsers) != len(users) {
					t.Error("批量插入失败")
				}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t(t)
		})
	}
}

var db *DB
var mysqlDb *DB

type UserInfo struct {
	Id          int64     `json:"id" gom:"id"`
	PhoneNumber string    `json:"phone_number" gom:"phone_number"`
	Unionid     string    `json:"unionid" gom:"unionid"`
	NickName    string    `json:"nick_name" gom:"nick_name"`
	HeadSrc     string    `json:"head_src" gom:"head_src"`
	Sex         int       `json:"sex" gom:"sex"`
	Score       int64     `json:"score" gom:"-"`
	DonateTag   int64     `json:"donate_tag" gom:"donate_tag"`
	Title       string    `json:"title" gom:"-"`
	CheckIn     bool      `json:"check_in" gom:"-"`
	CreateDate  time.Time `json:"create_date" gom:"create_date"`
}
type TbRecord struct {
	Id         string
	Age        int
	Height     int
	Width      int
	Length     int
	CreateDate time.Time
}
type User2 struct {
	Id         string    `json:"id,omitempty" gom:"id"`
	Name       string    `json:"name" gom:"name"`
	Age        int       `json:"age,omitempty"`
	Height     float64   `json:"height,omitempty"`
	Width      float32   `json:"width,omitempty"`
	BinData    []byte    `json:"bin_data,omitempty"`
	CreateDate time.Time `json:"create_date"`
}

func init() {
	fmt.Println("init DB.............")
	if f, ok := factory.Get("Postgres"); f == nil || ok {
		postgres.InitFactory()
	}
	temp, er := Open("Postgres", pgDsn, true)
	if er != nil {
		panic(er)
	}
	db = temp
	if f, ok := factory.Get("mysql"); f == nil || ok {
		mysql.InitFactory()
	}
	tt, er := Open("mysql", mysqlDsn, true)
	if er != nil {
		panic(er)
	}
	mysqlDb = tt
}

type User struct {
	Id       int64     `json:"id" gom:"id"`
	Pwd      string    `json:"pwd" gom:"pwd"`
	Email    string    `json:"email" gom:"email"`
	Valid    int       `json:"valid" gom:"valid"`
	NickName string    `json:"nicks" gom:"nick_name"`
	RegDate  time.Time `json:"reg_date" gom:"reg_date"`
}

func (User) TableName() string {
	return "user"
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
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestCustomTableName(t *testing.T) {
	users := make([]UserInfo, 0)
	_, ser := db.Table("user_info2").Page(0, 1000).Select(&users)
	if ser != nil {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}

func TestMultiOrders(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.OrderByAsc("id").OrderBy("nick_name", define.Desc).OrderByDesc("create_date").Page(0, 10).Select(&users)
	if er != nil {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestRawCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where2("nick_name like ? ", "%淑兰%").Page(0, 10).Select(&users)
	if er != nil {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where(Cnd("nick_name", define.LikeIgnoreStart, "淑兰")).Page(0, 10).Select(&users)

	if er != nil {
		t.Error("counts :", er, db)
		t.Fail()
	}
}
func TestMultiCondition(t *testing.T) {
	users := make([]UserInfo, 0)
	_, er := db.Where(Cnd("nick_name", define.LikeIgnoreStart, "淑兰").Or2(Cnd("phone_number", define.Eq, "13663049871").Eq("nick_name", "吃素是福"))).Page(0, 10).Select(&users)
	if er != nil {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestMultiEmptyCondition(t *testing.T) {
	cnd := CndEmpty().And2(CndEmpty().Eq("id", 23).Gt("test", 2)).And2(CndEmpty()).Eq("name", "kmlixh")
	sql, data := db.Factory().ConditionToSql(false, cnd)
	if sql == "" || data == nil {
		t.Error("TestMultiEmptyCondition failed")
	}
}

func TestStructCondition(t *testing.T) {
	user := UserInfo{PhoneNumber: "13663049871", NickName: "吃素是福"}
	db.Insert(user)
	users := make([]UserInfo, 0)
	_, er := db.Where(StructToCondition(user)).Page(0, 10).Select(&users)
	if er != nil {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}

func TestDefaultStruct(t *testing.T) {
	logs := make([]TbRecord, 0)
	_, er := db.Select(&logs)
	if er != nil {
		t.Error("counts :", len(logs), db)
		t.Fail()
	}
}

func TestRawQueryWithGroupBy(t *testing.T) {
	logs := make([]TbRecord, 0)
	_, er := db.RawSql("select count(id) as id,sum(age) as age,sum(height) as height from tb_record group by create_date").Select(&logs)
	if er != nil {
		t.Error("counts :", len(logs), db)
		t.Fail()
	}
}
func TestCount(t *testing.T) {
	_, er := db.Table("user_info").Count("id")
	if er != nil {
		t.Error("counts :", db)
		t.Fail()
	}
}
func TestSum(t *testing.T) {
	count, er := db.Table("tb_record").Sum("age")
	if er != nil {
		t.Error("counts :", count, er)
	}
	print(count)
}
func TestFirst(t *testing.T) {
	var log TbRecord
	_, er := db.First(&log)
	if er != nil {
		t.Error("log :", log, db)
	}
}

type EmptyStruct struct {
}

func TestSpecial(t *testing.T) {
	ts := []Tt{

		{"测试对数组使用StructToMap", func(t *testing.T) {
			var v []interface{}
			m, er := StructToMap(v)
			if er == nil {
				t.Error("interface数组未报错", m, er)
			}
		}},
		{"用interface使用StructToMap", func(t *testing.T) {
			var v interface{}
			m, er := StructToMap(v)
			if er == nil {
				t.Error("interface未报错", m, er)
			}
		}},
		{"测试MapToCondition", func(t *testing.T) {
			maps := map[string]interface{}{"name": "kmlixh", "age": 12, "sex": "big cook"}
			c := MapToCondition(maps)
			if c == nil {
				t.Error("MaptoCondition失败", c)
			}
		}},
		{"测试MapToCondition", func(t *testing.T) {
			maps := map[string]interface{}{"name": "kmlixh", "age": 12, "sex": "big cook"}
			c := MapToCondition(maps)
			if c == nil {
				t.Error("MaptoCondition失败", c)
			}
		}},
		{"测试空结构体获取Map", func(t *testing.T) {
			d, er := StructToMap(EmptyStruct{})
			if er == nil {
				t.Error("MaptoCondition失败", d, er)
			}
		}},
		{"非自增主键插入单条数据", func(t *testing.T) {
			user := User2{
				Id:         uuid.New().String(),
				Age:        20,
				Height:     120.23,
				Width:      123.11,
				BinData:    []byte{12, 43, 54, 122, 127},
				CreateDate: time.Now(),
			}
			results, er := db.Insert(user)
			if er != nil {
				t.Error("单个非自增插入错误", results, er)
			}
			c, er := results.RowsAffected()
			if c != 1 || er != nil {
				t.Error("单个非自增插入错误", results, er)
			}
		}},
		{"非自增主键批量插入数据", func(t *testing.T) {
			var users []User2
			for i := 0; i < 100; i++ {
				uid := uuid.New().String()
				user := User2{
					Id:         uid,
					Name:       uid + "_" + strconv.Itoa(i),
					Age:        20,
					Height:     120.23,
					Width:      123.11,
					BinData:    []byte{12, 43, 54, 122, 127},
					CreateDate: time.Now(),
				}
				users = append(users, user)
			}
			results, er := db.Insert(users)
			c, er := results.RowsAffected()
			if c != 100 || er != nil {
				t.Error("批量非自增插入错误", results, er)
			}

		}},
		{
			"测试获取RawDb", func(t *testing.T) {
				rawDb := db.GetRawDb()
				if rawDb == nil {
					t.Error("rawDb is nil", rawDb)
				}
			},
		},
		{
			"测试CleanDb", func(t *testing.T) {
				db.CleanDb()
			},
		},
		{
			"非自增主键查询", func(t *testing.T) {
				var users []User2
				_, err := db.Select(&users)
				if err != nil {
					t.Error(err)
				}
			},
		},
		{
			"获取空的OrderBys", func(t *testing.T) {
				db.CleanDb().GetOrderBys()
			},
		},
		{
			"获取空的Page", func(t *testing.T) {
				db.CleanDb().GetPage()
			},
		},
		{
			"获取空的Condition", func(t *testing.T) {
				db.CleanDb().GetCondition()
			},
		},
		{
			"open by wrong driver", func(t *testing.T) {
				ddb, er := Open("sdf", mysqlDsn, false)
				if er == nil {
					t.Error(ddb, er)
				}
				ddbs, ers := OpenWithConfig("sdf", mysqlDsn, 1000, 1000, false)
				if ers == nil {
					t.Error(ddbs, ers)
				}
			},
		},
		{
			"open by wrong Config", func(t *testing.T) {
				ddb, er := Open("sdf", mysqlDsn, false)
				if er == nil {
					t.Error(ddb, er)
				}
				ddbs, ers := OpenWithConfig("mysql", mysqlDsn, -1000, -1000, false)
				if ers != nil {
					t.Error(ddbs, ers)
				}
			},
		},
		{
			"简单类型查询返回多列", func(t *testing.T) {
				var ids []int64
				_, er := db.RawSql("select * from user").Select(&ids, "id", "sdfds")
				if er == nil {
					t.Error("简单数据插入多列应该报错")
				}
			},
		},
		{
			"无条件更新应当报错", func(t *testing.T) {
				_, er := db.Update(User{NickName: "sdfdsf", RegDate: time.Now()})
				if er == nil {
					t.Error("无条件更新应当报错")
				}
			},
		},
		{
			"事务回滚测试", func(t *testing.T) {
				uid := uuid.New().String()

				c, er := db.DoTransaction(func(dbTx *DB) (interface{}, error) {
					c, er := dbTx.Insert(User{NickName: uid, Valid: 2, Email: "test@gg.com", RegDate: time.Now()})
					if er != nil {
						return c, er
					}
					_, err := dbTx.RawSql("update dafadsf set dfadf").Update(nil)
					if err != nil {
						return 0, err
					}
					return 0, nil
				})
				if er != nil {
					var temp User
					db.Where2("nick_name=?", uid).Select(&temp)
					if temp.Id != 0 {
						t.Error("事务回滚失败", c, er)
					}
				}
			},
		},
		{
			"test order by", func(t *testing.T) {
				var users []User
				_, er := db.OrderByDesc("id").Select(&users)
				if er != nil {
					t.Error(users, er)
				}
			},
		},
		{
			"test in one", func(t *testing.T) {
				var users []User
				_, er := db.Where(CndEmpty().In("id", "sss")).OrderByDesc("id").Select(&users)
				if er != nil {
					t.Error(users, er)
				}
			},
		},
	}

	for _, tt := range ts {
		t.Run(tt.name, tt.t)
	}
}

func TestDB_Delete(t *testing.T) {
	tests := []struct {
		name string
		t    func(t *testing.T)
	}{
		{"测试单个插入后删除", func(t *testing.T) {
			nck := uuid.New().String()
			user := User{NickName: nck, Pwd: "1213", Email: nck + "@nck.com", RegDate: time.Now()}
			r, er := db.Insert(user)
			if r == nil || er != nil {
				t.Error("单个插入失败1", er)
			}
			c1, er := r.RowsAffected()
			if c1 == 0 || er != nil {
				t.Error("单个插入失败2", er)
			}
			r2, ers := db.Table("user").Where2("nick_name=?", nck).Delete()
			c2, er := r2.RowsAffected()

			if c2 != 1 || ers != nil {
				t.Error("批量删除失败", c2, er)
			}
		}},
		{
			"批量插入后操作删除", func(t *testing.T) {
				var users []User
				var ncks []interface{}
				for i := 0; i < 100; i++ {
					nck := uuid.New().String()
					ncks = append(ncks, nck)
					user := User{NickName: nck, Pwd: "pwd" + strconv.Itoa(i), Email: nck + "@nck.com", Valid: 1, RegDate: time.Now()}
					users = append(users, user)
				}
				r, er := db.Insert(users)
				if r == nil || er != nil {
					t.Error("批量插入失败1", er)
				}
				c1, er := r.RowsAffected()
				if c1 == 0 || er != nil {
					t.Error("批量插入失败2", er)
				}
				lastId, er := r.LastInsertId()
				if lastId == 0 || er != nil {
					t.Error("批量插入失败2", er)
				}
				r2, ers := db.Table("user").Where(CndRaw("valid=?", 1).In("nick_name", ncks...)).Delete()
				c2, er := r2.RowsAffected()

				if c2 != 100 || ers != nil {
					t.Error("批量删除失败", c2, er)
				}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t(t)
		})
	}
}
func TestDB_Update(t *testing.T) {
	tests := []struct {
		name string
		t    func(t *testing.T)
	}{
		{"默认单个更新", func(t *testing.T) {
			nck := uuid.New().String()
			user := User{NickName: nck, Pwd: "1213", Valid: 1, Email: nck + "@nck.com", RegDate: time.Now()}
			r, er := db.Insert(user)
			if er != nil {
				t.Error("插入异常：", er.Error())
			}
			c, er := r.RowsAffected()
			if c == 0 || er != nil {
				t.Error("插入异常：", er.Error())
			}
			id, er := r.LastInsertId()
			if id == 0 || er != nil {
				t.Error("插入异常：", er.Error())
			}
			var temp User
			_, err := db.Where(CndRaw("id=?", id)).Select(&temp)
			if err != nil {
				t.Error("插入后查询失败：", err)
			}
			if temp.Id == 0 {
				t.Error("插入失败")
			}
			temp.Email = "changed@cc.cc"
			r, er = db.Update(temp, "email")
			ce, er := r.RowsAffected()
			if ce != 1 || er != nil {
				t.Error("更新失败", c, er)
			}
		}},
		{"指定表名更新", func(t *testing.T) {
			nck := uuid.New().String()
			user := User{NickName: nck, Pwd: "1213", Email: nck + "@nck.com", RegDate: time.Now()}
			r, er := db.Insert(user)
			if er != nil {
				t.Error("插入异常：", er.Error())
			}
			c, er := r.RowsAffected()
			if c == 0 || er != nil {
				t.Error("插入异常：", er.Error())
			}
			id, er := r.LastInsertId()
			if id == 0 || er != nil {
				t.Error("插入异常：", er.Error())
			}
			rr, er := db.Table("user").Where2("nick_name=?", nck).Update(User{RegDate: time.Now().Add(10 * time.Minute)})

			ce, er := rr.RowsAffected()
			if ce != 1 || er != nil {
				t.Error("更新失败", c, er)
			}
		}},
		{"带事务处理批量插入", func(t *testing.T) {
			c, er := db.DoTransaction(func(db *DB) (interface{}, error) {
				var users []User
				var ncks []interface{}
				for i := 0; i < 100; i++ {
					nck := uuid.New().String()
					ncks = append(ncks, nck)
					user := User{NickName: nck, Pwd: "pwd" + strconv.Itoa(i), Email: nck + "@nck.com", Valid: 1, RegDate: time.Now()}
					users = append(users, user)
				}
				r, er := db.Insert(users)
				c, err := r.RowsAffected()
				if er != nil || c != 100 || err != nil {
					t.Error("插入失败", er, err)
				}

				return c, er
			})
			if er != nil || c == 0 {
				t.Error("带事务批量操作失败", c, er)
			}
		}},
		{"测试Update Raw", func(t *testing.T) {
			r, er := db.RawSql("update user2 set age= ?", 101).Update(nil)
			if er != nil {
				t.Error("raw executeTableModel failed", r, er)
			}
		}},
		{"测试Update2 空状态", func(t *testing.T) {
			c, er := db.Update(nil)
			if er == nil {
				t.Error("空白更新未抛出一场", c, er)
			}
		}},
		{"GetOrderBys", func(t *testing.T) {
			orderbys := db.OrderBy("name", define.Desc).OrderBy("id", define.Asc).GetOrderBys()
			if orderbys == nil || len(orderbys) == 0 {
				t.Error(orderbys)
			}
		}},
		{"Get PageInfo", func(t *testing.T) {
			index, limit := db.Page(0, 1000).GetPage()
			if index != 0 || limit != 1000 {
				t.Error(index)
			}
		}},
		{"Get Cnd", func(t *testing.T) {
			cnd := db.Where(CndEq("name", "kmlixh")).GetCondition()
			if cnd == nil {
				t.Error(cnd)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}

type Tt struct {
	name string
	t    func(t *testing.T)
}

func TestDB_Select(t *testing.T) {
	tests := []Tt{
		{"测试RawSql", func(t *testing.T) {
			var users []UserInfo
			_, ser := db.RawSql("select * from user_info limit ?,?", 0, 1000).Select(&users)
			if ser != nil {
				t.Error("counts :", len(users), db)
			}
		}},
		{"测试RawSql查询单列", func(t *testing.T) {
			var users []UserInfo
			_, ser := db.RawSql("select * from user_info limit ?,?", 0, 1000).Select(&users, "id")
			if ser != nil {
				t.Error("counts :", len(users), db)
			}
		}},
		{"测试RawSql查询单列进简单数组", func(t *testing.T) {
			var ids []int64
			_, ser := db.RawSql("select * from user_info limit ?,?", 0, 1000).Select(&ids, "id")
			if ser == nil {
				t.Error("counts :", len(ids), db)
			}
		}},
		{"测试RawSql查询单列进简单数组column为空是否报错", func(t *testing.T) {
			var ids []int64
			_, ser := db.RawSql("select * from user_info limit ?,?", 0, 1000).Select(&ids, "id", "sdfa")
			if ser == nil {
				t.Error("简单类型columns不为1时必须报错")
			}
		}},
		{"测试RawSql时限定列数", func(t *testing.T) {
			var users []UserInfo
			_, ser := db.RawSql("select * from user_info limit ?,?", 0, 1000).Select(&users, "id", "valid")
			if ser == nil {
				t.Error("counts :", len(users), db)
			}
		}},
		{
			"测试Count时cnd为nil", func(t *testing.T) {
				var cnd define.Condition
				cc, er := db.Where(cnd).Table(UserInfo{}.TableName()).Count("id")
				if er != nil {
					t.Error("count failed:", er, cc)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}

}

func TestDB_GetTables(t *testing.T) {

	tests := []struct {
		name    string
		db      *DB
		want    []string
		wantErr bool
	}{
		{"获取mysqldb", mysqlDb, []string{"tb_record", "user", "user2", "user_info", "user_info2"}, false},
		{"获取PG tables", db, []string{"tb_record", "user", "user2", "user_info", "user_info2"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.db
			got, err := db.GetTables()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTables() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTables() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_GetCurrentSchema(t *testing.T) {

	tests := []struct {
		name    string
		db      *DB
		want    string
		wantErr bool
	}{
		{"测试Mysql", mysqlDb, "test", false},
		{"测试Mysql", db, "public", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.db
			got, err := db.GetCurrentSchema()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCurrentSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetCurrentSchema() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_GetTableStruct(t *testing.T) {

	type args struct {
		table string
	}
	tests := []struct {
		name    string
		db      *DB
		args    args
		want    define.ITableStruct
		wantErr bool
	}{
		{db: db, args: args{"tb_sys_menu"}},
		{db: mysqlDb, args: args{"tb_sys_role"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.db
			got, err := db.GetTableStruct(tt.args.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTableStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.GetTableName() == "" {
				t.Errorf("GetTableStruct() got = %v", got)
			}
			fmt.Print(got)
		})
	}
}
