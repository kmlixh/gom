package tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/kmlixh/gom/v2"
	_ "github.com/kmlixh/gom/v2/factory/mysql"
	"strconv"
	"testing"
	"time"
)

var dsn = "remote:remote123@tcp(10.0.1.5)/test?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

//var dsn = "remote:Remote171Yzy@tcp(13.236.1.51:3306)/user_centre?charset=utf8&loc=Asia%2FShanghai&parseTime=true"

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
	Id         string
	Age        int
	Height     int
	Width      int
	Length     int
	CreateDate time.Time
}
type User2 struct {
	Id         string    `json:"id,omitempty" gom:"!,id"`
	Name       string    `json:"name" gom:"#,name"`
	Age        int       `json:"age,omitempty"`
	Height     float64   `json:"height,omitempty"`
	Width      float32   `json:"width,omitempty"`
	BinData    []byte    `json:"bin_data,omitempty"`
	CreateDate time.Time `json:"create_date"`
}

func init() {
	fmt.Println("init DB.............")
	temp, er := gom.Open("mysql", dsn, true)
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
	_, err := gom.GetTableModel(&log)
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
	_, er := db.OrderByAsc("id").OrderBy("nick_name", gom.Desc).OrderByDesc("create_date").Page(0, 10).Select(&users)
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
	_, er := db.Where(gom.Cnd("nick_name", gom.LikeIgnoreStart, "淑兰")).Page(0, 10).Select(&users)
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
	_, er := db.Where(gom.Cnd("nick_name", gom.LikeIgnoreStart, "淑兰").Or2(gom.Cnd("phone_number", gom.Eq, "13663049871").Eq("nick_name", "吃素是福"))).Page(0, 10).Select(&users)
	if er != nil {
		panic(er)
	}
	if len(users) == 0 {
		t.Error("counts :", len(users), db)
		t.Fail()
	}
}
func TestMultiEmptyCondition(t *testing.T) {
	cnd := gom.CndEmpty().And2(gom.CndEmpty().Eq("id", 23).Gt("test", 2)).And2(gom.CndEmpty()).Eq("name", "kmlixh")
	sql, data := db.Factory().ConditionToSql(false, cnd)
	fmt.Println(sql, data)
}

func TestStructCondition(t *testing.T) {
	user := UserInfo{PhoneNumber: "13663049871", NickName: "吃素是福"}
	users := make([]UserInfo, 0)
	_, er := db.Where(gom.StructToCondition(user)).Page(0, 10).Select(&users)
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
	_, er := db.Table("user_info").Count("id")
	if er != nil {
		t.Error("counts :", db)
		t.Fail()
	}
}
func TestSum(t *testing.T) {
	count, er := db.Table("tb_record").Sum("age")
	if er != nil {
		t.Error("counts :", db)
		t.Fail()
	}
	print(count)
}
func TestFirst(t *testing.T) {
	var log TbRecord
	_, er := db.First(&log)
	if er != nil {
		t.Error("log :", log, db)
		t.Fail()
	}
}

type EmptyStruct struct {
}

func TestSpecial(t *testing.T) {
	ts := []Tt{
		{"测试使用Interface获取对象", func(t *testing.T) {
			var v interface{}
			m, er := gom.GetTableModel(v)
			if er != nil {
				t.Error("使用interface获取表模型应该报错", m, er)
			}
		}},
		{"测试使用Interface赋值Struct获取对象", func(t *testing.T) {
			var v interface{} = User{}
			m, er := gom.GetTableModel(v)
			if er != nil {
				t.Error(m, er)
			}
		}},
		{"测试对数组使用StructToMap", func(t *testing.T) {
			var v []interface{}
			m, n, er := gom.StructToMap(v)
			if er == nil {
				t.Error("interface数组未报错", m, n)
			}
		}},
		{"用interface使用StructToMap", func(t *testing.T) {
			var v interface{}
			m, n, er := gom.StructToMap(v)
			if er == nil {
				t.Error("interface未报错", m, n)
			}
		}},
		{"测试MapToCondition", func(t *testing.T) {
			maps := map[string]interface{}{"name": "kmlixh", "age": 12, "sex": "big cook"}
			c := gom.MapToCondition(maps)
			if c == nil {
				t.Error("MaptoCondition失败", c)
			}
		}},
		{"测试MapToCondition", func(t *testing.T) {
			maps := map[string]interface{}{"name": "kmlixh", "age": 12, "sex": "big cook"}
			c := gom.MapToCondition(maps)
			if c == nil {
				t.Error("MaptoCondition失败", c)
			}
		}},
		{"测试空结构体获取Map", func(t *testing.T) {
			d, ds, er := gom.StructToMap(EmptyStruct{})
			if er == nil {
				t.Error("MaptoCondition失败", d, ds)
			}
		}},
		{"空结构体获取TableModel", func(t *testing.T) {
			tb, er := gom.GetTableModel(EmptyStruct{})
			if er == nil {
				t.Error("空结构体生成tb成功", tb, er)
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
			count, id, er := db.Insert(user)
			if count != 1 || er != nil {
				t.Error("单个非自增插入错误", count, id, er)
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
			count, id, er := db.Insert(users)
			if count != 100 || er != nil {
				t.Error("批量非自增插入错误", count, id, er)
			}

		}},
		{
			"测试获取RawDb", func(t *testing.T) {
				rawDb := db.RawDb()
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
				db.CleanDb().GetCnd()
			},
		},
		{
			"open by wrong driver", func(t *testing.T) {
				ddb, er := gom.Open("sdf", dsn, false)
				if er == nil {
					t.Error(ddb, er)
				}
				ddbs, ers := gom.OpenWithConfig("sdf", dsn, 1000, 1000, false)
				if ers == nil {
					t.Error(ddbs, ers)
				}
			},
		},
		{
			"open by wrong Config", func(t *testing.T) {
				ddb, er := gom.Open("sdf", dsn, false)
				if er == nil {
					t.Error(ddb, er)
				}
				ddbs, ers := gom.OpenWithConfig("mysql", dsn, -1000, -1000, false)
				if ers != nil {
					t.Error(ddbs, ers)
				}
			},
		},
		{
			"简单类型查询返回多列", func(t *testing.T) {
				var ids []int64
				_, er := db.Raw("select * from user").Select(&ids, "id", "sdfds")
				if er == nil {
					t.Error("简单数据插入多列应该报错")
				}
			},
		},
		{
			"无条件更新应当报错", func(t *testing.T) {
				_, _, er := db.Update(User{NickName: "sdfdsf", RegDate: time.Now()})
				if er == nil {
					t.Error("无条件更新应当报错")
				}
			},
		},
		{
			"事务回滚测试", func(t *testing.T) {
				uid := uuid.New().String()

				c, er := db.DoTransaction(func(dbTx *gom.DB) (interface{}, error) {
					c, _, er := dbTx.Insert(User{NickName: uid, Valid: 2, Email: "test@gg.com", RegDate: time.Now()})
					if er != nil {
						return c, er
					}
					_, _, err := dbTx.Raw("update dafadsf set dfadf").Update(nil)
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
			"Test ITableName", func(t *testing.T) {
				var user User
				mm, er := gom.GetTableModel(user)
				if er != nil {
					t.Error(mm, er)
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
				_, er := db.Where(gom.CndEmpty().In("id", "sss")).OrderByDesc("id").Select(&users)
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
