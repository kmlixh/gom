package gom

import (
	"fmt"
	"gitee.com/janyees/gom/structs"
	"github.com/google/uuid"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestDB_CleanOrders(t *testing.T) {
	db1 := DB{}
	db2 := DB{}
	db3 := DB{}
	db2.OrderBy("name", structs.Desc)
	db2.OrderBy("name", structs.Desc).OrderByDesc("use")
	tests := []struct {
		name string
		raw  DB
		want *[]structs.OrderBy
	}{
		{"empty orders clean", db1, &[]structs.OrderBy{}},
		{"有一个时除去", db2, &[]structs.OrderBy{}},
		{"有多个时清空", db3, &[]structs.OrderBy{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := tt.raw
			if got := this.CleanOrders().orderBys; !reflect.DeepEqual(got, tt.want) {
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
			if got := this.Table(tt.args.tableName).Count(tt.args.columnName); got.Count < tt.want {
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
			gots := structs.UnZipSlice(tt.args)
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
			gots := structs.SliceToGroupSlice(tt.args)
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
			gots, _, er := structs.StructToMap(tt.args)
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
			user := User{NickName: nck, Email: nck + "@nck.com", RegDate: time.Now()}
			c, _, er := db.Insert(user)
			if c != 1 && er != nil {
				t.Error("插入异常：", er.Error())
			}
			var tmp User
			db.Where2("nick_name=?", nck).Select(&tmp)
			if tmp.Id == 0 {
				t.Error("插入成功但查询失败")
			}

		}},
		{
			"批量插入操作", func(t *testing.T) {
				var users []User
				var ncks []string
				for i := 0; i < 100; i++ {
					nck := uuid.New().String()
					ncks = append(ncks, nck)
					user := User{NickName: nck, Pwd: "pwd" + strconv.Itoa(i), Email: nck + "@nck.com", RegDate: time.Now()}
					users = append(users, user)
				}
				c, _, er := db.Insert(users)
				fmt.Println("插入结果：", c, er)
				var tempUsers []User
				_, err := db.Where(structs.CndRaw("id > ?", 0).In("nick_name", ncks)).Select(&tempUsers)
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

func TestDB_Delete(t *testing.T) {
	tests := []struct {
		name string
		t    func(t *testing.T)
	}{
		{"测试单个插入后删除", func(t *testing.T) {
			nck := uuid.New().String()
			user := User{NickName: nck, Pwd: "1213", Email: nck + "@nck.com", RegDate: time.Now()}
			c, _, er := db.Insert(user)
			if c != 1 && er != nil {
				t.Error("插入异常：", er.Error())
			}
			c, _, er = db.Table("user").Where2("nick_name=?", nck).Delete()
			if c != 1 {
				t.Error("删除失败")
			}
		}},
		{
			"批量插入后操作删除", func(t *testing.T) {
				var users []User
				var ncks []string
				for i := 0; i < 100; i++ {
					nck := uuid.New().String()
					ncks = append(ncks, nck)
					user := User{NickName: nck, Pwd: "pwd" + strconv.Itoa(i), Email: nck + "@nck.com", Valid: 1, RegDate: time.Now()}
					users = append(users, user)
				}
				c, _, er := db.Insert(users)
				c, _, er = db.Table("user").Where(structs.CndRaw("valid=?", 1).In("nick_name", ncks)).Delete()
				if c != 100 || er != nil {
					t.Error("批量删除失败", c, er)
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
			c, id, er := db.Insert(user)
			if c != 1 && er != nil {
				t.Error("插入异常：", er.Error())
			}
			var temp User
			_, err := db.Where(structs.CndRaw("id=?", id)).Select(&temp)
			if err != nil {
				t.Error("插入后查询失败：", err)
			}
			if temp.Id == 0 {
				t.Error("插入失败")
			}
			temp.Email = "changed@cc.cc"
			c, _, er = db.Update(temp, "email")
			if c != 1 {
				t.Error("更新失败", c, er)
			}
			fmt.Println("单个更新结果：", c, er)
		}},
		{"指定表名更新", func(t *testing.T) {
			nck := uuid.New().String()
			user := User{NickName: nck, Pwd: "1213", Email: nck + "@nck.com", RegDate: time.Now()}
			c, _, er := db.Insert(user)
			if c != 1 && er != nil {
				t.Error("插入异常：", er.Error())
				return
			}
			fmt.Println(user.TableName(), c, er)
			//var temp User
			//_, err := db.Where2("nick_name=?", nck).Select(&temp)
			//if err != nil {
			//	t.Error("查询异常：", err)
			//}
			c, _, er = db.Table("user").Where2("nick_name=?", nck).Update(User{RegDate: time.Now().Add(10 * time.Minute)})
			if c != 1 {
				t.Error("更新失败", c, er)
				return
			}
			fmt.Println("单个更新结果：", c, er)
		}},
		{
			"批量插入后操作删除", func(t *testing.T) {
				var users []User
				var ncks []string
				for i := 0; i < 100; i++ {
					nck := uuid.New().String()
					ncks = append(ncks, nck)
					user := User{NickName: nck, Pwd: "pwd" + strconv.Itoa(i), Valid: 1, Email: nck + "@nck.com", RegDate: time.Now()}
					users = append(users, user)
				}
				c, _, er := db.Insert(users)
				fmt.Println("插入结果：", c, er)
				c, _, er = db.Table("user").Where(structs.CndRaw("valid=?", 1).In("nick_name", ncks)).Delete()
				if c != 100 || er != nil {
					t.Error("批量删除失败")
				}
				fmt.Println(c, er)
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
			_, ser := db.Raw("select * from user_info limit ?,?", 0, 1000).Select(&users)
			if ser != nil {
				t.Error("counts :", len(users), db)
			}
		}},
		{"测试RawSql时限定列数", func(t *testing.T) {
			var users []UserInfo
			_, ser := db.Raw("select * from user_info limit ?,?", 0, 1000).Select(&users, "id", "valid")
			if ser != nil {
				t.Error("counts :", len(users), db)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}

}
