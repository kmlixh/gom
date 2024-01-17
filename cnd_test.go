package gom

import (
	"github.com/kmlixh/gom/v3/define"
	"testing"
)

func init() {
}
func TestConditions(t *testing.T) {

	tests := []Tt{
		{"测试Raw创建", func(t *testing.T) {
			CndRaw("name = ?", "kmlixh")
		}},
		{"测试Full", func(t *testing.T) {
			CndFull(define.And, "name", define.NotEq, "", "kmlixh")
		}},
		{"测试New", func(t *testing.T) { Cnd("name", define.NotEq, "kmlixh") }},

		{"测试NewEq", func(t *testing.T) {
			CndEq("name", "kmlixh")
		}},
		{"测试NewNotEq", func(t *testing.T) {
			CndNotEq("name", "kmlixh")
		}},
		{"测试NewGe", func(t *testing.T) {
			CndGe("name", "kmlixh")
		}},
		{"测试NewGt", func(t *testing.T) {
			CndGt("name", "kmlixh")
		}},
		{"测试NewLe", func(t *testing.T) {
			CndLe("name", "kmlixh")
		}},
		{"测试NewLt", func(t *testing.T) {
			CndLt("name", "kmlixh")
		}},
		{"测试NewLike", func(t *testing.T) {
			CndLike("name", "kmlixh")
		}},
		{"测试NewLikeIgnoreStart", func(t *testing.T) {
			CndLikeIgnoreStart("name", "kmlixh")
		}},
		{"测试LikeIgnoreEnd", func(t *testing.T) {
			CndLikeIgnoreEnd("name", "kmlixh")
		}},
		{"测试NewIn", func(t *testing.T) {
			CndIn("name", "kmlixh")
		}},
		{"测试NewNotIn", func(t *testing.T) {
			CndNotIn("name", "kmlixh")
		}},
		{"测试NewIsNull", func(t *testing.T) {
			CndIsNull("name")
		}},
		{"测试NewIsNotNull", func(t *testing.T) {
			CndIsNotNull("name")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}
func TestOperation(t *testing.T) {
	tests := []Tt{
		{"Test cnd function", func(t *testing.T) {
			cnd := Cnd("name", define.NotEq, "kmlixh").
				And("age", define.Lt, 12).And2(Cnd("gg", define.Eq, "ss")).And3("ssdf=?", 23).
				Or("sdfsd", define.Eq, "sdafs").Or2(CndRaw("age >= ?", 22)).Or3("name like ?", "sadf").OrNotEq("sdf", "sdfsd").
				Eq("name", "j").OrEq("sdfsd", "sdf").NotEq("name", "sdfds").
				Ge("height", 12).OrGe("width", 234).Gt("sdfs", "sdfs").OrGt("sfsdf", "sfdsf").
				Le("asdfa", "sdfds").Lt("sdfds", "sdfsdf").OrLe("sdfss", "sdf").OrLt("asdfs", "asdf").
				Like("sdfas", "sdafsd").LikeIgnoreStart("sfads", "sdfds").LikeIgnoreEnd("asdfasdf", "sdfds").OrLike("sadfas", "sdf").
				OrLike("sdfsd", "sdfds").OrLikeIgnoreStart("sdfsd", "sdfdsf").OrLikeIgnoreEnd("sdfsd", "werwe").
				In("id", "sdf", "sdf", "sdfsd").NotIn("sdfasdf", "sdf").OrIn("asdf", "dfdfd").OrNotIn("safs", "asfsdf").
				IsNull("sadfa").IsNotNull("asdfasd").OrIsNull("safasdf").OrIsNotNull("sadfasdf")
			str, er := db.Factory().ConditionToSql(false, cnd)
			if str == "" || er == nil {
				t.Error("condition to string failed", er, str)
			}
		}},
		{
			"测试Empty后缀情况", func(t *testing.T) {
				cnd := CndEmpty().And3("name=?", "km")
				str, er := db.Factory().ConditionToSql(false, cnd)
				if str != "name=?" || er == nil {
					t.Error("condition to string failed", er, str)
				}
			},
		},
		{
			"test MaptoConditon", func(t *testing.T) {
				maps := map[string]interface{}{"name": []string{"lier", "kmlixh", "sdfdsf"}, "id": "xxxx"}
				cnd := MapToCondition(maps)
				var users []User
				db.Where(cnd).Select(&users)
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}
