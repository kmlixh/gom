package tests

import (
	"github.com/kmlixh/gom/v2"
	"testing"
)

func init() {
}
func TestConditions(t *testing.T) {

	tests := []Tt{
		{"测试Raw创建", func(t *testing.T) {
			gom.NewRaw("name = ?", "kmlixh")
		}},
		{"测试Full", func(t *testing.T) {
			gom.NewFull(gom.And, "name", gom.NotEq, "", "kmlixh")
		}},
		{"测试New", func(t *testing.T) { gom.New("name", gom.NotEq, "kmlixh") }},

		{"测试NewEq", func(t *testing.T) {
			gom.NewEq("name", "kmlixh")
		}},
		{"测试NewNotEq", func(t *testing.T) {
			gom.NewNotEq("name", "kmlixh")
		}},
		{"测试NewGe", func(t *testing.T) {
			gom.NewGe("name", "kmlixh")
		}},
		{"测试NewGt", func(t *testing.T) {
			gom.NewGt("name", "kmlixh")
		}},
		{"测试NewLe", func(t *testing.T) {
			gom.NewLe("name", "kmlixh")
		}},
		{"测试NewLt", func(t *testing.T) {
			gom.NewLt("name", "kmlixh")
		}},
		{"测试NewLike", func(t *testing.T) {
			gom.NewLike("name", "kmlixh")
		}},
		{"测试NewLikeIgnoreStart", func(t *testing.T) {
			gom.NewLikeIgnoreStart("name", "kmlixh")
		}},
		{"测试LikeIgnoreEnd", func(t *testing.T) {
			gom.NewLikeIgnoreEnd("name", "kmlixh")
		}},
		{"测试NewIn", func(t *testing.T) {
			gom.NewIn("name", "kmlixh")
		}},
		{"测试NewNotIn", func(t *testing.T) {
			gom.NewNotIn("name", "kmlixh")
		}},
		{"测试NewIsNull", func(t *testing.T) {
			gom.NewIsNull("name")
		}},
		{"测试NewIsNotNull", func(t *testing.T) {
			gom.NewIsNotNull("name")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}
func TestOperation(t *testing.T) {
	tests := []Tt{
		{"Test cnd function", func(t *testing.T) {
			cnd := gom.New("name", gom.NotEq, "kmlixh").
				And("age", gom.Lt, 12).And2(gom.New("gg", gom.Eq, "ss")).And3("ssdf=?", 23).
				Or("sdfsd", gom.Eq, "sdafs").Or2(gom.NewRaw("age >= ?", 22)).Or3("name like ?", "sadf").OrNotEq("sdf", "sdfsd").
				Eq("name", "j").OrEq("sdfsd", "sdf").NotEq("name", "sdfds").
				Ge("height", 12).OrGe("width", 234).Gt("sdfs", "sdfs").OrGt("sfsdf", "sfdsf").
				Le("asdfa", "sdfds").Lt("sdfds", "sdfsdf").OrLe("sdfss", "sdf").OrLt("asdfs", "asdf").
				Like("sdfas", "sdafsd").LikeIgnoreStart("sfads", "sdfds").LikeIgnoreEnd("asdfasdf", "sdfds").OrLike("sadfas", "sdf").
				OrLike("sdfsd", "sdfds").OrLikeIgnoreStart("sdfsd", "sdfdsf").OrLikeIgnoreEnd("sdfsd", "werwe").
				In("id", "sdf", "sdf", "sdfsd").NotIn("sdfasdf", "sdf").OrIn("asdf", "dfdfd").OrNotIn("safs", "asfsdf").
				IsNull("sadfa").IsNotNull("asdfasd").OrIsNull("safasdf").OrIsNotNull("sadfasdf")
			str, er := db.Factory().ConditionToSql(cnd)
			if str == "" || er == nil {
				t.Error("condition to string failed", er, str)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}
