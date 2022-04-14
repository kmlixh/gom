package gom

import (
	"gitee.com/janyees/gom/cnds"
	"testing"
)

func init() {
}
func TestConditions(t *testing.T) {

	tests := []Tt{
		{"测试Raw创建", func(t *testing.T) {
			cnds.NewRaw("name = ?", "kmlixh")
		}},
		{"测试Full", func(t *testing.T) {
			cnds.NewFull(true, cnds.And, "name", cnds.NotEq, "", "kmlixh")
		}},
		{"测试New", func(t *testing.T) { cnds.New("name", cnds.NotEq, "kmlixh") }},

		{"测试NewEq", func(t *testing.T) {
			cnds.NewEq("name", "kmlixh")
		}},
		{"测试NewNotEq", func(t *testing.T) {
			cnds.NewNotEq("name", "kmlixh")
		}},
		{"测试NewGe", func(t *testing.T) {
			cnds.NewGe("name", "kmlixh")
		}},
		{"测试NewGt", func(t *testing.T) {
			cnds.NewGt("name", "kmlixh")
		}},
		{"测试NewLe", func(t *testing.T) {
			cnds.NewLe("name", "kmlixh")
		}},
		{"测试NewLt", func(t *testing.T) {
			cnds.NewLt("name", "kmlixh")
		}},
		{"测试NewLike", func(t *testing.T) {
			cnds.NewLike("name", "kmlixh")
		}},
		{"测试NewLikeIgnoreStart", func(t *testing.T) {
			cnds.NewLikeIgnoreStart("name", "kmlixh")
		}},
		{"测试LikeIgnoreEnd", func(t *testing.T) {
			cnds.NewLikeIgnoreEnd("name", "kmlixh")
		}},
		{"测试NewIn", func(t *testing.T) {
			cnds.NewIn("name", "kmlixh")
		}},
		{"测试NewNotIn", func(t *testing.T) {
			cnds.NewNotIn("name", "kmlixh")
		}},
		{"测试NewIsNull", func(t *testing.T) {
			cnds.NewIsNull("name")
		}},
		{"测试NewIsNotNull", func(t *testing.T) {
			cnds.NewIsNotNull("name")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}
func TestOperation(t *testing.T) {
	tests := []Tt{
		{"Test cnd function", func(t *testing.T) {
			cnd := cnds.New("name", cnds.NotEq, "kmlixh").
				And("age", cnds.Lt, 12).And2(cnds.New("gg", cnds.Eq, "ss")).And3("ssdf=?", 23).
				Or("sdfsd", cnds.Eq, "sdafs").Or2(cnds.NewRaw("age >= ?", 22)).Or3("name like ?", "sadf").OrNotEq("sdf", "sdfsd").
				Eq("name", "j").OrEq("sdfsd", "sdf").NotEq("name", "sdfds").
				Ge("height", 12).OrGe("width", 234).Gt("sdfs", "sdfs").OrGt("sfsdf", "sfdsf").
				Le("asdfa", "sdfds").Lt("sdfds", "sdfsdf").OrLe("sdfss", "sdf").OrLt("asdfs", "asdf").
				Like("sdfas", "sdafsd").LikeIgnoreStart("sfads", "sdfds").LikeIgnoreEnd("asdfasdf", "sdfds").OrLike("sadfas", "sdf").
				OrLike("sdfsd", "sdfds").OrLikeIgnoreStart("sdfsd", "sdfdsf").OrLikeIgnoreEnd("sdfsd", "werwe").
				In("id", "sdf", "sdf", "sdfsd").NotIn("sdfasdf", "sdf").OrIn("asdf", "dfdfd").OrNotIn("safs", "asfsdf").
				IsNull("sadfa").IsNotNull("asdfasd").OrIsNull("safasdf").OrIsNotNull("sadfasdf")
			str, er := db.factory.ConditionToSql(cnd)
			if str == "" || er == nil {
				t.Error("condition to string failed", er, str)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.t)
	}
}
