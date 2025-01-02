package define

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConditionBasics(t *testing.T) {
	// Test NewCondition
	cond := NewCondition("field", OpEq, "value")
	assert.Equal(t, "field", cond.Field)
	assert.Equal(t, OpEq, cond.Op)
	assert.Equal(t, "value", cond.Value)
	assert.Empty(t, cond.SubConds)
	assert.False(t, cond.IsSubGroup)

	// Test And
	subCond := NewCondition("sub_field", OpGt, 10)
	cond.And(subCond)
	assert.Len(t, cond.SubConds, 1)
	assert.Equal(t, JoinAnd, cond.SubConds[0].Join)

	// Test Or
	orCond := NewCondition("or_field", OpLt, 20)
	cond.Or(orCond)
	assert.Len(t, cond.SubConds, 2)
	assert.Equal(t, JoinOr, cond.SubConds[1].Join)

	// Test Or with nil
	result := cond.Or(nil)
	assert.Equal(t, cond, result)
}

func TestConditionBuilders(t *testing.T) {
	// Test Eq
	eq := Eq("field", "value")
	assert.Equal(t, "field", eq.Field)
	assert.Equal(t, OpEq, eq.Op)
	assert.Equal(t, "value", eq.Value)

	// Test Ne
	ne := Ne("field", "value")
	assert.Equal(t, OpNe, ne.Op)

	// Test Gt
	gt := Gt("field", 10)
	assert.Equal(t, OpGt, gt.Op)

	// Test Ge
	ge := Ge("field", 10)
	assert.Equal(t, OpGe, ge.Op)

	// Test Lt
	lt := Lt("field", 10)
	assert.Equal(t, OpLt, lt.Op)

	// Test Le
	le := Le("field", 10)
	assert.Equal(t, OpLe, le.Op)

	// Test Like
	like := Like("field", "%value%")
	assert.Equal(t, OpLike, like.Op)

	// Test NotLike
	notLike := NotLike("field", "%value%")
	assert.Equal(t, OpNotLike, notLike.Op)
}

func TestConditionInOperators(t *testing.T) {
	// Test In with slice
	values := []interface{}{1, 2, 3}
	in := In("field", values...)
	assert.Equal(t, OpIn, in.Op)
	assert.Equal(t, values, in.Value)

	// Test In with array
	inArray := In("field", []int{1, 2, 3})
	assert.Equal(t, OpIn, inArray.Op)
	assert.Len(t, inArray.Value.([]interface{}), 3)

	// Test NotIn
	notIn := NotIn("field", values...)
	assert.Equal(t, OpNotIn, notIn.Op)
	assert.Equal(t, values, notIn.Value)
}

func TestConditionNullOperators(t *testing.T) {
	// Test IsNull
	isNull := IsNull("field")
	assert.Equal(t, OpIsNull, isNull.Op)
	assert.Nil(t, isNull.Value)

	// Test IsNotNull
	isNotNull := IsNotNull("field")
	assert.Equal(t, OpIsNotNull, isNotNull.Op)
	assert.Nil(t, isNotNull.Value)
}

func TestConditionBetweenOperators(t *testing.T) {
	// Test Between
	between := Between("field", 1, 10)
	assert.Equal(t, OpBetween, between.Op)
	assert.Equal(t, []interface{}{1, 10}, between.Value)

	// Test NotBetween
	notBetween := NotBetween("field", 1, 10)
	assert.Equal(t, OpNotBetween, notBetween.Op)
	assert.Equal(t, []interface{}{1, 10}, notBetween.Value)
}

func TestComplexConditions(t *testing.T) {
	// Create a complex condition: (age > 18 AND role = 'admin') OR (status = 'active' AND level >= 5)
	cond := Gt("age", 18).
		And(Eq("role", "admin")).
		Or(Eq("status", "active").
			And(Ge("level", 5)))

	// Verify structure
	assert.Equal(t, "age", cond.Field)
	assert.Equal(t, OpGt, cond.Op)
	assert.Equal(t, 18, cond.Value)

	assert.Len(t, cond.SubConds, 2)
	assert.Equal(t, JoinAnd, cond.SubConds[0].Join)
	assert.Equal(t, JoinOr, cond.SubConds[1].Join)

	// Verify first AND condition
	andCond := cond.SubConds[0]
	assert.Equal(t, "role", andCond.Field)
	assert.Equal(t, OpEq, andCond.Op)
	assert.Equal(t, "admin", andCond.Value)

	// Verify OR condition and its AND subcondition
	orCond := cond.SubConds[1]
	assert.Equal(t, "status", orCond.Field)
	assert.Equal(t, OpEq, orCond.Op)
	assert.Equal(t, "active", orCond.Value)

	assert.Len(t, orCond.SubConds, 1)
	assert.Equal(t, JoinAnd, orCond.SubConds[0].Join)
	assert.Equal(t, "level", orCond.SubConds[0].Field)
	assert.Equal(t, OpGe, orCond.SubConds[0].Op)
	assert.Equal(t, 5, orCond.SubConds[0].Value)
}

func TestFlattenValues(t *testing.T) {
	// Test nil input
	assert.Nil(t, flattenValues(nil))

	// Test empty slice
	assert.Empty(t, flattenValues([]interface{}{}))

	// Test simple values
	values := []interface{}{1, "two", 3.0}
	flattened := flattenValues(values)
	assert.Equal(t, values, flattened)

	// Test nested slice
	nested := []interface{}{[]int{1, 2, 3}, "four", []string{"five", "six"}}
	flattened = flattenValues(nested)
	assert.Len(t, flattened, 6)
	assert.Contains(t, flattened, 1)
	assert.Contains(t, flattened, "four")
	assert.Contains(t, flattened, "five")
}
