package define

import (
	"testing"
)

func TestCondition(t *testing.T) {
	// Test basic condition creation
	cond := NewCondition("name", OpEq, "John")
	if cond.Field != "name" || cond.Op != OpEq || cond.Value != "John" {
		t.Error("Basic condition creation failed")
	}

	// Test AND condition
	subCond1 := NewCondition("age", OpGt, 18)
	subCond2 := NewCondition("active", OpEq, true)
	cond.And(subCond1).And(subCond2)

	if len(cond.SubConds) != 2 {
		t.Error("Failed to add sub-conditions")
	}

	if cond.SubConds[0].JoinType != JoinAnd {
		t.Error("First sub-condition should have AND join type")
	}

	if cond.SubConds[1].JoinType != JoinAnd {
		t.Error("Second sub-condition should have AND join type")
	}

	// Test OR condition
	cond = NewCondition("name", OpEq, "John")
	subCond1 = NewCondition("age", OpGt, 18)
	subCond2 = NewCondition("active", OpEq, true)
	cond.Or(subCond1).Or(subCond2)

	if len(cond.SubConds) != 2 {
		t.Error("Failed to add OR sub-conditions")
	}

	if cond.SubConds[0].JoinType != JoinOr {
		t.Error("First sub-condition should have OR join type")
	}

	if cond.SubConds[1].JoinType != JoinOr {
		t.Error("Second sub-condition should have OR join type")
	}
}

func TestConditionBuilders(t *testing.T) {
	// Test Eq
	cond := Eq("name", "John")
	if cond.Field != "name" || cond.Op != OpEq || cond.Value != "John" {
		t.Error("Eq builder failed")
	}

	// Test Ne
	cond = Ne("name", "John")
	if cond.Field != "name" || cond.Op != OpNe || cond.Value != "John" {
		t.Error("Ne builder failed")
	}

	// Test Gt
	cond = Gt("age", 18)
	if cond.Field != "age" || cond.Op != OpGt || cond.Value != 18 {
		t.Error("Gt builder failed")
	}

	// Test Ge
	cond = Ge("age", 18)
	if cond.Field != "age" || cond.Op != OpGe || cond.Value != 18 {
		t.Error("Ge builder failed")
	}

	// Test Lt
	cond = Lt("age", 18)
	if cond.Field != "age" || cond.Op != OpLt || cond.Value != 18 {
		t.Error("Lt builder failed")
	}

	// Test Le
	cond = Le("age", 18)
	if cond.Field != "age" || cond.Op != OpLe || cond.Value != 18 {
		t.Error("Le builder failed")
	}

	// Test Like
	cond = Like("name", "%John%")
	if cond.Field != "name" || cond.Op != OpLike || cond.Value != "%John%" {
		t.Error("Like builder failed")
	}

	// Test NotLike
	cond = NotLike("name", "%John%")
	if cond.Field != "name" || cond.Op != OpNotLike || cond.Value != "%John%" {
		t.Error("NotLike builder failed")
	}

	// Test In
	values := []interface{}{1, 2, 3}
	cond = In("id", values...)
	if cond.Field != "id" || cond.Op != OpIn {
		t.Error("In builder failed")
	}

	// Test NotIn
	cond = NotIn("id", values...)
	if cond.Field != "id" || cond.Op != OpNotIn {
		t.Error("NotIn builder failed")
	}

	// Test IsNull
	cond = IsNull("name")
	if cond.Field != "name" || cond.Op != OpIsNull {
		t.Error("IsNull builder failed")
	}

	// Test IsNotNull
	cond = IsNotNull("name")
	if cond.Field != "name" || cond.Op != OpIsNotNull {
		t.Error("IsNotNull builder failed")
	}

	// Test Between
	cond = Between("age", 18, 30)
	if cond.Field != "age" || cond.Op != OpBetween {
		t.Error("Between builder failed")
	}

	// Test NotBetween
	cond = NotBetween("age", 18, 30)
	if cond.Field != "age" || cond.Op != OpNotBetween {
		t.Error("NotBetween builder failed")
	}
}

func TestRawCondition(t *testing.T) {
	// Test raw condition with arguments
	cond := Raw("age > ? AND status = ?", 18, "active")
	if !cond.IsRawExpr {
		t.Error("Expected IsRawExpr to be true")
	}
	if cond.Field != "age > ? AND status = ?" {
		t.Errorf("Expected Field to be 'age > ? AND status = ?', got '%s'", cond.Field)
	}
	if args, ok := cond.Value.([]interface{}); !ok {
		t.Error("Expected Value to be []interface{}")
	} else if len(args) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(args))
	}

	// Test raw condition without arguments
	cond = Raw("status IS NOT NULL")
	if !cond.IsRawExpr {
		t.Error("Expected IsRawExpr to be true")
	}
	if cond.Field != "status IS NOT NULL" {
		t.Errorf("Expected Field to be 'status IS NOT NULL', got '%s'", cond.Field)
	}
	if args, ok := cond.Value.([]interface{}); !ok {
		t.Error("Expected Value to be []interface{}")
	} else if len(args) != 0 {
		t.Errorf("Expected 0 arguments, got %d", len(args))
	}
}
