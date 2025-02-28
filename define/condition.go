package define

import (
	"log"
	"reflect"
)

// Condition represents a WHERE condition
type Condition struct {
	Field      string
	Op         OpType
	Value      interface{} // For normal conditions: single value; for IN/BETWEEN: []interface{}; for raw expressions: []interface{} as args
	JoinType   JoinType
	SubConds   []*Condition // Sub-conditions for nested queries
	IsSubGroup bool         // Whether this is a sub-group of conditions
	IsRawExpr  bool         // Whether this is a raw SQL expression
}

// Conditions represents a slice of Condition
type Conditions []Condition

// NewCondition creates a new condition
func NewCondition(field string, op OpType, value interface{}) *Condition {
	if field == "" {
		return nil
	}
	if value == nil && op != OpIsNull && op != OpIsNotNull {
		return nil
	}
	return &Condition{
		Field: field,
		Op:    op,
		Value: value,
	}
}

// And adds a condition with AND join
func (c *Condition) And(cond *Condition) *Condition {
	cond.JoinType = JoinAnd
	if c.SubConds == nil {
		c.SubConds = make([]*Condition, 0)
	}
	c.SubConds = append(c.SubConds, cond)
	return c
}

// Or adds a condition with OR join
func (c *Condition) Or(cond *Condition) *Condition {
	if cond == nil {
		log.Printf("Warning: nil condition passed to Or() method")
		return c
	}
	cond.JoinType = JoinOr
	if c.SubConds == nil {
		c.SubConds = make([]*Condition, 0)
	}
	c.SubConds = append(c.SubConds, cond)
	return c
}

// Condition builder functions
func Eq(field string, value interface{}) *Condition {
	return NewCondition(field, OpEq, value)
}

func Ne(field string, value interface{}) *Condition {
	return NewCondition(field, OpNe, value)
}

func Gt(field string, value interface{}) *Condition {
	return NewCondition(field, OpGt, value)
}

func Ge(field string, value interface{}) *Condition {
	return NewCondition(field, OpGe, value)
}

func Lt(field string, value interface{}) *Condition {
	return NewCondition(field, OpLt, value)
}

func Le(field string, value interface{}) *Condition {
	return NewCondition(field, OpLe, value)
}

func Like(field string, value interface{}) *Condition {
	return NewCondition(field, OpLike, value)
}

func NotLike(field string, value interface{}) *Condition {
	return NewCondition(field, OpNotLike, value)
}

// flattenValues flattens a slice of values into a slice of interface{}
func flattenValues(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	if len(values) == 0 {
		return []interface{}{}
	}
	result := make([]interface{}, 0)
	for _, v := range values {
		if val := reflect.ValueOf(v); val.Kind() == reflect.Slice {
			for i := 0; i < val.Len(); i++ {
				result = append(result, val.Index(i).Interface())
			}
		} else {
			result = append(result, v)
		}
	}
	return result
}

// In creates an IN condition with variadic parameters that may include arrays
func In(field string, values ...interface{}) *Condition {
	return &Condition{
		Field: field,
		Op:    OpIn,
		Value: flattenValues(values),
	}
}

// NotIn creates a NOT IN condition with variadic parameters that may include arrays
func NotIn(field string, values ...interface{}) *Condition {
	return &Condition{
		Field: field,
		Op:    OpNotIn,
		Value: flattenValues(values),
	}
}

func IsNull(field string) *Condition {
	return NewCondition(field, OpIsNull, nil)
}

func IsNotNull(field string) *Condition {
	return NewCondition(field, OpIsNotNull, nil)
}

func Between(field string, start, end interface{}) *Condition {
	return NewCondition(field, OpBetween, []interface{}{start, end})
}

func NotBetween(field string, start, end interface{}) *Condition {
	return NewCondition(field, OpNotBetween, []interface{}{start, end})
}

// Raw creates a new raw SQL expression condition
func Raw(expr string, args ...interface{}) *Condition {
	return &Condition{
		Field:     expr,
		IsRawExpr: true,
		Value:     args,
	}
}
