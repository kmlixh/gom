package structs

type Linker int

const (
	_ Linker = iota
	And
	Or
)

type Operation int

const (
	_ Operation = iota
	Eq
	NotEq
	Ge
	Gt
	Le
	Lt
	Like
	LikeIgnoreStart
	LikeIgnoreEnd
	In
	NotIn
	IsNull
	IsNotNull
	Raw
)

type Condition interface {
	Depth() int64
	Linker() Linker
	Field() string
	Operation() Operation
	Values() []interface{}
	SetValues([]interface{})
	Items() []Condition
	HasSubConditions() bool
	RawExpression() string
	Valid() bool
	Eq(field string, values interface{}) Condition
	EqBool(b bool, field string, values interface{}) Condition
	OrEq(field string, values interface{}) Condition
	OrEqBool(b bool, field string, values interface{}) Condition
	Ge(field string, values interface{}) Condition
	GeBool(b bool, field string, values interface{}) Condition
	OrGe(field string, values interface{}) Condition
	OrGeBool(b bool, field string, values interface{}) Condition
	Gt(field string, values interface{}) Condition
	GtBool(b bool, field string, values interface{}) Condition
	OrGt(field string, values interface{}) Condition
	OrGtBool(b bool, field string, values interface{}) Condition
	Le(field string, values interface{}) Condition
	LeBool(b bool, field string, values interface{}) Condition
	OrLe(field string, values interface{}) Condition
	OrLeBool(b bool, field string, values interface{}) Condition
	Lt(field string, values interface{}) Condition
	LtBool(b bool, field string, values interface{}) Condition
	OrLt(field string, values interface{}) Condition
	OrLtBool(b bool, field string, values interface{}) Condition
	NotEq(field string, values interface{}) Condition
	NotEqBool(b bool, field string, values interface{}) Condition
	OrNotEq(field string, values interface{}) Condition
	OrNotEqBool(b bool, field string, values interface{}) Condition
	In(field string, values ...interface{}) Condition
	InBool(b bool, field string, values ...interface{}) Condition
	OrIn(field string, values ...interface{}) Condition
	OrInBool(b bool, field string, values ...interface{}) Condition
	NotIn(field string, values ...interface{}) Condition
	NotInBool(b bool, field string, values ...interface{}) Condition
	OrNotIn(field string, values ...interface{}) Condition
	OrNotInBool(b bool, field string, values ...interface{}) Condition
	Like(field string, values interface{}) Condition
	LikeBool(b bool, field string, values interface{}) Condition
	OrLike(field string, values interface{}) Condition
	OrLikeBool(b bool, field string, values interface{}) Condition
	LikeIgnoreStart(field string, values interface{}) Condition
	LikeIgnoreStartBool(b bool, field string, values interface{}) Condition
	OrLikeIgnoreStart(field string, values interface{}) Condition
	OrLikeIgnoreStartBool(b bool, field string, values interface{}) Condition
	LikeIgnoreEnd(field string, values interface{}) Condition
	LikeIgnoreEndBool(b bool, field string, values interface{}) Condition
	OrLikeIgnoreEnd(field string, values interface{}) Condition
	OrLikeIgnoreEndBool(b bool, field string, values interface{}) Condition
	IsNull(filed string) Condition
	IsNullBool(b bool, filed string) Condition
	IsNotNull(field string) Condition
	IsNotNullBool(b bool, field string) Condition
	And(field string, operation Operation, value ...interface{}) Condition
	AndBool(b bool, field string, operation Operation, value ...interface{}) Condition
	And1(condition Condition) Condition
	Or(condition Condition) Condition
	And2(rawExpresssion string, values ...interface{}) Condition
	And2Bool(b bool, rawExpresssion string, values ...interface{}) Condition
	Or2(rawExpresssion string, values ...interface{}) Condition
	Or2Bool(b bool, rawExpresssion string, values ...interface{}) Condition
}
type ConditionImpl struct {
	depth         int64
	linker        Linker
	field         string
	operation     Operation
	values        []interface{}
	items         []Condition
	rawExpression string
	valid         bool
}

func (c *ConditionImpl) Depth() int64 {
	return c.depth
}

func (c *ConditionImpl) Linker() Linker {
	return c.linker
}
func (c *ConditionImpl) Field() string {
	return c.field
}
func (c *ConditionImpl) Operation() Operation {
	return c.operation
}
func (c *ConditionImpl) Values() []interface{} {
	return c.values
}
func (c *ConditionImpl) SetValues(values []interface{}) {
	c.values = values
}
func (c *ConditionImpl) Items() []Condition {
	return c.items
}
func (c *ConditionImpl) HasSubConditions() bool {
	return len(c.items) > 0
}
func (c *ConditionImpl) RawExpression() string {
	return c.rawExpression
}
func (c *ConditionImpl) Valid() bool {
	return c.valid
}

func (c *ConditionImpl) Eq(field string, values interface{}) Condition {
	return c.EqBool(true, field, values)
}

func (c *ConditionImpl) OrEq(field string, values interface{}) Condition {
	return c.OrEqBool(true, field, values)
}

func (c *ConditionImpl) Ge(field string, values interface{}) Condition {
	return c.GeBool(true, field, values)
}

func (c *ConditionImpl) OrGe(field string, values interface{}) Condition {
	return c.OrGeBool(true, field, values)
}

func (c *ConditionImpl) Gt(field string, values interface{}) Condition {
	return c.GtBool(true, field, values)
}

func (c *ConditionImpl) OrGt(field string, values interface{}) Condition {
	return c.OrGtBool(true, field, values)
}

func (c *ConditionImpl) Le(field string, values interface{}) Condition {
	return c.LeBool(true, field, values)
}

func (c *ConditionImpl) OrLe(field string, values interface{}) Condition {
	return c.OrLeBool(true, field, values)
}

func (c *ConditionImpl) Lt(field string, values interface{}) Condition {
	return c.LtBool(true, field, values)
}

func (c *ConditionImpl) OrLt(field string, values interface{}) Condition {
	return c.OrLtBool(true, field, values)
}

func (c *ConditionImpl) NotEq(field string, values interface{}) Condition {
	return c.NotEqBool(true, field, values)
}

func (c *ConditionImpl) OrNotEq(field string, values interface{}) Condition {
	return c.OrNotEqBool(true, field, values)
}

func (c *ConditionImpl) In(field string, values ...interface{}) Condition {
	return c.InBool(true, field, values...)
}

func (c *ConditionImpl) OrIn(field string, values ...interface{}) Condition {
	return c.OrInBool(true, field, values...)

}

func (c *ConditionImpl) NotIn(field string, values ...interface{}) Condition {
	return c.NotInBool(true, field, values...)
}

func (c *ConditionImpl) OrNotIn(field string, values ...interface{}) Condition {
	return c.OrNotInBool(true, field, values...)
}

func (c *ConditionImpl) Like(field string, values interface{}) Condition {
	return c.LikeBool(true, field, values)
}

func (c *ConditionImpl) OrLike(field string, values interface{}) Condition {
	return c.OrLikeBool(true, field, values)
}

func (c *ConditionImpl) LikeIgnoreStart(field string, values interface{}) Condition {
	return c.LikeIgnoreStartBool(true, field, values)
}

func (c *ConditionImpl) OrLikeIgnoreStart(field string, values interface{}) Condition {
	return c.OrLikeIgnoreStartBool(true, field, values)
}

func (c *ConditionImpl) LikeIgnoreEnd(field string, values interface{}) Condition {
	return c.LikeIgnoreEndBool(true, field, values)
}

func (c *ConditionImpl) OrLikeIgnoreEnd(field string, values interface{}) Condition {
	return c.OrLikeIgnoreEndBool(true, field, values)
}

func (c *ConditionImpl) IsNull(filed string) Condition {
	return c.IsNullBool(true, filed)
}

func (c *ConditionImpl) IsNotNull(field string) Condition {
	return c.IsNotNullBool(true, field)
}

func (c *ConditionImpl) And(field string, operation Operation, value ...interface{}) Condition {
	return c.AndBool(true, field, operation, value...)
}

func (c *ConditionImpl) And2(rawExpresssion string, values ...interface{}) Condition {
	return c.And2Bool(true, rawExpresssion, values...)
}

func (c *ConditionImpl) EqBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Eq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrEqBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, Eq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) GeBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Ge, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrGeBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, Ge, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) GtBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Eq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrGtBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, Gt, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LeBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Le, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLeBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, Le, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LtBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Lt, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLtBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, Lt, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) NotEqBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, NotEq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrNotEqBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, NotEq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) InBool(b bool, field string, values ...interface{}) Condition {
	condition := CndBool(b, field, In, UnZipSlice(values)...)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrInBool(b bool, field string, values ...interface{}) Condition {
	condition := CndFull(b, Or, field, NotEq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) NotInBool(b bool, field string, values ...interface{}) Condition {
	condition := CndBool(b, field, NotIn, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrNotInBool(b bool, field string, values ...interface{}) Condition {
	condition := CndFull(b, Or, field, NotIn, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LikeBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Like, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLikeBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, Like, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LikeIgnoreStartBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, LikeIgnoreStart, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLikeIgnoreStartBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, LikeIgnoreStart, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LikeIgnoreEndBool(b bool, field string, values interface{}) Condition {
	condition := CndBool(b, field, Like, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) IsNullBool(b bool, filed string) Condition {
	condition := CndBool(b, filed, IsNull, nil)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) IsNotNullBool(b bool, filed string) Condition {
	condition := CndBool(b, filed, IsNotNull, nil)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLikeIgnoreEndBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(b, Or, field, LikeIgnoreEnd, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) And1(condition Condition) Condition {
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) AndBool(b bool, field string, operation Operation, values ...interface{}) Condition {
	cc := CndBool(b, field, operation, values...).(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}

func (c *ConditionImpl) And2Bool(b bool, rawExpresssion string, values ...interface{}) Condition {
	condition := CndRawBool(b, rawExpresssion, values...)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Or(condition Condition) Condition {
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Or2Bool(b bool, rawExpresssion string, values ...interface{}) Condition {
	condition := CndRawBool(b, rawExpresssion, values...)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Or2(rawExpresssion string, values ...interface{}) Condition {
	return c.Or2Bool(true, rawExpresssion, values...)
}

func CndBool(b bool, field string, operation Operation, values ...interface{}) Condition {
	return &ConditionImpl{
		0,
		And,
		field,
		operation,
		values,
		nil,
		"",
		b,
	}
}
func Cnd(field string, operation Operation, values ...interface{}) Condition {
	return CndBool(true, field, operation, values...)
}
func CndRaw(rawExpresssion string, values ...interface{}) Condition {
	return CndRawBool(true, rawExpresssion, values...)
}
func CndRawBool(b bool, rawExpresssion string, values ...interface{}) Condition {
	return &ConditionImpl{depth: 0, linker: And, rawExpression: rawExpresssion, values: values, operation: Raw, valid: b}
}
func CndFull(b bool, linker Linker, field string, operation Operation, values ...interface{}) Condition {
	return &ConditionImpl{
		0,
		linker,
		field,
		operation,
		values,
		nil,
		"",
		b,
	}
}
