package gom

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
	OperationCustom1
	OperationCustom2
	OperationCustom3
	OperationCustom4
	OperationCustom5
	OperationCustom6
	OperationCustom7
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
	IsEnalbe() bool
	Eq(field string, values interface{}) Condition
	OrEq(field string, values interface{}) Condition
	Ge(field string, values interface{}) Condition
	OrGe(field string, values interface{}) Condition
	Gt(field string, values interface{}) Condition
	OrGt(field string, values interface{}) Condition
	Le(field string, values interface{}) Condition
	OrLe(field string, values interface{}) Condition
	Lt(field string, values interface{}) Condition
	OrLt(field string, values interface{}) Condition
	NotEq(field string, values interface{}) Condition
	OrNotEq(field string, values interface{}) Condition
	In(field string, values ...interface{}) Condition
	OrIn(field string, values ...interface{}) Condition
	NotIn(field string, values ...interface{}) Condition
	OrNotIn(field string, values ...interface{}) Condition
	Like(field string, values interface{}) Condition
	OrLike(field string, values interface{}) Condition
	LikeIgnoreStart(field string, values interface{}) Condition
	OrLikeIgnoreStart(field string, values interface{}) Condition
	LikeIgnoreEnd(field string, values interface{}) Condition
	OrLikeIgnoreEnd(field string, values interface{}) Condition
	IsNull(filed string) Condition
	IsNotNull(field string) Condition
	And(field string, operation Operation, value ...interface{}) Condition
	And1(condition Condition) Condition
	Or(condition Condition) Condition
	And2(rawExpresssion string, values ...interface{}) Condition
	Or2(rawExpresssion string, values ...interface{}) Condition
}
type ConditionImpl struct {
	depth         int64
	linker        Linker
	field         string
	operation     Operation
	values        []interface{}
	items         []Condition
	rawExpression string
	enable        bool
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
func (c *ConditionImpl) IsEnalbe() bool {
	return c.enable
}

func (c *ConditionImpl) Eq(field string, values interface{}) Condition {
	condition := Cnd(field, Eq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrEq(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Eq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Ge(field string, values interface{}) Condition {
	condition := Cnd(field, Ge, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrGe(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Ge, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Gt(field string, values interface{}) Condition {
	condition := Cnd(field, Eq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrGt(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Gt, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Le(field string, values interface{}) Condition {
	condition := Cnd(field, Le, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLe(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Le, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Lt(field string, values interface{}) Condition {
	condition := Cnd(field, Lt, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLt(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Lt, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) NotEq(field string, values interface{}) Condition {
	condition := Cnd(field, NotEq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrNotEq(field string, values interface{}) Condition {
	condition := CndFull(Or, field, NotEq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) In(field string, values ...interface{}) Condition {
	condition := Cnd(field, In, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrIn(field string, values ...interface{}) Condition {
	condition := CndFull(Or, field, NotEq, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) NotIn(field string, values ...interface{}) Condition {
	condition := Cnd(field, NotIn, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrNotIn(field string, values ...interface{}) Condition {
	condition := CndFull(Or, field, NotIn, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) Like(field string, values interface{}) Condition {
	condition := Cnd(field, Like, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLike(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Like, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LikeIgnoreStart(field string, values interface{}) Condition {
	condition := Cnd(field, LikeIgnoreStart, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLikeIgnoreStart(field string, values interface{}) Condition {
	condition := CndFull(Or, field, LikeIgnoreStart, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) LikeIgnoreEnd(field string, values interface{}) Condition {
	condition := Cnd(field, Like, values)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) IsNull(filed string) Condition {
	condition := Cnd(filed, IsNull, nil)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) IsNotNull(filed string) Condition {
	condition := Cnd(filed, IsNotNull, nil)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) OrLikeIgnoreEnd(field string, values interface{}) Condition {
	condition := CndFull(Or, field, LikeIgnoreEnd, values)
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
func (c *ConditionImpl) And(field string, operation Operation, values ...interface{}) Condition {
	cc := Cnd(field, operation, values...).(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}
func (c *ConditionImpl) And2(rawExpresssion string, values ...interface{}) Condition {
	condition := CndRaw(rawExpresssion, values...)
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
func (c *ConditionImpl) Or2(rawExpresssion string, values ...interface{}) Condition {
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*ConditionImpl)
	cc.depth = c.depth + 1
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func CndEq(field string, values interface{}) Condition {
	return &ConditionImpl{
		0,
		And,
		field,
		Eq,
		[]interface{}{values},
		nil,
		"",
		true,
	}
}
func CndNotEq(field string, values interface{}) Condition {
	return &ConditionImpl{
		0,
		And,
		field,
		NotEq,
		[]interface{}{values},
		nil,
		"",
		true,
	}
}

func Cnd(field string, operation Operation, values ...interface{}) Condition {
	return &ConditionImpl{
		0,
		And,
		field,
		operation,
		values,
		nil,
		"",
		true,
	}
}
func CndRaw(rawExpresssion string, values ...interface{}) Condition {
	return &ConditionImpl{depth: 0, linker: And, rawExpression: rawExpresssion, values: values, operation: OperationCustom1, enable: true}
}
func CndFull(linker Linker, field string, operation Operation, values ...interface{}) Condition {
	return &ConditionImpl{
		0,
		linker,
		field,
		operation,
		values,
		nil,
		"",
		true,
	}
}
