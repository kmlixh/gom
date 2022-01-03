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
	Linker() Linker
	Field() string
	Operation() Operation
	Values() []interface{}
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
	linker        Linker
	field         string
	operation     Operation
	values        []interface{}
	items         []Condition
	rawExpression string
	enable        bool
}

func (c ConditionImpl) Linker() Linker {
	return c.linker
}
func (c ConditionImpl) Field() string {
	return c.field
}
func (c ConditionImpl) Operation() Operation {
	return c.operation
}
func (c ConditionImpl) Values() []interface{} {
	return c.values
}
func (c ConditionImpl) Items() []Condition {
	return c.items
}
func (c ConditionImpl) HasSubConditions() bool {
	return len(c.items) > 0
}
func (c ConditionImpl) RawExpression() string {
	return c.rawExpression
}
func (c ConditionImpl) IsEnalbe() bool {
	return c.enable
}

func (c ConditionImpl) Eq(field string, values interface{}) Condition {
	condition := CndAnd(field, Eq, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrEq(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Eq, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) Ge(field string, values interface{}) Condition {
	condition := CndAnd(field, Ge, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrGe(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Ge, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) Gt(field string, values interface{}) Condition {
	condition := CndAnd(field, Eq, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrGt(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Gt, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) Le(field string, values interface{}) Condition {
	condition := CndAnd(field, Le, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrLe(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Le, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) Lt(field string, values interface{}) Condition {
	condition := CndAnd(field, Lt, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrLt(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Lt, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) NotEq(field string, values interface{}) Condition {
	condition := CndAnd(field, NotEq, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrNotEq(field string, values interface{}) Condition {
	condition := CndFull(Or, field, NotEq, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) In(field string, values ...interface{}) Condition {
	condition := CndAnd(field, In, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrIn(field string, values ...interface{}) Condition {
	condition := CndFull(Or, field, NotEq, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) NotIn(field string, values ...interface{}) Condition {
	condition := CndAnd(field, NotIn, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrNotIn(field string, values ...interface{}) Condition {
	condition := CndFull(Or, field, NotIn, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) Like(field string, values interface{}) Condition {
	condition := CndAnd(field, Like, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrLike(field string, values interface{}) Condition {
	condition := CndFull(Or, field, Like, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) LikeIgnoreStart(field string, values interface{}) Condition {
	condition := CndAnd(field, LikeIgnoreStart, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrLikeIgnoreStart(field string, values interface{}) Condition {
	condition := CndFull(Or, field, LikeIgnoreStart, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) LikeIgnoreEnd(field string, values interface{}) Condition {
	condition := CndAnd(field, Like, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) IsNull(filed string) Condition {
	condition := CndAnd(filed, IsNull, nil)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) IsNotNull(filed string) Condition {
	condition := CndAnd(filed, IsNotNull, nil)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) OrLikeIgnoreEnd(field string, values interface{}) Condition {
	condition := CndFull(Or, field, LikeIgnoreEnd, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) And1(condition Condition) Condition {
	cc := condition.(ConditionImpl)
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}
func (c ConditionImpl) And(field string, operation Operation, values ...interface{}) Condition {
	cc := CndAnd(field, operation, values).(ConditionImpl)
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}
func (c ConditionImpl) And2(rawExpresssion string, values ...interface{}) Condition {
	condition := CndRaw(rawExpresssion, values)
	c.items = append(c.items, condition)
	return c
}
func (c ConditionImpl) Or(condition Condition) Condition {
	cc := condition.(ConditionImpl)
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func (c ConditionImpl) Or2(rawExpresssion string, values ...interface{}) Condition {
	condition := CndRaw(rawExpresssion, values)
	cc := condition.(ConditionImpl)
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}

func CndAnd(field string, operation Operation, values ...interface{}) Condition {
	return ConditionImpl{
		And,
		field,
		operation,
		values,
		nil,
		"",
		true,
	}
}
func CndOr(field string, operation Operation, values ...interface{}) Condition {
	return ConditionImpl{
		Or,
		field,
		operation,
		values,
		nil,
		"",
		true,
	}
}
func CndRaw(rawExpresssion string, values ...interface{}) Condition {
	return ConditionImpl{linker: And, rawExpression: rawExpresssion, values: values, operation: OperationCustom1}
}
func CndFull(linker Linker, field string, operation Operation, values ...interface{}) Condition {
	return ConditionImpl{
		linker,
		field,
		operation,
		values,
		nil,
		"",
		true,
	}
}
