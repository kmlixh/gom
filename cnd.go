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
	RawOperation
)

type Condition interface {
	PayLoads() int64 //有效荷载，说明当前条件及其链式条件共有多少有多少个条件
	Linker() Linker
	Field() string
	Operation() Operation
	Values() []interface{}
	SetValues([]interface{})
	Items() []Condition
	HasSubConditions() bool
	RawExpression() string
	Eq(field string, values interface{}) Condition
	EqBool(b bool, field string, value interface{}) Condition
	OrEq(field string, value interface{}) Condition
	OrEqBool(b bool, field string, value interface{}) Condition
	Ge(field string, value interface{}) Condition
	GeBool(b bool, field string, value interface{}) Condition
	OrGe(field string, value interface{}) Condition
	OrGeBool(b bool, field string, value interface{}) Condition
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
	IsNullBool(b bool, field string) Condition
	IsNotNull(field string) Condition
	IsNotNullBool(b bool, field string) Condition
	OrIsNull(filed string) Condition
	OrIsNullBool(b bool, field string) Condition
	OrIsNotNull(field string) Condition
	OrIsNotNullBool(b bool, field string) Condition
	And(field string, operation Operation, value ...interface{}) Condition
	AndBool(b bool, field string, operation Operation, value ...interface{}) Condition
	And2(condition Condition) Condition
	And3(rawExpresssion string, values ...interface{}) Condition
	And3Bool(b bool, rawExpresssion string, values ...interface{}) Condition
	Or(field string, operation Operation, value ...interface{}) Condition
	OrBool(b bool, field string, operation Operation, value ...interface{}) Condition
	Or2(condition Condition) Condition
	Or3(rawExpresssion string, values ...interface{}) Condition
	Or3Bool(b bool, rawExpresssion string, values ...interface{}) Condition
}
type CndImpl struct {
	payloads      int64
	linker        Linker
	field         string
	operation     Operation
	values        []interface{}
	items         []Condition
	rawExpression string
}

func (c *CndImpl) PayLoads() int64 {
	return c.payloads
}

func (c *CndImpl) Linker() Linker {
	return c.linker
}
func (c *CndImpl) Field() string {
	return c.field
}
func (c *CndImpl) Operation() Operation {
	return c.operation
}
func (c *CndImpl) Values() []interface{} {
	return c.values
}
func (c *CndImpl) SetValues(values []interface{}) {
	c.values = values
}
func (c *CndImpl) Items() []Condition {
	return c.items
}
func (c *CndImpl) HasSubConditions() bool {
	return len(c.items) > 0
}
func (c *CndImpl) RawExpression() string {
	return c.rawExpression
}

func (c *CndImpl) Eq(field string, values interface{}) Condition {
	return c.EqBool(true, field, values)
}

func (c *CndImpl) OrEq(field string, values interface{}) Condition {
	return c.OrEqBool(true, field, values)
}

func (c *CndImpl) Ge(field string, values interface{}) Condition {
	return c.GeBool(true, field, values)
}

func (c *CndImpl) OrGe(field string, values interface{}) Condition {
	return c.OrGeBool(true, field, values)
}

func (c *CndImpl) Gt(field string, values interface{}) Condition {
	return c.GtBool(true, field, values)
}

func (c *CndImpl) OrGt(field string, values interface{}) Condition {
	return c.OrGtBool(true, field, values)
}

func (c *CndImpl) Le(field string, values interface{}) Condition {
	return c.LeBool(true, field, values)
}

func (c *CndImpl) OrLe(field string, values interface{}) Condition {
	return c.OrLeBool(true, field, values)
}

func (c *CndImpl) Lt(field string, values interface{}) Condition {
	return c.LtBool(true, field, values)
}

func (c *CndImpl) OrLt(field string, values interface{}) Condition {
	return c.OrLtBool(true, field, values)
}

func (c *CndImpl) NotEq(field string, values interface{}) Condition {
	return c.NotEqBool(true, field, values)
}

func (c *CndImpl) OrNotEq(field string, values interface{}) Condition {
	return c.OrNotEqBool(true, field, values)
}

func (c *CndImpl) In(field string, values ...interface{}) Condition {
	return c.InBool(true, field, values...)
}

func (c *CndImpl) OrIn(field string, values ...interface{}) Condition {
	return c.OrInBool(true, field, values...)

}

func (c *CndImpl) NotIn(field string, values ...interface{}) Condition {
	return c.NotInBool(true, field, values...)
}

func (c *CndImpl) OrNotIn(field string, values ...interface{}) Condition {
	return c.OrNotInBool(true, field, values...)
}

func (c *CndImpl) Like(field string, values interface{}) Condition {
	return c.LikeBool(true, field, values)
}

func (c *CndImpl) OrLike(field string, values interface{}) Condition {
	return c.OrLikeBool(true, field, values)
}

func (c *CndImpl) LikeIgnoreStart(field string, values interface{}) Condition {
	return c.LikeIgnoreStartBool(true, field, values)
}

func (c *CndImpl) OrLikeIgnoreStart(field string, values interface{}) Condition {
	return c.OrLikeIgnoreStartBool(true, field, values)
}

func (c *CndImpl) LikeIgnoreEnd(field string, values interface{}) Condition {
	return c.LikeIgnoreEndBool(true, field, values)
}

func (c *CndImpl) OrLikeIgnoreEnd(field string, values interface{}) Condition {
	return c.OrLikeIgnoreEndBool(true, field, values)
}

func (c *CndImpl) IsNull(filed string) Condition {
	return c.IsNullBool(true, filed)
}

func (c *CndImpl) IsNotNull(field string) Condition {
	return c.IsNotNullBool(true, field)
}

func (c *CndImpl) And(field string, operation Operation, value ...interface{}) Condition {
	return c.AndBool(true, field, operation, value)
}
func (c *CndImpl) And2(condition Condition) Condition {
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) And3(rawExpresssion string, values ...interface{}) Condition {
	return c.And3Bool(true, rawExpresssion, values...)
}

func (c *CndImpl) EqBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Eq, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrEqBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, Eq, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) GeBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Ge, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrGeBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, Ge, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) GtBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Gt, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrGtBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, Gt, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LeBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Le, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLeBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, Le, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LtBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Lt, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLtBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, Lt, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) NotEqBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, NotEq, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrNotEqBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, NotEq, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) InBool(b bool, field string, values ...interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, In, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrInBool(b bool, field string, values ...interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, NotEq, "", values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) NotInBool(b bool, field string, values ...interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, NotIn, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrNotInBool(b bool, field string, values ...interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, NotIn, "", values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Like, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLikeBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, Like, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeIgnoreStartBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, LikeIgnoreStart, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLikeIgnoreStartBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := CndFull(Or, field, LikeIgnoreStart, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeIgnoreEndBool(b bool, field string, values interface{}) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, Like, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) IsNullBool(b bool, field string) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, IsNull, nil)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) IsNotNullBool(b bool, filed string) Condition {
	if !b {
		return c
	}
	condition := Cnd(filed, IsNotNull, nil)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrIsNull(field string) Condition {
	return c.OrIsNullBool(true, field)
}

func (c *CndImpl) OrIsNullBool(b bool, field string) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, IsNull, nil)
	cc := condition.(*CndImpl)
	cc.linker = Or
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrIsNotNull(field string) Condition {
	return c.OrIsNotNullBool(true, field)
}

func (c *CndImpl) OrIsNotNullBool(b bool, field string) Condition {
	if !b {
		return c
	}
	condition := Cnd(field, IsNotNull, nil)
	cc := condition.(*CndImpl)
	cc.linker = Or
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrLikeIgnoreEndBool(b bool, field string, values interface{}) Condition {
	condition := CndFull(Or, field, LikeIgnoreEnd, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) AndBool(b bool, field string, operation Operation, values ...interface{}) Condition {
	if !b {
		return c
	}
	cc := Cnd(field, operation, values...).(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) And3Bool(b bool, rawExpresssion string, values ...interface{}) Condition {
	if !b {
		return c
	}
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = And
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or(field string, operation Operation, values ...interface{}) Condition {
	return c.OrBool(true, field, operation, values...)
}
func (c *CndImpl) OrBool(b bool, field string, operation Operation, values ...interface{}) Condition {
	if !b {
		return c
	}
	cc := Cnd(field, operation, values...).(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or2(condition Condition) Condition {
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or3(rawExpresssion string, values ...interface{}) Condition {
	return c.Or3Bool(true, rawExpresssion, values...)
}
func (c *CndImpl) Or3Bool(b bool, rawExpresssion string, values ...interface{}) Condition {
	if !b {
		return c
	}
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = Or
	c.items = append(c.items, cc)
	return c
}

func CndEq(field string, value interface{}) Condition {
	return Cnd(field, Eq, value)
}
func CndNotEq(field string, value interface{}) Condition {
	return Cnd(field, NotEq, value)
}
func CndGe(field string, value interface{}) Condition {
	return Cnd(field, Ge, value)
}
func CndGt(field string, value interface{}) Condition {
	return Cnd(field, Gt, value)
}
func CndLe(field string, value interface{}) Condition {
	return Cnd(field, Le, value)
}
func CndLt(field string, value interface{}) Condition {
	return Cnd(field, Lt, value)
}
func CndLike(field string, value interface{}) Condition {
	return Cnd(field, Like, value)
}
func CndLikeIgnoreStart(field string, value interface{}) Condition {
	return Cnd(field, LikeIgnoreStart, value)
}
func CndLikeIgnoreEnd(field string, value interface{}) Condition {
	return Cnd(field, LikeIgnoreEnd, value)
}
func CndIn(field string, values ...interface{}) Condition {
	return Cnd(field, In, values...)
}
func CndNotIn(field string, values ...interface{}) Condition {
	return Cnd(field, NotIn, values...)
}
func CndIsNull(field string) Condition {
	return Cnd(field, IsNull)
}
func CndIsNotNull(field string) Condition {
	return Cnd(field, IsNotNull)
}

func Cnd(field string, operation Operation, values ...interface{}) Condition {
	return CndFull(And, field, operation, "", values...)
}

func CndEmpty() Condition {
	return CndRaw("")
}

func CndRaw(rawExpresssion string, values ...interface{}) Condition {
	payloads := int64(1)
	if rawExpresssion == "" {
		payloads = 0
	}
	return &CndImpl{payloads: payloads, linker: And, rawExpression: rawExpresssion, values: values, operation: RawOperation}
}

func CndFull(linker Linker, field string, operation Operation, rawExpression string, values ...interface{}) Condition {
	return &CndImpl{
		1,
		linker,
		field,
		operation,
		values,
		nil,
		rawExpression,
	}
}
