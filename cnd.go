package gom

import "github.com/kmlixh/gom/v4/define"

type CndImpl struct {
	payloads      int64
	linker        define.Linker
	field         string
	operation     define.Operation
	values        []interface{}
	items         []define.Condition
	rawExpression string
}

func (c *CndImpl) PayLoads() int64 {
	return c.payloads
}

func (c *CndImpl) Linker() define.Linker {
	return c.linker
}
func (c *CndImpl) Field() string {
	return c.field
}
func (c *CndImpl) Operation() define.Operation {
	return c.operation
}
func (c *CndImpl) Values() []interface{} {
	return c.values
}
func (c *CndImpl) SetValues(values []interface{}) {
	c.values = values
}
func (c *CndImpl) Items() []define.Condition {
	return c.items
}
func (c *CndImpl) HasSubConditions() bool {
	return len(c.items) > 0
}
func (c *CndImpl) RawExpression() string {
	return c.rawExpression
}

func (c *CndImpl) Eq(field string, values interface{}) define.Condition {
	return c.EqBool(true, field, values)
}

func (c *CndImpl) OrEq(field string, values interface{}) define.Condition {
	return c.OrEqBool(true, field, values)
}

func (c *CndImpl) Ge(field string, values interface{}) define.Condition {
	return c.GeBool(true, field, values)
}

func (c *CndImpl) OrGe(field string, values interface{}) define.Condition {
	return c.OrGeBool(true, field, values)
}

func (c *CndImpl) Gt(field string, values interface{}) define.Condition {
	return c.GtBool(true, field, values)
}

func (c *CndImpl) OrGt(field string, values interface{}) define.Condition {
	return c.OrGtBool(true, field, values)
}

func (c *CndImpl) Le(field string, values interface{}) define.Condition {
	return c.LeBool(true, field, values)
}

func (c *CndImpl) OrLe(field string, values interface{}) define.Condition {
	return c.OrLeBool(true, field, values)
}

func (c *CndImpl) Lt(field string, values interface{}) define.Condition {
	return c.LtBool(true, field, values)
}

func (c *CndImpl) OrLt(field string, values interface{}) define.Condition {
	return c.OrLtBool(true, field, values)
}

func (c *CndImpl) NotEq(field string, values interface{}) define.Condition {
	return c.NotEqBool(true, field, values)
}

func (c *CndImpl) OrNotEq(field string, values interface{}) define.Condition {
	return c.OrNotEqBool(true, field, values)
}

func (c *CndImpl) In(field string, values ...interface{}) define.Condition {
	return c.InBool(true, field, values...)
}

func (c *CndImpl) OrIn(field string, values ...interface{}) define.Condition {
	return c.OrInBool(true, field, values...)

}

func (c *CndImpl) NotIn(field string, values ...interface{}) define.Condition {
	return c.NotInBool(true, field, values...)
}

func (c *CndImpl) OrNotIn(field string, values ...interface{}) define.Condition {
	return c.OrNotInBool(true, field, values...)
}

func (c *CndImpl) NotLike(field string, values interface{}) define.Condition {
	return c.NotLikeBool(true, field, values)

}

func (c *CndImpl) NotLikeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.NotLike, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrNotLike(field string, values interface{}) define.Condition {
	return c.OrNotLikeBool(true, field, values)
}

func (c *CndImpl) OrNotLikeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.NotLike, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Like(field string, values interface{}) define.Condition {
	return c.LikeBool(true, field, values)
}

func (c *CndImpl) OrLike(field string, values interface{}) define.Condition {
	return c.OrLikeBool(true, field, values)
}

func (c *CndImpl) LikeIgnoreStart(field string, values interface{}) define.Condition {
	return c.LikeIgnoreStartBool(true, field, values)
}

func (c *CndImpl) OrLikeIgnoreStart(field string, values interface{}) define.Condition {
	return c.OrLikeIgnoreStartBool(true, field, values)
}

func (c *CndImpl) LikeIgnoreEnd(field string, values interface{}) define.Condition {
	return c.LikeIgnoreEndBool(true, field, values)
}

func (c *CndImpl) OrLikeIgnoreEnd(field string, values interface{}) define.Condition {
	return c.OrLikeIgnoreEndBool(true, field, values)
}

func (c *CndImpl) IsNull(filed string) define.Condition {
	return c.IsNullBool(true, filed)
}

func (c *CndImpl) IsNotNull(field string) define.Condition {
	return c.IsNotNullBool(true, field)
}

func (c *CndImpl) And(field string, operation define.Operation, value ...interface{}) define.Condition {
	return c.AndBool(true, field, operation, value)
}
func (c *CndImpl) And2(condition define.Condition) define.Condition {
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = define.And
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) And3(rawExpresssion string, values ...interface{}) define.Condition {
	return c.And3Bool(true, rawExpresssion, values...)
}

func (c *CndImpl) EqBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Eq, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrEqBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.Eq, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) GeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Ge, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrGeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.Ge, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) GtBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Gt, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrGtBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.Gt, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Le, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.Le, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LtBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Lt, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLtBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.Lt, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) NotEqBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.NotEq, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrNotEqBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.NotEq, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) InBool(b bool, field string, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.In, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrInBool(b bool, field string, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.NotEq, "", values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) NotInBool(b bool, field string, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.NotIn, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrNotInBool(b bool, field string, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.NotIn, "", values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Like, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLikeBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.Like, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeIgnoreStartBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.LikeIgnoreStart, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLikeIgnoreStartBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndFull(define.Or, field, define.LikeIgnoreStart, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeIgnoreEndBool(b bool, field string, values interface{}) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.Like, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) IsNullBool(b bool, field string) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.IsNull, nil)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) IsNotNullBool(b bool, filed string) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(filed, define.IsNotNull, nil)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrIsNull(field string) define.Condition {
	return c.OrIsNullBool(true, field)
}

func (c *CndImpl) OrIsNullBool(b bool, field string) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.IsNull, nil)
	cc := condition.(*CndImpl)
	cc.linker = define.Or
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrIsNotNull(field string) define.Condition {
	return c.OrIsNotNullBool(true, field)
}

func (c *CndImpl) OrIsNotNullBool(b bool, field string) define.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, define.IsNotNull, nil)
	cc := condition.(*CndImpl)
	cc.linker = define.Or
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrLikeIgnoreEndBool(b bool, field string, values interface{}) define.Condition {
	condition := CndFull(define.Or, field, define.LikeIgnoreEnd, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) AndBool(b bool, field string, operation define.Operation, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	cc := Cnd(field, operation, values...).(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = define.And
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) And3Bool(b bool, rawExpresssion string, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = define.And
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or(field string, operation define.Operation, values ...interface{}) define.Condition {
	return c.OrBool(true, field, operation, values...)
}
func (c *CndImpl) OrBool(b bool, field string, operation define.Operation, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	cc := Cnd(field, operation, values...).(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = define.Or
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or2(condition define.Condition) define.Condition {
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = define.Or
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or3(rawExpresssion string, values ...interface{}) define.Condition {
	return c.Or3Bool(true, rawExpresssion, values...)
}
func (c *CndImpl) Or3Bool(b bool, rawExpresssion string, values ...interface{}) define.Condition {
	if !b {
		return c
	}
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = define.Or
	c.items = append(c.items, cc)
	return c
}

func CndEq(field string, value interface{}) define.Condition {
	return Cnd(field, define.Eq, value)
}
func CndNotEq(field string, value interface{}) define.Condition {
	return Cnd(field, define.NotEq, value)
}
func CndGe(field string, value interface{}) define.Condition {
	return Cnd(field, define.Ge, value)
}
func CndGt(field string, value interface{}) define.Condition {
	return Cnd(field, define.Gt, value)
}
func CndLe(field string, value interface{}) define.Condition {
	return Cnd(field, define.Le, value)
}
func CndLt(field string, value interface{}) define.Condition {
	return Cnd(field, define.Lt, value)
}
func CndLike(field string, value interface{}) define.Condition {
	return Cnd(field, define.Like, value)
}
func CndLikeIgnoreStart(field string, value interface{}) define.Condition {
	return Cnd(field, define.LikeIgnoreStart, value)
}
func CndLikeIgnoreEnd(field string, value interface{}) define.Condition {
	return Cnd(field, define.LikeIgnoreEnd, value)
}
func CndIn(field string, values ...interface{}) define.Condition {
	return Cnd(field, define.In, values...)
}
func CndNotIn(field string, values ...interface{}) define.Condition {
	return Cnd(field, define.NotIn, values...)
}
func CndIsNull(field string) define.Condition {
	return Cnd(field, define.IsNull)
}
func CndIsNotNull(field string) define.Condition {
	return Cnd(field, define.IsNotNull)
}

func Cnd(field string, operation define.Operation, values ...interface{}) define.Condition {
	return CndFull(define.And, field, operation, "", values...)
}

func CndEmpty() define.Condition {
	return CndRaw("")
}

func CndRaw(rawExpresssion string, values ...interface{}) define.Condition {
	payloads := int64(1)
	if rawExpresssion == "" {
		payloads = 0
	}
	return &CndImpl{payloads: payloads, linker: define.And, rawExpression: rawExpresssion, values: UnZipSlice(values), operation: define.RawOperation}
}

func CndFull(linker define.Linker, field string, operation define.Operation, rawExpression string, values ...interface{}) define.Condition {
	return &CndImpl{
		1,
		linker,
		field,
		operation,
		UnZipSlice(values),
		nil,
		rawExpression,
	}
}
