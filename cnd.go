package gom

import "github.com/kmlixh/gom/v2/defines"

type CndImpl struct {
	payloads      int64
	linker        defines.Linker
	field         string
	operation     defines.Operation
	values        []interface{}
	items         []defines.Condition
	rawExpression string
}

func (c *CndImpl) PayLoads() int64 {
	return c.payloads
}

func (c *CndImpl) Linker() defines.Linker {
	return c.linker
}
func (c *CndImpl) Field() string {
	return c.field
}
func (c *CndImpl) Operation() defines.Operation {
	return c.operation
}
func (c *CndImpl) Values() []interface{} {
	return c.values
}
func (c *CndImpl) SetValues(values []interface{}) {
	c.values = values
}
func (c *CndImpl) Items() []defines.Condition {
	return c.items
}
func (c *CndImpl) HasSubConditions() bool {
	return len(c.items) > 0
}
func (c *CndImpl) RawExpression() string {
	return c.rawExpression
}

func (c *CndImpl) Eq(field string, values interface{}) defines.Condition {
	return c.EqBool(true, field, values)
}

func (c *CndImpl) OrEq(field string, values interface{}) defines.Condition {
	return c.OrEqBool(true, field, values)
}

func (c *CndImpl) Ge(field string, values interface{}) defines.Condition {
	return c.GeBool(true, field, values)
}

func (c *CndImpl) OrGe(field string, values interface{}) defines.Condition {
	return c.OrGeBool(true, field, values)
}

func (c *CndImpl) Gt(field string, values interface{}) defines.Condition {
	return c.GtBool(true, field, values)
}

func (c *CndImpl) OrGt(field string, values interface{}) defines.Condition {
	return c.OrGtBool(true, field, values)
}

func (c *CndImpl) Le(field string, values interface{}) defines.Condition {
	return c.LeBool(true, field, values)
}

func (c *CndImpl) OrLe(field string, values interface{}) defines.Condition {
	return c.OrLeBool(true, field, values)
}

func (c *CndImpl) Lt(field string, values interface{}) defines.Condition {
	return c.LtBool(true, field, values)
}

func (c *CndImpl) OrLt(field string, values interface{}) defines.Condition {
	return c.OrLtBool(true, field, values)
}

func (c *CndImpl) NotEq(field string, values interface{}) defines.Condition {
	return c.NotEqBool(true, field, values)
}

func (c *CndImpl) OrNotEq(field string, values interface{}) defines.Condition {
	return c.OrNotEqBool(true, field, values)
}

func (c *CndImpl) In(field string, values ...interface{}) defines.Condition {
	return c.InBool(true, field, values...)
}

func (c *CndImpl) OrIn(field string, values ...interface{}) defines.Condition {
	return c.OrInBool(true, field, values...)

}

func (c *CndImpl) NotIn(field string, values ...interface{}) defines.Condition {
	return c.NotInBool(true, field, values...)
}

func (c *CndImpl) OrNotIn(field string, values ...interface{}) defines.Condition {
	return c.OrNotInBool(true, field, values...)
}

func (c *CndImpl) Like(field string, values interface{}) defines.Condition {
	return c.LikeBool(true, field, values)
}

func (c *CndImpl) OrLike(field string, values interface{}) defines.Condition {
	return c.OrLikeBool(true, field, values)
}

func (c *CndImpl) LikeIgnoreStart(field string, values interface{}) defines.Condition {
	return c.LikeIgnoreStartBool(true, field, values)
}

func (c *CndImpl) OrLikeIgnoreStart(field string, values interface{}) defines.Condition {
	return c.OrLikeIgnoreStartBool(true, field, values)
}

func (c *CndImpl) LikeIgnoreEnd(field string, values interface{}) defines.Condition {
	return c.LikeIgnoreEndBool(true, field, values)
}

func (c *CndImpl) OrLikeIgnoreEnd(field string, values interface{}) defines.Condition {
	return c.OrLikeIgnoreEndBool(true, field, values)
}

func (c *CndImpl) IsNull(filed string) defines.Condition {
	return c.IsNullBool(true, filed)
}

func (c *CndImpl) IsNotNull(field string) defines.Condition {
	return c.IsNotNullBool(true, field)
}

func (c *CndImpl) And(field string, operation defines.Operation, value ...interface{}) defines.Condition {
	return c.AndBool(true, field, operation, value)
}
func (c *CndImpl) And2(condition defines.Condition) defines.Condition {
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = defines.And
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) And3(rawExpresssion string, values ...interface{}) defines.Condition {
	return c.And3Bool(true, rawExpresssion, values...)
}

func (c *CndImpl) EqBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Eq, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrEqBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.Eq, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) GeBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Ge, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrGeBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.Ge, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) GtBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Gt, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrGtBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.Gt, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LeBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Le, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLeBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.Le, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LtBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Lt, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLtBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.Lt, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) NotEqBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.NotEq, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrNotEqBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.NotEq, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) InBool(b bool, field string, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.In, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrInBool(b bool, field string, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.NotEq, "", values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) NotInBool(b bool, field string, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.NotIn, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrNotInBool(b bool, field string, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.NotIn, "", values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Like, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLikeBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.Like, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeIgnoreStartBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.LikeIgnoreStart, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) OrLikeIgnoreStartBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndFull(defines.Or, field, defines.LikeIgnoreStart, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) LikeIgnoreEndBool(b bool, field string, values interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.Like, values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) IsNullBool(b bool, field string) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.IsNull, nil)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) IsNotNullBool(b bool, filed string) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(filed, defines.IsNotNull, nil)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrIsNull(field string) defines.Condition {
	return c.OrIsNullBool(true, field)
}

func (c *CndImpl) OrIsNullBool(b bool, field string) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.IsNull, nil)
	cc := condition.(*CndImpl)
	cc.linker = defines.Or
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrIsNotNull(field string) defines.Condition {
	return c.OrIsNotNullBool(true, field)
}

func (c *CndImpl) OrIsNotNullBool(b bool, field string) defines.Condition {
	if !b {
		return c
	}
	condition := Cnd(field, defines.IsNotNull, nil)
	cc := condition.(*CndImpl)
	cc.linker = defines.Or
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) OrLikeIgnoreEndBool(b bool, field string, values interface{}) defines.Condition {
	condition := CndFull(defines.Or, field, defines.LikeIgnoreEnd, "", values)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) AndBool(b bool, field string, operation defines.Operation, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	cc := Cnd(field, operation, values...).(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = defines.And
	c.items = append(c.items, cc)
	return c
}

func (c *CndImpl) And3Bool(b bool, rawExpresssion string, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = defines.And
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or(field string, operation defines.Operation, values ...interface{}) defines.Condition {
	return c.OrBool(true, field, operation, values...)
}
func (c *CndImpl) OrBool(b bool, field string, operation defines.Operation, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	cc := Cnd(field, operation, values...).(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = defines.Or
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or2(condition defines.Condition) defines.Condition {
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = defines.Or
	c.items = append(c.items, cc)
	return c
}
func (c *CndImpl) Or3(rawExpresssion string, values ...interface{}) defines.Condition {
	return c.Or3Bool(true, rawExpresssion, values...)
}
func (c *CndImpl) Or3Bool(b bool, rawExpresssion string, values ...interface{}) defines.Condition {
	if !b {
		return c
	}
	condition := CndRaw(rawExpresssion, values...)
	cc := condition.(*CndImpl)
	c.payloads += cc.payloads
	cc.linker = defines.Or
	c.items = append(c.items, cc)
	return c
}

func CndEq(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.Eq, value)
}
func CndNotEq(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.NotEq, value)
}
func CndGe(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.Ge, value)
}
func CndGt(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.Gt, value)
}
func CndLe(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.Le, value)
}
func CndLt(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.Lt, value)
}
func CndLike(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.Like, value)
}
func CndLikeIgnoreStart(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.LikeIgnoreStart, value)
}
func CndLikeIgnoreEnd(field string, value interface{}) defines.Condition {
	return Cnd(field, defines.LikeIgnoreEnd, value)
}
func CndIn(field string, values ...interface{}) defines.Condition {
	return Cnd(field, defines.In, values...)
}
func CndNotIn(field string, values ...interface{}) defines.Condition {
	return Cnd(field, defines.NotIn, values...)
}
func CndIsNull(field string) defines.Condition {
	return Cnd(field, defines.IsNull)
}
func CndIsNotNull(field string) defines.Condition {
	return Cnd(field, defines.IsNotNull)
}

func Cnd(field string, operation defines.Operation, values ...interface{}) defines.Condition {
	return CndFull(defines.And, field, operation, "", values...)
}

func CndEmpty() defines.Condition {
	return CndRaw("")
}

func CndRaw(rawExpresssion string, values ...interface{}) defines.Condition {
	payloads := int64(1)
	if rawExpresssion == "" {
		payloads = 0
	}
	return &CndImpl{payloads: payloads, linker: defines.And, rawExpression: rawExpresssion, values: UnZipSlice(values), operation: defines.RawOperation}
}

func CndFull(linker defines.Linker, field string, operation defines.Operation, rawExpression string, values ...interface{}) defines.Condition {
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
