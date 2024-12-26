package define

// OpType represents the type of condition operator
type OpType int

// JoinType represents how conditions are joined
type JoinType int

const (
	JoinAnd JoinType = iota // AND connection
	JoinOr                  // OR connection
)

const (
	OpEq         OpType = iota // Equal
	OpNe                       // Not Equal
	OpGt                       // Greater Than
	OpGe                       // Greater Than or Equal
	OpLt                       // Less Than
	OpLe                       // Less Than or Equal
	OpLike                     // LIKE
	OpNotLike                  // NOT LIKE
	OpIn                       // IN
	OpNotIn                    // NOT IN
	OpIsNull                   // IS NULL
	OpIsNotNull                // IS NOT NULL
	OpBetween                  // BETWEEN
	OpNotBetween               // NOT BETWEEN
)

// Condition represents a where condition
type Condition struct {
	Field      string       // Field name
	Op         OpType       // Operator type
	Value      interface{}  // Value to compare against
	Join       JoinType     // How this condition joins with others (AND/OR)
	SubConds   []*Condition // Sub-conditions for nested queries
	IsSubGroup bool         // Whether this is a sub-group of conditions
}

// NewCondition creates a new condition
func NewCondition(field string, op OpType, value interface{}) *Condition {
	return &Condition{
		Field: field,
		Op:    op,
		Value: value,
	}
}

// And adds a condition with AND join
func (c *Condition) And(cond *Condition) *Condition {
	cond.Join = JoinAnd
	if c.SubConds == nil {
		c.SubConds = make([]*Condition, 0)
	}
	c.SubConds = append(c.SubConds, cond)
	return c
}

// Or adds a condition with OR join
func (c *Condition) Or(cond *Condition) *Condition {
	cond.Join = JoinOr
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

// flattenValues flattens a list of values that may contain arrays
func flattenValues(values []interface{}) []interface{} {
	var result []interface{}
	for _, v := range values {
		switch val := v.(type) {
		case []interface{}:
			result = append(result, val...)
		case []string:
			for _, s := range val {
				result = append(result, s)
			}
		case []int:
			for _, i := range val {
				result = append(result, i)
			}
		case []int64:
			for _, i := range val {
				result = append(result, i)
			}
		case []float64:
			for _, f := range val {
				result = append(result, f)
			}
		default:
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
