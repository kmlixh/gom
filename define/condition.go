package define

// Condition represents a SQL condition
type Condition struct {
	Field string
	Op    string
	Value interface{}
}
