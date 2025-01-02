package gom

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yaoapp/gom/define"
)

func TestChainBasics(t *testing.T) {
	chain := NewChain()

	// Test Table
	chain.Table("users")
	assert.Equal(t, "users", chain.table)

	// Test Fields
	chain.Fields("id", "name", "email")
	assert.Equal(t, []string{"id", "name", "email"}, chain.fields)

	// Test Where with simple condition
	chain.Where(define.Eq("id", 1))
	assert.NotNil(t, chain.conditions)
	assert.Len(t, chain.conditions, 1)
	assert.Equal(t, "id", chain.conditions[0].Field)
	assert.Equal(t, define.OpEq, chain.conditions[0].Op)
	assert.Equal(t, 1, chain.conditions[0].Value)

	// Test OrderBy
	chain.OrderBy("id", define.OrderDesc)
	assert.NotNil(t, chain.orders)
	assert.Len(t, chain.orders, 1)
	assert.Equal(t, "id", chain.orders[0].Field)
	assert.Equal(t, define.OrderDesc, chain.orders[0].Type)

	// Test Limit and Offset
	chain.Limit(10).Offset(5)
	assert.Equal(t, uint(10), chain.limit)
	assert.Equal(t, uint(5), chain.offset)
}

func TestChainConditions(t *testing.T) {
	chain := NewChain()

	// Test multiple Where conditions
	chain.Where(define.Gt("age", 18)).
		Where(define.Like("email", "%@example.com")).
		Where(define.In("status", "active", "pending"))

	assert.Len(t, chain.conditions, 3)

	// Verify first condition
	assert.Equal(t, "age", chain.conditions[0].Field)
	assert.Equal(t, define.OpGt, chain.conditions[0].Op)
	assert.Equal(t, 18, chain.conditions[0].Value)

	// Verify second condition
	assert.Equal(t, "email", chain.conditions[1].Field)
	assert.Equal(t, define.OpLike, chain.conditions[1].Op)
	assert.Equal(t, "%@example.com", chain.conditions[1].Value)

	// Verify third condition
	assert.Equal(t, "status", chain.conditions[2].Field)
	assert.Equal(t, define.OpIn, chain.conditions[2].Op)
	assert.Equal(t, []interface{}{"active", "pending"}, chain.conditions[2].Value)
}

func TestChainOrdering(t *testing.T) {
	chain := NewChain()

	// Test multiple OrderBy calls
	chain.OrderBy("created_at", define.OrderDesc).
		OrderBy("name", define.OrderAsc).
		OrderBy("id", define.OrderDesc)

	assert.Len(t, chain.orders, 3)

	// Verify first order
	assert.Equal(t, "created_at", chain.orders[0].Field)
	assert.Equal(t, define.OrderDesc, chain.orders[0].Type)

	// Verify second order
	assert.Equal(t, "name", chain.orders[1].Field)
	assert.Equal(t, define.OrderAsc, chain.orders[1].Type)

	// Verify third order
	assert.Equal(t, "id", chain.orders[2].Field)
	assert.Equal(t, define.OrderDesc, chain.orders[2].Type)
}

func TestChainReset(t *testing.T) {
	chain := NewChain()

	// Set various chain properties
	chain.Table("users").
		Fields("id", "name").
		Where(define.Eq("active", true)).
		OrderBy("id", define.OrderDesc).
		Limit(10).
		Offset(5)

	// Verify properties are set
	assert.Equal(t, "users", chain.table)
	assert.Equal(t, []string{"id", "name"}, chain.fields)
	assert.Len(t, chain.conditions, 1)
	assert.Len(t, chain.orders, 1)
	assert.Equal(t, uint(10), chain.limit)
	assert.Equal(t, uint(5), chain.offset)

	// Reset chain
	chain.Reset()

	// Verify all properties are reset
	assert.Empty(t, chain.table)
	assert.Empty(t, chain.fields)
	assert.Empty(t, chain.conditions)
	assert.Empty(t, chain.orders)
	assert.Zero(t, chain.limit)
	assert.Zero(t, chain.offset)
}

func TestChainClone(t *testing.T) {
	original := NewChain()

	// Set various chain properties
	original.Table("users").
		Fields("id", "name").
		Where(define.Eq("active", true)).
		OrderBy("id", define.OrderDesc).
		Limit(10).
		Offset(5)

	// Clone the chain
	cloned := original.Clone()

	// Verify cloned properties match original
	assert.Equal(t, original.table, cloned.table)
	assert.Equal(t, original.fields, cloned.fields)
	assert.Equal(t, len(original.conditions), len(cloned.conditions))
	assert.Equal(t, len(original.orders), len(cloned.orders))
	assert.Equal(t, original.limit, cloned.limit)
	assert.Equal(t, original.offset, cloned.offset)

	// Modify cloned chain
	cloned.Table("posts").
		Fields("title").
		Where(define.Eq("published", true)).
		OrderBy("created_at", define.OrderAsc).
		Limit(20).
		Offset(0)

	// Verify original chain remains unchanged
	assert.Equal(t, "users", original.table)
	assert.Equal(t, []string{"id", "name"}, original.fields)
	assert.Len(t, original.conditions, 1)
	assert.Equal(t, "active", original.conditions[0].Field)
	assert.Len(t, original.orders, 1)
	assert.Equal(t, "id", original.orders[0].Field)
	assert.Equal(t, uint(10), original.limit)
	assert.Equal(t, uint(5), original.offset)

	// Verify cloned chain has new values
	assert.Equal(t, "posts", cloned.table)
	assert.Equal(t, []string{"title"}, cloned.fields)
	assert.Len(t, cloned.conditions, 1)
	assert.Equal(t, "published", cloned.conditions[0].Field)
	assert.Len(t, cloned.orders, 1)
	assert.Equal(t, "created_at", cloned.orders[0].Field)
	assert.Equal(t, uint(20), cloned.limit)
	assert.Equal(t, uint(0), cloned.offset)
}

func TestChainValidation(t *testing.T) {
	chain := NewChain()

	// Test empty table
	err := chain.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "table name is required")

	// Test with table but no fields
	chain.Table("users")
	err = chain.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one field is required")

	// Test with invalid limit
	chain.Fields("*").Limit(0)
	err = chain.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "limit must be greater than 0")

	// Test with valid configuration
	chain.Limit(10)
	err = chain.Validate()
	assert.NoError(t, err)
}

func TestChainComplexQuery(t *testing.T) {
	chain := NewChain()

	// Build a complex query
	chain.Table("users").
		Fields("id", "name", "email", "status").
		Where(define.Gt("age", 18)).
		Where(define.In("status", "active", "pending")).
		Where(define.Like("email", "%@example.com")).
		Where(define.IsNotNull("last_login")).
		OrderBy("created_at", define.OrderDesc).
		OrderBy("name", define.OrderAsc).
		Limit(20).
		Offset(40)

	// Verify table and fields
	assert.Equal(t, "users", chain.table)
	assert.Equal(t, []string{"id", "name", "email", "status"}, chain.fields)

	// Verify conditions
	assert.Len(t, chain.conditions, 4)
	assert.Equal(t, "age", chain.conditions[0].Field)
	assert.Equal(t, define.OpGt, chain.conditions[0].Op)
	assert.Equal(t, 18, chain.conditions[0].Value)

	assert.Equal(t, "status", chain.conditions[1].Field)
	assert.Equal(t, define.OpIn, chain.conditions[1].Op)
	assert.Equal(t, []interface{}{"active", "pending"}, chain.conditions[1].Value)

	assert.Equal(t, "email", chain.conditions[2].Field)
	assert.Equal(t, define.OpLike, chain.conditions[2].Op)
	assert.Equal(t, "%@example.com", chain.conditions[2].Value)

	assert.Equal(t, "last_login", chain.conditions[3].Field)
	assert.Equal(t, define.OpIsNotNull, chain.conditions[3].Op)

	// Verify ordering
	assert.Len(t, chain.orders, 2)
	assert.Equal(t, "created_at", chain.orders[0].Field)
	assert.Equal(t, define.OrderDesc, chain.orders[0].Type)
	assert.Equal(t, "name", chain.orders[1].Field)
	assert.Equal(t, define.OrderAsc, chain.orders[1].Type)

	// Verify limit and offset
	assert.Equal(t, uint(20), chain.limit)
	assert.Equal(t, uint(40), chain.offset)

	// Validate the chain
	err := chain.Validate()
	assert.NoError(t, err)
}
