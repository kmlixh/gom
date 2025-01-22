package example

import (
	"time"

	"github.com/kmlixh/gom/v4/define"
)

// User represents a user in the system
type User struct {
	ID        int64     `gom:"id,primary_key,auto_increment"`
	Username  string    `gom:"username,size:50,not_null,unique"`
	Email     string    `gom:"email,size:100,not_null,unique"`
	Age       int       `gom:"age"`
	Active    bool      `gom:"active,default:true"`
	Role      string    `gom:"role,size:20,default:'user'"`
	CreatedAt time.Time `gom:"created_at,not_null"`
	UpdatedAt time.Time `gom:"updated_at,not_null"`
}

// TableName returns the table name for User
func (u *User) TableName() string {
	return "users"
}

// ExampleQueries demonstrates various query patterns
func ExampleQueries() {
	// Basic equality condition
	basicCond := define.Eq("username", "john_doe")

	// Multiple conditions with AND
	multiCond := define.Eq("active", true).
		And(define.Gt("age", 18)).
		And(define.Like("email", "%@example.com"))

	// OR conditions
	orCond := define.Eq("role", "admin").
		Or(define.Eq("role", "manager"))

	// Complex nested conditions with array IN
	roles := []string{"manager", "supervisor"}
	extraRoles := []interface{}{"leader", "admin"}
	complexCond := define.Eq("active", true).And(
		define.Eq("role", "admin").
			Or(define.Gt("age", 25).
				And(define.In("role", roles, extraRoles, "guest"))))

	// Date range condition
	dateRangeCond := define.Ge("created_at", time.Now().AddDate(0, -1, 0)).
		And(define.Lt("created_at", time.Now()))

	// NULL checks with mixed array IN condition
	statuses := []string{"active", "pending"}
	moreStatuses := []interface{}{"reviewing", "approved"}
	nullCheckCond := define.IsNull("deleted_at").
		Or(define.IsNotNull("last_login").
			And(define.In("status", statuses, moreStatuses)))

	// BETWEEN condition with NOT IN using mixed arrays
	restrictedRoles := []string{"guest", "banned"}
	moreRestricted := []string{"temporary", "limited"}
	betweenCond := define.Between("age", 18, 30).
		And(define.NotIn("role", restrictedRoles, moreRestricted))

	// Array operations with numeric types
	ages := []int{25, 30, 35}
	moreAges := []int64{40, 45, 50}
	ageCond := define.In("age", ages, moreAges, 60)

	// Combining multiple complex conditions
	finalCond := complexCond.
		And(dateRangeCond).
		And(nullCheckCond.Or(betweenCond)).
		And(ageCond)

	// These conditions can be used with Where2 method
	_ = basicCond
	_ = multiCond
	_ = orCond
	_ = complexCond
	_ = dateRangeCond
	_ = nullCheckCond
	_ = betweenCond
	_ = ageCond
	_ = finalCond
}

// CreateUserTableSQL returns the SQL to create the users table
func CreateUserTableSQL() string {
	return `
CREATE TABLE users (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    active BOOLEAN DEFAULT true,
    role VARCHAR(20) DEFAULT 'user',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
)`
}
