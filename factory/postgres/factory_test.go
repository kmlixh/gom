package postgres

import (
	"testing"

	"github.com/kmlixh/gom/v4/define"
	"github.com/stretchr/testify/assert"
)

func TestBuildCondition(t *testing.T) {
	factory := &Factory{}

	tests := []struct {
		name     string
		cond     *define.Condition
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "Simple Equal",
			cond:     define.Eq("name", "test"),
			wantSQL:  "name = $1",
			wantArgs: []interface{}{"test"},
		},
		{
			name:     "Simple Not Equal",
			cond:     define.Ne("age", 20),
			wantSQL:  "age != $1",
			wantArgs: []interface{}{20},
		},
		{
			name:     "Greater Than",
			cond:     define.Gt("score", 80),
			wantSQL:  "score > $1",
			wantArgs: []interface{}{80},
		},
		{
			name:     "Less Than or Equal",
			cond:     define.Le("price", 100.5),
			wantSQL:  "price <= $1",
			wantArgs: []interface{}{100.5},
		},
		{
			name:     "Like",
			cond:     define.Like("description", "%test%"),
			wantSQL:  "description LIKE $1",
			wantArgs: []interface{}{"%test%"},
		},
		{
			name:     "Not Like",
			cond:     define.NotLike("title", "draft%"),
			wantSQL:  "title NOT LIKE $1",
			wantArgs: []interface{}{"draft%"},
		},
		{
			name:     "In",
			cond:     define.In("status", "active", "pending"),
			wantSQL:  "status IN ($1, $2)",
			wantArgs: []interface{}{"active", "pending"},
		},
		{
			name:     "Not In",
			cond:     define.NotIn("category", 1, 2, 3),
			wantSQL:  "category NOT IN ($1, $2, $3)",
			wantArgs: []interface{}{1, 2, 3},
		},
		{
			name:     "Is Null",
			cond:     define.IsNull("deleted_at"),
			wantSQL:  "deleted_at IS NULL",
			wantArgs: nil,
		},
		{
			name:     "Is Not Null",
			cond:     define.IsNotNull("updated_at"),
			wantSQL:  "updated_at IS NOT NULL",
			wantArgs: nil,
		},
		{
			name:     "Between",
			cond:     define.Between("date", "2023-01-01", "2023-12-31"),
			wantSQL:  "date BETWEEN $1 AND $2",
			wantArgs: []interface{}{"2023-01-01", "2023-12-31"},
		},
		{
			name:     "Not Between",
			cond:     define.NotBetween("time", "09:00", "17:00"),
			wantSQL:  "time NOT BETWEEN $1 AND $2",
			wantArgs: []interface{}{"09:00", "17:00"},
		},
		{
			name: "Complex AND",
			cond: define.Gt("age", 18).
				And(define.Eq("active", true)),
			wantSQL:  "age > $1 AND active = $2",
			wantArgs: []interface{}{18, true},
		},
		{
			name: "Complex OR",
			cond: define.Eq("role", "admin").
				Or(define.Eq("role", "superuser")),
			wantSQL:  "role = $1 OR role = $2",
			wantArgs: []interface{}{"admin", "superuser"},
		},
		{
			name: "Complex Mixed",
			cond: define.Gt("age", 18).
				And(define.Eq("role", "admin")).
				Or(define.Eq("role", "superuser").
					And(define.Ge("experience", 5))),
			wantSQL:  "age > $1 AND role = $2 OR (role = $3 AND experience >= $4)",
			wantArgs: []interface{}{18, "admin", "superuser", 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := factory.buildCondition(tt.cond, 1)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestBuildSelect(t *testing.T) {
	factory := &Factory{}

	tests := []struct {
		name     string
		table    string
		fields   []string
		cond     *define.Condition
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "Simple Select All",
			table:    "users",
			fields:   []string{"*"},
			cond:     nil,
			wantSQL:  `SELECT * FROM "users"`,
			wantArgs: nil,
		},
		{
			name:     "Select Specific Fields",
			table:    "products",
			fields:   []string{"id", "name", "price"},
			cond:     nil,
			wantSQL:  `SELECT "id", "name", "price" FROM "products"`,
			wantArgs: nil,
		},
		{
			name:     "Select With Condition",
			table:    "orders",
			fields:   []string{"*"},
			cond:     define.Eq("status", "pending"),
			wantSQL:  `SELECT * FROM "orders" WHERE status = $1`,
			wantArgs: []interface{}{"pending"},
		},
		{
			name:   "Select With Complex Condition",
			table:  "employees",
			fields: []string{"id", "name", "department"},
			cond: define.Gt("age", 30).
				And(define.In("department", "IT", "HR")),
			wantSQL:  `SELECT "id", "name", "department" FROM "employees" WHERE age > $1 AND department IN ($2, $3)`,
			wantArgs: []interface{}{30, "IT", "HR"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := factory.BuildSelect(tt.table, tt.fields, []*define.Condition{tt.cond}, "", 0, 0)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestBuildUpdate(t *testing.T) {
	factory := &Factory{}

	tests := []struct {
		name       string
		table      string
		fields     map[string]interface{}
		fieldOrder []string
		conditions []*define.Condition
		wantSQL    string
		wantArgs   []interface{}
	}{
		{
			name:  "Simple Update",
			table: "users",
			fields: map[string]interface{}{
				"name":  "John Doe",
				"email": "john@example.com",
			},
			fieldOrder: []string{"name", "email"},
			conditions: []*define.Condition{define.Eq("id", 1)},
			wantSQL:    `UPDATE "users" SET "name" = $1, "email" = $2 WHERE id = $3`,
			wantArgs:   []interface{}{"John Doe", "john@example.com", 1},
		},
		{
			name:  "Update Multiple Fields",
			table: "products",
			fields: map[string]interface{}{
				"price":      99.99,
				"stock":      100,
				"updated_at": "2023-01-01",
			},
			fieldOrder: []string{"price", "stock", "updated_at"},
			conditions: []*define.Condition{define.Gt("id", 10)},
			wantSQL:    `UPDATE "products" SET "price" = $1, "stock" = $2, "updated_at" = $3 WHERE id > $4`,
			wantArgs:   []interface{}{99.99, 100, "2023-01-01", 10},
		},
		{
			name:  "Update With Complex Condition",
			table: "orders",
			fields: map[string]interface{}{
				"status": "completed",
			},
			fieldOrder: []string{"status"},
			conditions: []*define.Condition{
				define.Eq("status", "pending").And(define.Gt("total", 1000)),
			},
			wantSQL:  `UPDATE "orders" SET "status" = $1 WHERE status = $2 AND total > $3`,
			wantArgs: []interface{}{"completed", "pending", 1000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := factory.BuildUpdate(tt.table, tt.fields, tt.fieldOrder, tt.conditions)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestBuildInsert(t *testing.T) {
	factory := &Factory{}

	tests := []struct {
		name       string
		table      string
		fields     map[string]interface{}
		fieldOrder []string
		wantSQL    string
		wantArgs   []interface{}
	}{
		{
			name:  "Simple Insert",
			table: "users",
			fields: map[string]interface{}{
				"name":  "John Doe",
				"email": "john@example.com",
			},
			fieldOrder: []string{"name", "email"},
			wantSQL:    `INSERT INTO "users" ("name", "email") VALUES ($1, $2)`,
			wantArgs:   []interface{}{"John Doe", "john@example.com"},
		},
		{
			name:  "Insert Multiple Fields",
			table: "products",
			fields: map[string]interface{}{
				"name":       "Product 1",
				"price":      99.99,
				"stock":      100,
				"created_at": "2023-01-01",
			},
			fieldOrder: []string{"name", "price", "stock", "created_at"},
			wantSQL:    `INSERT INTO "products" ("name", "price", "stock", "created_at") VALUES ($1, $2, $3, $4)`,
			wantArgs:   []interface{}{"Product 1", 99.99, 100, "2023-01-01"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := factory.BuildInsert(tt.table, tt.fields, tt.fieldOrder)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestBuildDelete(t *testing.T) {
	factory := &Factory{}

	tests := []struct {
		name     string
		table    string
		cond     *define.Condition
		wantSQL  string
		wantArgs []interface{}
	}{
		{
			name:     "Simple Delete",
			table:    "users",
			cond:     define.Eq("id", 1),
			wantSQL:  `DELETE FROM "users" WHERE id = $1`,
			wantArgs: []interface{}{1},
		},
		{
			name:  "Delete With Complex Condition",
			table: "orders",
			cond: define.Eq("status", "cancelled").
				Or(define.Lt("created_at", "2023-01-01")),
			wantSQL:  `DELETE FROM "orders" WHERE status = $1 OR created_at < $2`,
			wantArgs: []interface{}{"cancelled", "2023-01-01"},
		},
		{
			name:  "Delete With Multiple Conditions",
			table: "products",
			cond: define.Eq("active", false).
				And(define.Le("stock", 0)).
				And(define.IsNull("updated_at")),
			wantSQL:  `DELETE FROM "products" WHERE active = $1 AND stock <= $2 AND updated_at IS NULL`,
			wantArgs: []interface{}{false, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := factory.BuildDelete(tt.table, []*define.Condition{tt.cond})
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}
