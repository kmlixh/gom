package postgres

import (
	"testing"

	"github.com/kmlixh/gom/v4/define"
	"github.com/stretchr/testify/assert"
)

func TestFactory_BuildCondition_IN(t *testing.T) {
	factory := &Factory{}

	tests := []struct {
		name          string
		condition     *define.Condition
		expectedSQL   string
		expectedArgs  []interface{}
		expectedCount int
	}{
		{
			name: "Simple IN condition",
			condition: &define.Condition{
				Field: "user_id",
				Op:    define.OpIn,
				Value: []interface{}{1, 2, 3},
			},
			expectedSQL:   `"user_id" IN ($1, $2, $3)`,
			expectedArgs:  []interface{}{1, 2, 3},
			expectedCount: 3,
		},
		{
			name: "NOT IN condition",
			condition: &define.Condition{
				Field: "status",
				Op:    define.OpNotIn,
				Value: []interface{}{"active", "pending"},
			},
			expectedSQL:   `"status" NOT IN ($1, $2)`,
			expectedArgs:  []interface{}{"active", "pending"},
			expectedCount: 2,
		},
		{
			name: "IN with special characters in field name",
			condition: &define.Condition{
				Field: "user.id",
				Op:    define.OpIn,
				Value: []interface{}{1, 2},
			},
			expectedSQL:   `"user"."id" IN ($1, $2)`,
			expectedArgs:  []interface{}{1, 2},
			expectedCount: 2,
		},
		{
			name: "Empty IN values",
			condition: &define.Condition{
				Field: "id",
				Op:    define.OpIn,
				Value: []interface{}{},
			},
			expectedSQL:   "",
			expectedArgs:  nil,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramCount := 1
			sql, args := factory.buildCondition(tt.condition, &paramCount)

			assert.Equal(t, tt.expectedSQL, sql, "SQL should match")
			assert.Equal(t, tt.expectedArgs, args, "Arguments should match")
			assert.Equal(t, tt.expectedCount+1, paramCount, "Parameter count should be incremented correctly")
		})
	}
}

func TestFactory_BuildSelect_WithIN(t *testing.T) {
	factory := &Factory{}

	conditions := []*define.Condition{
		{
			Field: "department_id",
			Op:    define.OpIn,
			Value: []interface{}{1, 2, 3},
		},
		{
			Field:    "status",
			Op:       define.OpIn,
			Value:    []interface{}{"active", "pending"},
			JoinType: define.JoinAnd,
		},
	}

	proto := factory.BuildSelect(
		"employees",
		[]string{"id", "name", "department_id", "status"},
		conditions,
		"id ASC",
		10,
		0,
	)

	expectedSQL := `SELECT "id", "name", "department_id", "status" FROM "employees" WHERE "department_id" IN ($1, $2, $3) AND "status" IN ($4, $5) ORDER BY id ASC LIMIT 10`
	expectedArgs := []interface{}{1, 2, 3, "active", "pending"}

	assert.NoError(t, proto.Error)
	assert.Equal(t, expectedSQL, proto.Sql)
	assert.Equal(t, expectedArgs, proto.Args)
}
