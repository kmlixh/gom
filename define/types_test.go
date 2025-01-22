package define

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Custom types for testing
type CustomInt int
type CustomString string

func TestIsolationLevel(t *testing.T) {
	tests := []struct {
		name  string
		level IsolationLevel
		want  sql.IsolationLevel
	}{
		{
			name:  "LevelDefault",
			level: LevelDefault,
			want:  sql.LevelDefault,
		},
		{
			name:  "LevelReadUncommitted",
			level: LevelReadUncommitted,
			want:  sql.LevelReadUncommitted,
		},
		{
			name:  "LevelReadCommitted",
			level: LevelReadCommitted,
			want:  sql.LevelReadCommitted,
		},
		{
			name:  "LevelWriteCommitted",
			level: LevelWriteCommitted,
			want:  sql.LevelWriteCommitted,
		},
		{
			name:  "LevelRepeatableRead",
			level: LevelRepeatableRead,
			want:  sql.LevelRepeatableRead,
		},
		{
			name:  "LevelSnapshot",
			level: LevelSnapshot,
			want:  sql.LevelSnapshot,
		},
		{
			name:  "LevelSerializable",
			level: LevelSerializable,
			want:  sql.LevelSerializable,
		},
		{
			name:  "LevelLinearizable",
			level: LevelLinearizable,
			want:  sql.LevelLinearizable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if sql.IsolationLevel(tt.level) != tt.want {
				t.Errorf("IsolationLevel %v = %v, want %v", tt.name, tt.level, tt.want)
			}
		})
	}
}

func TestTransactionPropagation(t *testing.T) {
	tests := []struct {
		name       string
		propagate  TransactionPropagation
		wantValue  int
		wantString string
	}{
		{
			name:       "PropagationRequired",
			propagate:  PropagationRequired,
			wantValue:  0,
			wantString: "PropagationRequired",
		},
		{
			name:       "PropagationRequiresNew",
			propagate:  PropagationRequiresNew,
			wantValue:  1,
			wantString: "PropagationRequiresNew",
		},
		{
			name:       "PropagationNested",
			propagate:  PropagationNested,
			wantValue:  2,
			wantString: "PropagationNested",
		},
		{
			name:       "PropagationSupports",
			propagate:  PropagationSupports,
			wantValue:  3,
			wantString: "PropagationSupports",
		},
		{
			name:       "PropagationNotSupported",
			propagate:  PropagationNotSupported,
			wantValue:  4,
			wantString: "PropagationNotSupported",
		},
		{
			name:       "PropagationNever",
			propagate:  PropagationNever,
			wantValue:  5,
			wantString: "PropagationNever",
		},
		{
			name:       "PropagationMandatory",
			propagate:  PropagationMandatory,
			wantValue:  6,
			wantString: "PropagationMandatory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.propagate) != tt.wantValue {
				t.Errorf("TransactionPropagation %v = %v, want %v", tt.name, tt.propagate, tt.wantValue)
			}
		})
	}
}

func TestTransactionOptions(t *testing.T) {
	tests := []struct {
		name    string
		options TransactionOptions
		want    TransactionOptions
	}{
		{
			name: "default options",
			options: TransactionOptions{
				Timeout:         time.Second * 30,
				IsolationLevel:  LevelDefault,
				PropagationMode: PropagationRequired,
				ReadOnly:        false,
			},
			want: TransactionOptions{
				Timeout:         time.Second * 30,
				IsolationLevel:  LevelDefault,
				PropagationMode: PropagationRequired,
				ReadOnly:        false,
			},
		},
		{
			name: "custom options",
			options: TransactionOptions{
				Timeout:         time.Minute,
				IsolationLevel:  LevelReadCommitted,
				PropagationMode: PropagationRequiresNew,
				ReadOnly:        true,
			},
			want: TransactionOptions{
				Timeout:         time.Minute,
				IsolationLevel:  LevelReadCommitted,
				PropagationMode: PropagationRequiresNew,
				ReadOnly:        true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.options.Timeout != tt.want.Timeout {
				t.Errorf("Timeout = %v, want %v", tt.options.Timeout, tt.want.Timeout)
			}
			if tt.options.IsolationLevel != tt.want.IsolationLevel {
				t.Errorf("IsolationLevel = %v, want %v", tt.options.IsolationLevel, tt.want.IsolationLevel)
			}
			if tt.options.PropagationMode != tt.want.PropagationMode {
				t.Errorf("PropagationMode = %v, want %v", tt.options.PropagationMode, tt.want.PropagationMode)
			}
			if tt.options.ReadOnly != tt.want.ReadOnly {
				t.Errorf("ReadOnly = %v, want %v", tt.options.ReadOnly, tt.want.ReadOnly)
			}
		})
	}
}

func TestJoinType(t *testing.T) {
	tests := []struct {
		name     string
		joinType JoinType
		want     int
	}{
		{
			name:     "JoinAnd",
			joinType: JoinAnd,
			want:     0,
		},
		{
			name:     "JoinOr",
			joinType: JoinOr,
			want:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.joinType) != tt.want {
				t.Errorf("JoinType %v = %v, want %v", tt.name, tt.joinType, tt.want)
			}
		})
	}
}

func TestLockType(t *testing.T) {
	tests := []struct {
		name     string
		lockType LockType
		want     int
	}{
		{
			name:     "LockNone",
			lockType: LockNone,
			want:     0,
		},
		{
			name:     "LockForUpdate",
			lockType: LockForUpdate,
			want:     1,
		},
		{
			name:     "LockForShare",
			lockType: LockForShare,
			want:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.lockType) != tt.want {
				t.Errorf("LockType %v = %v, want %v", tt.name, tt.lockType, tt.want)
			}
		})
	}
}

func TestOpType(t *testing.T) {
	tests := []struct {
		name   string
		opType OpType
		want   int
	}{
		{"OpEq", OpEq, 0},
		{"OpNe", OpNe, 1},
		{"OpGt", OpGt, 2},
		{"OpGe", OpGe, 3},
		{"OpLt", OpLt, 4},
		{"OpLe", OpLe, 5},
		{"OpLike", OpLike, 6},
		{"OpNotLike", OpNotLike, 7},
		{"OpIn", OpIn, 8},
		{"OpNotIn", OpNotIn, 9},
		{"OpIsNull", OpIsNull, 10},
		{"OpIsNotNull", OpIsNotNull, 11},
		{"OpBetween", OpBetween, 12},
		{"OpNotBetween", OpNotBetween, 13},
		{"OpCustom", OpCustom, 14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.opType) != tt.want {
				t.Errorf("OpType %v = %v, want %v", tt.name, tt.opType, tt.want)
			}
		})
	}
}

func TestComplexTypeConversions(t *testing.T) {
	t.Run("Custom_Type_Conversion", func(t *testing.T) {
		customInt := CustomInt(123)
		customStr := CustomString("test")

		// Test conversion from custom type to base type
		intVal := int(customInt)
		strVal := string(customStr)

		if intVal != 123 {
			t.Errorf("Expected int value 123, got %v", intVal)
		}
		if strVal != "test" {
			t.Errorf("Expected string value 'test', got %v", strVal)
		}
	})

	t.Run("Nested_Struct_Conversion", func(t *testing.T) {
		type Address struct {
			Street string
			City   string
		}

		type Person struct {
			Name    string
			Address *Address
		}

		addr := &Address{
			Street: "123 Main St",
			City:   "Test City",
		}

		person := &Person{
			Name:    "Test Person",
			Address: addr,
		}

		if person.Address == nil {
			t.Error("Address should not be nil")
		} else {
			if person.Address.Street != "123 Main St" {
				t.Errorf("Expected street '123 Main St', got %v", person.Address.Street)
			}
			if person.Address.City != "Test City" {
				t.Errorf("Expected city 'Test City', got %v", person.Address.City)
			}
		}
	})
}

func TestTypeEdgeCases(t *testing.T) {
	t.Run("Zero_Values", func(t *testing.T) {
		type TestStruct struct {
			IntField    int
			StringField string
			BoolField   bool
			FloatField  float64
			TimeField   time.Time
		}

		obj := TestStruct{}
		result, err := StructToMap(obj)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Check that zero values are included in the map
		assert.Equal(t, 0, result["IntField"])
		assert.Equal(t, "", result["StringField"])
		assert.Equal(t, false, result["BoolField"])
		assert.Equal(t, 0.0, result["FloatField"])
		assert.NotNil(t, result["TimeField"])
	})

	t.Run("Invalid_Types", func(t *testing.T) {
		// Test with non-struct types
		_, err := StructToMap(42)
		assert.Error(t, err)

		_, err = StructToMap("string")
		assert.Error(t, err)

		_, err = StructToMap(nil)
		assert.Error(t, err)
	})
}
