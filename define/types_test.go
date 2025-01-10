package define

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
	t.Run("Custom Type Conversion", func(t *testing.T) {
		type CustomInt int
		type CustomString string
		type ComplexStruct struct {
			ID      CustomInt
			Name    CustomString
			Created time.Time
			Data    map[string]interface{}
		}

		obj := ComplexStruct{
			ID:      CustomInt(123),
			Name:    CustomString("test"),
			Created: time.Now(),
			Data: map[string]interface{}{
				"key":    "value",
				"number": 42,
			},
		}

		m := StructToMap(obj)
		assert.NotNil(t, m)
		assert.Equal(t, 123, m["ID"])
		assert.Equal(t, "test", m["Name"])
		assert.NotNil(t, m["Created"])
		assert.NotNil(t, m["Data"])
	})

	t.Run("Nested Struct Conversion", func(t *testing.T) {
		type Address struct {
			Street string
			City   string
		}
		type Person struct {
			Name string
			Age  int
			Addr Address
		}

		p := Person{
			Name: "John",
			Age:  30,
			Addr: Address{
				Street: "123 Main St",
				City:   "Test City",
			},
		}

		m := StructToMap(p)
		assert.NotNil(t, m)
		assert.Equal(t, "John", m["Name"])
		assert.Equal(t, 30, m["Age"])
		addrMap, ok := m["Addr"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "123 Main St", addrMap["Street"])
		assert.Equal(t, "Test City", addrMap["City"])
	})
}

func TestTypeEdgeCases(t *testing.T) {
	t.Run("Zero Values", func(t *testing.T) {
		type ZeroStruct struct {
			Int    int
			String string
			Bool   bool
			Float  float64
			Time   time.Time
			Ptr    *string
		}

		z := ZeroStruct{}
		m := StructToMap(z)
		assert.NotNil(t, m)
		assert.Equal(t, 0, m["Int"])
		assert.Equal(t, "", m["String"])
		assert.Equal(t, false, m["Bool"])
		assert.Equal(t, 0.0, m["Float"])
		assert.NotNil(t, m["Time"])
		assert.Nil(t, m["Ptr"])
	})

	t.Run("Invalid Types", func(t *testing.T) {
		type InvalidStruct struct {
			Ch      chan int
			Func    func()
			Complex complex128
		}

		inv := InvalidStruct{
			Ch:      make(chan int),
			Func:    func() {},
			Complex: complex(1, 2),
		}

		m := StructToMap(inv)
		assert.NotNil(t, m)
		assert.Empty(t, m)
	})
}
