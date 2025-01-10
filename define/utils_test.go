package define

import (
	"reflect"
	"testing"
)

type TestStruct struct {
	ID        int    `gom:"id,auto"`
	Name      string `gom:"name"`
	Age       int    `gom:"age"`
	Email     string `gom:"email,default"`
	Ignored   string
	ZeroField int `gom:"zero_field"`
}

func TestStructToMap(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected map[string]interface{}
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name:     "non-struct input",
			input:    "not a struct",
			expected: nil,
		},
		{
			name: "valid struct",
			input: &TestStruct{
				ID:        1,
				Name:      "test",
				Age:       25,
				Email:     "",
				Ignored:   "ignored",
				ZeroField: 0,
			},
			expected: map[string]interface{}{
				"name":       "test",
				"age":        25,
				"zero_field": 0,
			},
		},
		{
			name: "struct with non-zero values",
			input: TestStruct{
				ID:        2,
				Name:      "test2",
				Age:       30,
				Email:     "test@example.com",
				Ignored:   "ignored",
				ZeroField: 42,
			},
			expected: map[string]interface{}{
				"name":       "test2",
				"age":        30,
				"email":      "test@example.com",
				"zero_field": 42,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StructToMap(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("StructToMap() = %v, want %v", result, tt.expected)
			}
		})
	}
}
