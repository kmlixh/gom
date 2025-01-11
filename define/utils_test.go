package define

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	t.Run("nil_input", func(t *testing.T) {
		result, err := StructToMap(nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("non-struct_input", func(t *testing.T) {
		result, err := StructToMap(42)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("valid_struct", func(t *testing.T) {
		type TestStruct struct {
			Name      string
			Age       int
			Email     string
			ZeroField int
		}
		obj := TestStruct{
			Name: "test",
			Age:  25,
		}
		result, err := StructToMap(obj)
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{
			"Name": "test",
			"Age":  25,
		}, result)
	})

	t.Run("struct_with_non-zero_values", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Age   int
			Email string
		}
		obj := TestStruct{
			Name:  "test",
			Age:   25,
			Email: "test@example.com",
		}
		result, err := StructToMap(obj)
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{
			"Name":  "test",
			"Age":   25,
			"Email": "test@example.com",
		}, result)
	})
}
