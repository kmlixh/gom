package define

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestModel struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name"`
	Age       int64     `gom:"age"`
	Email     string    `gom:"email,default"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at"`
	IsActive  bool      `gom:"is_active"`
}

func (t *TestModel) TableName() string {
	return "test_model"
}

// Model without TableName method
type TestModelNoTable struct {
	ID   int64  `gom:"id,@"`
	Name string `gom:"name"`
}

// Model with invalid tags
type TestModelInvalidTags struct {
	ID   int64  `gom:",@,"`
	Name string `gom:","`
}

// Model with no tags
type TestModelNoTags struct {
	ID   int64
	Name string
}

func TestGetTransfer(t *testing.T) {
	t.Run("Valid Model", func(t *testing.T) {
		model := &TestModel{}
		transfer := GetTransfer(model)
		assert.NotNil(t, transfer)
		assert.Equal(t, "test_model", transfer.TableName)
		assert.Len(t, transfer.Fields, 7)
		assert.Len(t, transfer.FieldOrder, 7)
		assert.NotNil(t, transfer.PrimaryKey)
		assert.Equal(t, "id", transfer.PrimaryKey.Column)
		assert.True(t, transfer.PrimaryKey.IsAuto)
		assert.True(t, transfer.PrimaryKey.IsPrimary)
	})

	t.Run("Model Without TableName", func(t *testing.T) {
		model := &TestModelNoTable{}
		transfer := GetTransfer(model)
		assert.NotNil(t, transfer)
		assert.Equal(t, "test_model_no_table", transfer.TableName)
		assert.Len(t, transfer.Fields, 2)
		assert.NotNil(t, transfer.PrimaryKey)
		assert.False(t, transfer.PrimaryKey.IsAuto)
	})

	t.Run("Model With Invalid Tags", func(t *testing.T) {
		model := &TestModelInvalidTags{}
		transfer := GetTransfer(model)
		assert.NotNil(t, transfer)
		assert.Len(t, transfer.Fields, 2)
		assert.NotNil(t, transfer.PrimaryKey)
	})

	t.Run("Model With No Tags", func(t *testing.T) {
		model := &TestModelNoTags{}
		transfer := GetTransfer(model)
		assert.NotNil(t, transfer)
		assert.Empty(t, transfer.Fields)
		assert.Nil(t, transfer.PrimaryKey)
	})

	t.Run("Nil Model", func(t *testing.T) {
		transfer := GetTransfer(nil)
		assert.Nil(t, transfer)
	})
}

func TestTransferToMap(t *testing.T) {
	t.Run("Valid Model", func(t *testing.T) {
		model := &TestModel{
			ID:        1,
			Name:      "Test",
			Age:       25,
			Email:     "test@example.com",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			IsActive:  true,
		}

		transfer := GetTransfer(model)
		m := transfer.ToMap(model)
		assert.NotNil(t, m)
		assert.Equal(t, int64(1), m["id"])
		assert.Equal(t, "Test", m["name"])
		assert.Equal(t, int64(25), m["age"])
		assert.Equal(t, "test@example.com", m["email"])
		assert.Equal(t, 1, m["is_active"]) // bool is converted to int
	})

	t.Run("Nil Model", func(t *testing.T) {
		transfer := GetTransfer(&TestModel{})
		m := transfer.ToMap(nil)
		assert.Nil(t, m)
	})

	t.Run("Model With No Tags", func(t *testing.T) {
		model := &TestModelNoTags{
			ID:   1,
			Name: "Test",
		}
		transfer := GetTransfer(model)
		m := transfer.ToMap(model)
		assert.Empty(t, m)
	})
}

func TestTransferGetPrimaryKeyValue(t *testing.T) {
	t.Run("Valid Model", func(t *testing.T) {
		model := &TestModel{
			ID: 1,
		}
		transfer := GetTransfer(model)
		value, err := transfer.GetPrimaryKeyValue(model)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), value)
	})

	t.Run("Nil Model", func(t *testing.T) {
		transfer := GetTransfer(&TestModel{})
		value, err := transfer.GetPrimaryKeyValue(nil)
		assert.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("Model Without Primary Key", func(t *testing.T) {
		model := &TestModelNoTags{}
		transfer := GetTransfer(model)
		value, err := transfer.GetPrimaryKeyValue(model)
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}

func TestTransferCache(t *testing.T) {
	t.Run("Cache Hit", func(t *testing.T) {
		model := &TestModel{}
		// First call - creates cache
		transfer1 := GetTransfer(model)
		// Second call - uses cache
		transfer2 := GetTransfer(model)
		assert.Same(t, transfer1, transfer2)
	})

	t.Run("Different Types", func(t *testing.T) {
		model1 := &TestModel{}
		model2 := &TestModelNoTable{}
		transfer1 := GetTransfer(model1)
		transfer2 := GetTransfer(model2)
		assert.NotSame(t, transfer1, transfer2)
	})
}

func TestFieldConversion(t *testing.T) {
	t.Run("All Field Types", func(t *testing.T) {
		type AllTypesModel struct {
			Int        int        `gom:"int"`
			Int8       int8       `gom:"int8"`
			Int16      int16      `gom:"int16"`
			Int32      int32      `gom:"int32"`
			Int64      int64      `gom:"int64"`
			Uint       uint       `gom:"uint"`
			Uint8      uint8      `gom:"uint8"`
			Uint16     uint16     `gom:"uint16"`
			Uint32     uint32     `gom:"uint32"`
			Uint64     uint64     `gom:"uint64"`
			Float32    float32    `gom:"float32"`
			Float64    float64    `gom:"float64"`
			Complex64  complex64  `gom:"complex64"`
			Complex128 complex128 `gom:"complex128"`
			Bool       bool       `gom:"bool"`
			String     string     `gom:"string"`
			Time       time.Time  `gom:"time"`
		}

		model := &AllTypesModel{
			Int:        1,
			Int8:       2,
			Int16:      3,
			Int32:      4,
			Int64:      5,
			Uint:       6,
			Uint8:      7,
			Uint16:     8,
			Uint32:     9,
			Uint64:     10,
			Float32:    11.1,
			Float64:    12.2,
			Complex64:  complex(13, 14),
			Complex128: complex(15, 16),
			Bool:       true,
			String:     "test",
			Time:       time.Now(),
		}

		transfer := GetTransfer(model)
		m := transfer.ToMap(model)
		assert.NotNil(t, m)
		assert.Len(t, m, 17)
	})
}

func TestNilModel(t *testing.T) {
	t.Run("GetTransfer", func(t *testing.T) {
		assert.Nil(t, GetTransfer(nil))
	})

	t.Run("ToMap", func(t *testing.T) {
		transfer := GetTransfer(&TestModel{})
		assert.Nil(t, transfer.ToMap(nil))
	})

	t.Run("GetPrimaryKeyValue", func(t *testing.T) {
		transfer := GetTransfer(&TestModel{})
		value, err := transfer.GetPrimaryKeyValue(nil)
		assert.NoError(t, err)
		assert.Nil(t, value)
	})
}
