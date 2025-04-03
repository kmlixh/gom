package define

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestResultModel struct {
	ID        int64                  `gom:"id"`
	Name      string                 `gom:"name"`
	Age       int64                  `gom:"age"`
	IsActive  bool                   `gom:"is_active"`
	Score     float64                `gom:"score"`
	CreatedAt time.Time              `gom:"created_at"`
	Tags      []string               `gom:"tags"`
	Metadata  map[string]interface{} `gom:"metadata"`
}

func TestResultBasicOperations(t *testing.T) {
	t.Run("Basic Methods", func(t *testing.T) {
		result := &Result{
			ID:       1,
			Affected: 2,
			Error:    nil,
			Data: []map[string]interface{}{
				{
					"id":         int64(1),
					"name":       "Test 1",
					"age":        int64(25),
					"is_active":  true,
					"score":      float64(85.5),
					"created_at": time.Now(),
					"tags":       []string{"tag1", "tag2"},
					"metadata":   map[string]interface{}{"key": "value"},
				},
				{
					"id":         int64(2),
					"name":       "Test 2",
					"age":        int64(30),
					"is_active":  false,
					"score":      float64(90.0),
					"created_at": time.Now(),
					"tags":       []string{"tag3", "tag4"},
					"metadata":   map[string]interface{}{"key": "value2"},
				},
			},
			Columns: []string{"id", "name", "age", "is_active", "score", "created_at", "tags", "metadata"},
		}

		// Test LastInsertId
		id, err := result.LastInsertId()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), id)

		// Test RowsAffected
		affected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), affected)

		// Test Error method
		assert.NoError(t, result.Error)

		// Test Empty method
		assert.False(t, result.Empty())
	})

	t.Run("Into", func(t *testing.T) {
		now := time.Now()
		result := &Result{
			Data: []map[string]interface{}{
				{
					"id":         int64(1),
					"name":       "Test 1",
					"age":        int64(25),
					"is_active":  true,
					"score":      float64(85.5),
					"created_at": now,
					"tags":       []string{"tag1", "tag2"},
					"metadata":   map[string]interface{}{"key": "value"},
				},
			},
		}

		var models []TestResultModel
		err := result.Into(&models)
		assert.NoError(t, err)
		assert.Len(t, models, 1)

		model := models[0]
		assert.Equal(t, int64(1), model.ID)
		assert.Equal(t, "Test 1", model.Name)
		assert.Equal(t, int64(25), model.Age)
		assert.True(t, model.IsActive)
		assert.Equal(t, float64(85.5), model.Score)
		assert.Equal(t, now.Unix(), model.CreatedAt.Unix())
		assert.Equal(t, []string{"tag1", "tag2"}, model.Tags)
		assert.Equal(t, "value", model.Metadata["key"])
	})

	t.Run("IntoMap", func(t *testing.T) {
		result := &Result{
			Data: []map[string]interface{}{
				{"id": 1, "name": "Test"},
			},
		}

		m, err := result.IntoMap()
		assert.NoError(t, err)
		assert.Equal(t, 1, m["id"])
		assert.Equal(t, "Test", m["name"])
	})

	t.Run("IntoMaps", func(t *testing.T) {
		result := &Result{
			Data: []map[string]interface{}{
				{"id": 1, "name": "Test 1"},
				{"id": 2, "name": "Test 2"},
			},
		}

		maps, err := result.IntoMaps()
		assert.NoError(t, err)
		assert.Len(t, maps, 2)
		assert.Equal(t, 1, maps[0]["id"])
		assert.Equal(t, "Test 1", maps[0]["name"])
		assert.Equal(t, 2, maps[1]["id"])
		assert.Equal(t, "Test 2", maps[1]["name"])
	})
}

func TestResultErrorHandling(t *testing.T) {
	t.Run("Error Methods", func(t *testing.T) {
		result := &Result{Error: sql.ErrNoRows}

		// Test LastInsertId with error
		_, err := result.LastInsertId()
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)

		// Test RowsAffected with error
		_, err = result.RowsAffected()
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)

		// Test Error method
		assert.Error(t, result.Error)
		assert.Equal(t, sql.ErrNoRows, result.Error)
	})

	t.Run("Error Into", func(t *testing.T) {
		result := &Result{Error: sql.ErrNoRows}

		var models []TestResultModel
		err := result.Into(&models)
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("Error IntoMap", func(t *testing.T) {
		result := &Result{Error: sql.ErrNoRows}

		_, err := result.IntoMap()
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)
	})

	t.Run("Error IntoMaps", func(t *testing.T) {
		result := &Result{Error: sql.ErrNoRows}

		_, err := result.IntoMaps()
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)
	})
}

func TestResultTypeConversion(t *testing.T) {
	type ConversionModel struct {
		BoolFromInt    bool                   `gom:"bool_from_int"`
		BoolFromString bool                   `gom:"bool_from_string"`
		BoolFromBytes  bool                   `gom:"bool_from_bytes"`
		IntFromString  int64                  `gom:"int_from_string"`
		FloatFromInt   float64                `gom:"float_from_int"`
		JSONString     map[string]interface{} `gom:"json_string"`
		JSONBytes      []string               `gom:"json_bytes"`
	}

	result := &Result{
		Data: []map[string]interface{}{
			{
				"bool_from_int":    int64(1),
				"bool_from_string": "true",
				"bool_from_bytes":  "1",
				"int_from_string":  "123",
				"float_from_int":   int64(123),
				"json_string":      `{"key":"value"}`,
				"json_bytes":       []byte(`["item1","item2"]`),
			},
		},
	}

	var models []ConversionModel
	err := result.Into(&models)
	assert.NoError(t, err)
	assert.Len(t, models, 1)

	model := models[0]
	assert.True(t, model.BoolFromInt)
	assert.True(t, model.BoolFromString)
	assert.True(t, model.BoolFromBytes)
	assert.Equal(t, int64(123), model.IntFromString)
	assert.Equal(t, float64(123), model.FloatFromInt)
	assert.Equal(t, "value", model.JSONString["key"])
	assert.Equal(t, []string{"item1", "item2"}, model.JSONBytes)
}

func TestResultEdgeCases(t *testing.T) {
	t.Run("Nil Result", func(t *testing.T) {
		var result *Result
		var models []TestResultModel
		err := result.Into(&models)
		assert.Error(t, err)
		assert.Equal(t, "result is nil", err.Error())
		assert.Len(t, models, 0)
	})

	t.Run("Invalid Destination", func(t *testing.T) {
		result := &Result{
			Data: []map[string]interface{}{{"id": 1}},
		}

		// Non-pointer destination
		var models []TestResultModel
		err := result.Into(models)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "destination must be a non-nil pointer")

		// Nil pointer
		err = result.Into(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "destination must be a non-nil pointer")

	})

	t.Run("Empty Result", func(t *testing.T) {
		result := &Result{}
		assert.True(t, result.Empty())

		// Test First with empty result
		first := result.First()
		assert.Error(t, first.Error)
		assert.Equal(t, sql.ErrNoRows, first.Error)
	})

	t.Run("JSON Conversion", func(t *testing.T) {
		result := &Result{
			ID:       1,
			Affected: 2,
			Data: []map[string]interface{}{
				{"id": 1, "name": "Test"},
			},
			Columns: []string{"id", "name"},
		}

		// Test ToJSON
		jsonStr, err := result.ToJSON()
		assert.NoError(t, err)

		// Verify JSON structure
		var decoded struct {
			ID       int64                    `json:"ID"`
			Affected int64                    `json:"Affected"`
			Data     []map[string]interface{} `json:"Data"`
			Columns  []string                 `json:"Columns"`
		}
		err = json.Unmarshal([]byte(jsonStr), &decoded)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), decoded.ID)
		assert.Equal(t, int64(2), decoded.Affected)
		assert.Len(t, decoded.Data, 1)
		assert.Equal(t, float64(1), decoded.Data[0]["id"])
		assert.Equal(t, "Test", decoded.Data[0]["name"])
		assert.Equal(t, []string{"id", "name"}, decoded.Columns)
	})
}

func TestResultFirst(t *testing.T) {
	t.Run("With Data", func(t *testing.T) {
		result := &Result{
			Data: []map[string]interface{}{
				{"id": 1, "name": "First"},
				{"id": 2, "name": "Second"},
			},
		}

		first := result.First()
		assert.NoError(t, first.Error)
		assert.Len(t, first.Data, 1)
		assert.Equal(t, 1, first.Data[0]["id"])
		assert.Equal(t, "First", first.Data[0]["name"])
	})

	t.Run("Empty Data", func(t *testing.T) {
		result := &Result{
			Data: []map[string]interface{}{},
		}

		first := result.First()
		assert.Error(t, first.Error)
		assert.Equal(t, sql.ErrNoRows, first.Error)
	})

	t.Run("With Error", func(t *testing.T) {
		result := &Result{
			Error: sql.ErrConnDone,
		}

		first := result.First()
		assert.Error(t, first.Error)
		assert.Equal(t, sql.ErrConnDone, first.Error)
	})
}

func TestResultJSON(t *testing.T) {
	t.Run("Valid JSON", func(t *testing.T) {
		result := &Result{
			ID:       1,
			Affected: 2,
			Data: []map[string]interface{}{
				{
					"id":   1,
					"name": "Test",
					"tags": []string{"tag1", "tag2"},
					"metadata": map[string]interface{}{
						"key": "value",
					},
				},
			},
			Columns: []string{"id", "name", "tags", "metadata"},
		}

		jsonStr, err := result.ToJSON()
		assert.NoError(t, err)

		var decoded Result
		err = json.Unmarshal([]byte(jsonStr), &decoded)
		assert.NoError(t, err)
		assert.Equal(t, result.ID, decoded.ID)
		assert.Equal(t, result.Affected, decoded.Affected)
		assert.Equal(t, len(result.Data), len(decoded.Data))
		assert.Equal(t, result.Columns, decoded.Columns)
	})

	t.Run("Error Result", func(t *testing.T) {
		result := &Result{
			Error: sql.ErrNoRows,
		}

		_, err := result.ToJSON()
		assert.Error(t, err)
		assert.Equal(t, sql.ErrNoRows, err)
	})
}

func TestResultNullTypeConversion(t *testing.T) {
	type NullValueModel struct {
		NullString  sql.NullString  `gom:"null_string"`
		NullInt     sql.NullInt64   `gom:"null_int"`
		NullFloat   sql.NullFloat64 `gom:"null_float"`
		NullBool    sql.NullBool    `gom:"null_bool"`
		NullTime    sql.NullTime    `gom:"null_time"`
		NonNullText string          `gom:"text"`
	}

	result := &Result{
		Data: []map[string]interface{}{
			{
				"null_string": sql.NullString{Valid: false},
				"null_int":    sql.NullInt64{Valid: false},
				"null_float":  sql.NullFloat64{Valid: false},
				"null_bool":   sql.NullBool{Valid: false},
				"null_time":   sql.NullTime{Valid: false},
				"text":        "Not null value",
			},
		},
	}

	var model NullValueModel
	err := result.Into(&model)
	assert.NoError(t, err)

	assert.False(t, model.NullString.Valid)
	assert.False(t, model.NullInt.Valid)
	assert.False(t, model.NullFloat.Valid)
	assert.False(t, model.NullBool.Valid)
	assert.False(t, model.NullTime.Valid)
	assert.Equal(t, "Not null value", model.NonNullText)
}
