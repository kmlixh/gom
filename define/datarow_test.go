package define

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataRow(t *testing.T) {
	now := time.Now()
	data := map[string]any{
		"string_val":     "test",
		"int_val":        42,
		"int64_val":      int64(42),
		"float64_val":    42.42,
		"bool_val":       true,
		"time_val":       now,
		"bytes_val":      []byte("test"),
		"map_val":        map[string]interface{}{"key": "value"},
		"slice_val":      []interface{}{"item1", "item2"},
		"null_val":       nil,
		"json_map_val":   `{"key":"value"}`,
		"json_slice_val": `["item1","item2"]`,
		"string_bytes":   []byte("test string"),
		"int_bytes":      []byte("42"),
		"float64_bytes":  []byte("42.42"),
		"bool_bytes_1":   []byte{1},
		"bool_bytes_0":   []byte{0},
		"bool_true":      []byte("true"),
		"bool_yes":       []byte("yes"),
		"bool_1":         []byte("1"),
		"bool_on":        []byte("on"),
		"time_bytes":     []byte(now.Format(time.RFC3339)),
		"invalid_int":    []byte("not a number"),
		"invalid_float":  []byte("not a float"),
		"invalid_bool":   []byte("not a bool"),
		"invalid_time":   []byte("not a time"),
		"invalid_json":   []byte("not a json"),
	}

	row := &DataRow{data: data}

	t.Run("GetDataMap", func(t *testing.T) {
		assert.Equal(t, data, row.GetDataMap())
	})

	t.Run("GetString", func(t *testing.T) {
		val, err := row.GetString("string_val")
		assert.NoError(t, err)
		assert.Equal(t, "test", val)

		val, err = row.GetString("string_bytes")
		assert.NoError(t, err)
		assert.Equal(t, "test string", val)

		val, err = row.GetString("non_existent")
		assert.Error(t, err)
		assert.Equal(t, "", val)

		val = row.GetStringOrDefault("non_existent", "default")
		assert.Equal(t, "default", val)
	})

	t.Run("GetInt", func(t *testing.T) {
		val, err := row.GetInt("int_val")
		assert.NoError(t, err)
		assert.Equal(t, 42, val)

		val, err = row.GetInt("int_bytes")
		assert.NoError(t, err)
		assert.Equal(t, 42, val)

		val, err = row.GetInt("invalid_int")
		assert.Error(t, err)
		assert.Equal(t, 0, val)

		val = row.GetIntOrDefault("non_existent", 42)
		assert.Equal(t, 42, val)
	})

	t.Run("GetInt64", func(t *testing.T) {
		val, err := row.GetInt64("int64_val")
		assert.NoError(t, err)
		assert.Equal(t, int64(42), val)

		val, err = row.GetInt64("int_bytes")
		assert.NoError(t, err)
		assert.Equal(t, int64(42), val)

		val, err = row.GetInt64("invalid_int")
		assert.Error(t, err)
		assert.Equal(t, int64(0), val)

		val = row.GetInt64OrDefault("non_existent", 42)
		assert.Equal(t, int64(42), val)
	})

	t.Run("GetFloat64", func(t *testing.T) {
		val, err := row.GetFloat64("float64_val")
		assert.NoError(t, err)
		assert.Equal(t, 42.42, val)

		val, err = row.GetFloat64("float64_bytes")
		assert.NoError(t, err)
		assert.Equal(t, 42.42, val)

		val, err = row.GetFloat64("invalid_float")
		assert.Error(t, err)
		assert.Equal(t, float64(0), val)

		val = row.GetFloat64OrDefault("non_existent", 42.42)
		assert.Equal(t, 42.42, val)
	})

	t.Run("GetBool", func(t *testing.T) {
		val, err := row.GetBool("bool_val")
		assert.NoError(t, err)
		assert.True(t, val)

		val, err = row.GetBool("bool_bytes_1")
		assert.NoError(t, err)
		assert.True(t, val)

		val, err = row.GetBool("bool_bytes_0")
		assert.NoError(t, err)
		assert.False(t, val)

		val, err = row.GetBool("bool_true")
		assert.NoError(t, err)
		assert.True(t, val)

		val, err = row.GetBool("bool_yes")
		assert.NoError(t, err)
		assert.True(t, val)

		val, err = row.GetBool("bool_1")
		assert.NoError(t, err)
		assert.True(t, val)

		val, err = row.GetBool("bool_on")
		assert.NoError(t, err)
		assert.True(t, val)

		val, err = row.GetBool("invalid_bool")
		assert.Error(t, err)
		assert.False(t, val)

		val = row.GetBoolOrDefault("non_existent", true)
		assert.True(t, val)
	})

	t.Run("GetTime", func(t *testing.T) {
		val, err := row.GetTime("time_val")
		assert.NoError(t, err)
		assert.Equal(t, now.Unix(), val.Unix())

		val, err = row.GetTime("time_bytes")
		assert.NoError(t, err)
		assert.Equal(t, now.Unix(), val.Unix())

		val, err = row.GetTime("invalid_time")
		assert.Error(t, err)
		assert.Equal(t, time.Time{}, val)

		val = row.GetTimeOrDefault("non_existent", now)
		assert.Equal(t, now.Unix(), val.Unix())
	})

	t.Run("GetBytes", func(t *testing.T) {
		val, err := row.GetBytes("bytes_val")
		assert.NoError(t, err)
		assert.Equal(t, []byte("test"), val)

		val, err = row.GetBytes("string_val")
		assert.NoError(t, err)
		assert.Equal(t, []byte("test"), val)

		val, err = row.GetBytes("non_existent")
		assert.Error(t, err)
		assert.Nil(t, val)

		val = row.GetBytesOrDefault("non_existent", []byte("default"))
		assert.Equal(t, []byte("default"), val)
	})

	t.Run("GetMap", func(t *testing.T) {
		val, err := row.GetMap("map_val")
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"key": "value"}, val)

		val, err = row.GetMap("json_map_val")
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"key": "value"}, val)

		val, err = row.GetMap("invalid_json")
		assert.Error(t, err)
		assert.Nil(t, val)

		val = row.GetMapOrDefault("non_existent", map[string]interface{}{"key": "default"})
		assert.Equal(t, map[string]interface{}{"key": "default"}, val)
	})

	t.Run("GetSlice", func(t *testing.T) {
		val, err := row.GetSlice("slice_val")
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{"item1", "item2"}, val)

		val, err = row.GetSlice("json_slice_val")
		assert.NoError(t, err)
		assert.Equal(t, []interface{}{"item1", "item2"}, val)

		val, err = row.GetSlice("invalid_json")
		assert.Error(t, err)
		assert.Nil(t, val)

		val = row.GetSliceOrDefault("non_existent", []interface{}{"default"})
		assert.Equal(t, []interface{}{"default"}, val)
	})

	t.Run("GetNullString", func(t *testing.T) {
		val, err := row.GetNullString("string_val")
		assert.NoError(t, err)
		assert.True(t, val.Valid)
		assert.Equal(t, "test", val.String)

		val, err = row.GetNullString("null_val")
		assert.NoError(t, err)
		assert.False(t, val.Valid)

		val, err = row.GetNullString("non_existent")
		assert.Error(t, err)
		assert.False(t, val.Valid)
	})

	t.Run("GetNullInt64", func(t *testing.T) {
		val, err := row.GetNullInt64("int64_val")
		assert.NoError(t, err)
		assert.True(t, val.Valid)
		assert.Equal(t, int64(42), val.Int64)

		val, err = row.GetNullInt64("null_val")
		assert.NoError(t, err)
		assert.False(t, val.Valid)

		val, err = row.GetNullInt64("non_existent")
		assert.Error(t, err)
		assert.False(t, val.Valid)
	})

	t.Run("GetNullFloat64", func(t *testing.T) {
		val, err := row.GetNullFloat64("float64_val")
		assert.NoError(t, err)
		assert.True(t, val.Valid)
		assert.Equal(t, 42.42, val.Float64)

		val, err = row.GetNullFloat64("null_val")
		assert.NoError(t, err)
		assert.False(t, val.Valid)

		val, err = row.GetNullFloat64("non_existent")
		assert.Error(t, err)
		assert.False(t, val.Valid)
	})

	t.Run("GetNullBool", func(t *testing.T) {
		val, err := row.GetNullBool("bool_val")
		assert.NoError(t, err)
		assert.True(t, val.Valid)
		assert.True(t, val.Bool)

		val, err = row.GetNullBool("null_val")
		assert.NoError(t, err)
		assert.False(t, val.Valid)

		val, err = row.GetNullBool("non_existent")
		assert.Error(t, err)
		assert.False(t, val.Valid)
	})

	t.Run("GetNullTime", func(t *testing.T) {
		val, err := row.GetNullTime("time_val")
		assert.NoError(t, err)
		assert.True(t, val.Valid)
		assert.Equal(t, now.Unix(), val.Time.Unix())

		val, err = row.GetNullTime("null_val")
		assert.NoError(t, err)
		assert.False(t, val.Valid)

		val, err = row.GetNullTime("non_existent")
		assert.Error(t, err)
		assert.False(t, val.Valid)
	})

	t.Run("Nil Data", func(t *testing.T) {
		nilRow := &DataRow{data: nil}

		_, err := nilRow.GetString("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetInt("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetInt64("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetFloat64("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetBool("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetTime("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetBytes("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetMap("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetSlice("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetNullString("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetNullInt64("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetNullFloat64("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetNullBool("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())

		_, err = nilRow.GetNullTime("any")
		assert.Error(t, err)
		assert.Equal(t, "no data available", err.Error())
	})
}
