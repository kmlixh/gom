package define

import (
	"fmt"
	"strconv"
	"time"
)

// DataRow represents a single row of data from Result
type DataRow struct {
	data map[string]any
}

// GetDataMap returns the underlying map data
func (r *DataRow) GetDataMap() map[string]any {
	return r.data
}

// GetString returns the string value for the given column name
func (r *DataRow) GetString(name string) string {
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		default:
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

// GetInt returns the int value for the given column name
func (r *DataRow) GetInt(name string) int {
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case int:
			return v
		case int64:
			return int(v)
		case string:
			if i, err := strconv.Atoi(v); err == nil {
				return i
			}
		case []byte:
			if i, err := strconv.Atoi(string(v)); err == nil {
				return i
			}
		}
	}
	return 0
}

// GetInt64 returns the int64 value for the given column name
func (r *DataRow) GetInt64(name string) int64 {
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		case []byte:
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				return i
			}
		}
	}
	return 0
}

// GetFloat64 returns the float64 value for the given column name
func (r *DataRow) GetFloat64(name string) float64 {
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		case []byte:
			if f, err := strconv.ParseFloat(string(v), 64); err == nil {
				return f
			}
		}
	}
	return 0
}

// GetBool returns the bool value for the given column name
func (r *DataRow) GetBool(name string) bool {
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case bool:
			return v
		case int:
			return v != 0
		case string:
			return v == "1" || v == "true" || v == "True"
		case []byte:
			s := string(v)
			return s == "1" || s == "true" || s == "True"
		}
	}
	return false
}

// GetTime returns the time.Time value for the given column name
func (r *DataRow) GetTime(name string) time.Time {
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case time.Time:
			return v
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return t
			}
		case []byte:
			if t, err := time.Parse(time.RFC3339, string(v)); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}
