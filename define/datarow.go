package define

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
func (r *DataRow) GetString(name string) (string, error) {
	if r.data == nil {
		return "", fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	}
	return "", fmt.Errorf("column %s not found", name)
}

// GetStringOrDefault returns the string value for the given column name or default value if not found
func (r *DataRow) GetStringOrDefault(name string, defaultValue string) string {
	if val, err := r.GetString(name); err == nil {
		return val
	}
	return defaultValue
}

// GetInt returns the int value for the given column name
func (r *DataRow) GetInt(name string) (int, error) {
	if r.data == nil {
		return 0, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case int:
			return v, nil
		case int64:
			return int(v), nil
		case string:
			return strconv.Atoi(v)
		case []byte:
			return strconv.Atoi(string(v))
		default:
			return 0, fmt.Errorf("cannot convert %T to int", val)
		}
	}
	return 0, fmt.Errorf("column %s not found", name)
}

// GetIntOrDefault returns the int value for the given column name or default value if not found
func (r *DataRow) GetIntOrDefault(name string, defaultValue int) int {
	if val, err := r.GetInt(name); err == nil {
		return val
	}
	return defaultValue
}

// GetInt64 returns the int64 value for the given column name
func (r *DataRow) GetInt64(name string) (int64, error) {
	if r.data == nil {
		return 0, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		case []byte:
			return strconv.ParseInt(string(v), 10, 64)
		default:
			return 0, fmt.Errorf("cannot convert %T to int64", val)
		}
	}
	return 0, fmt.Errorf("column %s not found", name)
}

// GetInt64OrDefault returns the int64 value for the given column name or default value if not found
func (r *DataRow) GetInt64OrDefault(name string, defaultValue int64) int64 {
	if val, err := r.GetInt64(name); err == nil {
		return val
	}
	return defaultValue
}

// GetFloat64 returns the float64 value for the given column name
func (r *DataRow) GetFloat64(name string) (float64, error) {
	if r.data == nil {
		return 0, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case int:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		case []byte:
			return strconv.ParseFloat(string(v), 64)
		default:
			return 0, fmt.Errorf("cannot convert %T to float64", val)
		}
	}
	return 0, fmt.Errorf("column %s not found", name)
}

// GetFloat64OrDefault returns the float64 value for the given column name or default value if not found
func (r *DataRow) GetFloat64OrDefault(name string, defaultValue float64) float64 {
	if val, err := r.GetFloat64(name); err == nil {
		return val
	}
	return defaultValue
}

// GetBool returns the bool value for the given column name
func (r *DataRow) GetBool(name string) (bool, error) {
	if r.data == nil {
		return false, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case bool:
			return v, nil
		case int64:
			return v != 0, nil
		case int:
			return v != 0, nil
		case string:
			s := strings.ToLower(v)
			if s == "true" || s == "1" || s == "yes" || s == "on" {
				return true, nil
			}
			if s == "false" || s == "0" || s == "no" || s == "off" {
				return false, nil
			}
			return strconv.ParseBool(v)
		case []byte:
			if len(v) == 1 {
				return v[0] != 0, nil
			}
			s := strings.ToLower(string(v))
			if s == "true" || s == "1" || s == "yes" || s == "on" {
				return true, nil
			}
			if s == "false" || s == "0" || s == "no" || s == "off" {
				return false, nil
			}
			return strconv.ParseBool(s)
		default:
			return false, fmt.Errorf("cannot convert %T to bool", val)
		}
	}
	return false, fmt.Errorf("column %s not found", name)
}

// GetBoolOrDefault returns the bool value for the given column name or default value if not found
func (r *DataRow) GetBoolOrDefault(name string, defaultValue bool) bool {
	if val, err := r.GetBool(name); err == nil {
		return val
	}
	return defaultValue
}

// GetTime returns the time.Time value for the given column name
func (r *DataRow) GetTime(name string) (time.Time, error) {
	if r.data == nil {
		return time.Time{}, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case time.Time:
			return v, nil
		case string:
			return time.Parse(time.RFC3339, v)
		case []byte:
			return time.Parse(time.RFC3339, string(v))
		default:
			return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", val)
		}
	}
	return time.Time{}, fmt.Errorf("column %s not found", name)
}

// GetTimeOrDefault returns the time.Time value for the given column name or default value if not found
func (r *DataRow) GetTimeOrDefault(name string, defaultValue time.Time) time.Time {
	if val, err := r.GetTime(name); err == nil {
		return val
	}
	return defaultValue
}

// GetBytes returns the []byte value for the given column name
func (r *DataRow) GetBytes(name string) ([]byte, error) {
	if r.data == nil {
		return nil, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case []byte:
			return v, nil
		case string:
			return []byte(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to []byte", val)
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

// GetBytesOrDefault returns the []byte value for the given column name or default value if not found
func (r *DataRow) GetBytesOrDefault(name string, defaultValue []byte) []byte {
	if val, err := r.GetBytes(name); err == nil {
		return val
	}
	return defaultValue
}

// GetMap returns the map[string]interface{} value for the given column name
func (r *DataRow) GetMap(name string) (map[string]interface{}, error) {
	if r.data == nil {
		return nil, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case map[string]interface{}:
			return v, nil
		case string:
			var result map[string]interface{}
			if err := json.Unmarshal([]byte(v), &result); err != nil {
				return nil, err
			}
			return result, nil
		case []byte:
			var result map[string]interface{}
			if err := json.Unmarshal(v, &result); err != nil {
				return nil, err
			}
			return result, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to map[string]interface{}", val)
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

// GetMapOrDefault returns the map[string]interface{} value for the given column name or default value if not found
func (r *DataRow) GetMapOrDefault(name string, defaultValue map[string]interface{}) map[string]interface{} {
	if val, err := r.GetMap(name); err == nil {
		return val
	}
	return defaultValue
}

// GetSlice returns the []interface{} value for the given column name
func (r *DataRow) GetSlice(name string) ([]interface{}, error) {
	if r.data == nil {
		return nil, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		switch v := val.(type) {
		case []interface{}:
			return v, nil
		case string:
			var result []interface{}
			if err := json.Unmarshal([]byte(v), &result); err != nil {
				return nil, err
			}
			return result, nil
		case []byte:
			var result []interface{}
			if err := json.Unmarshal(v, &result); err != nil {
				return nil, err
			}
			return result, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to []interface{}", val)
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

// GetSliceOrDefault returns the []interface{} value for the given column name or default value if not found
func (r *DataRow) GetSliceOrDefault(name string, defaultValue []interface{}) []interface{} {
	if val, err := r.GetSlice(name); err == nil {
		return val
	}
	return defaultValue
}

// GetNullString returns the sql.NullString value for the given column name
func (r *DataRow) GetNullString(name string) (sql.NullString, error) {
	if r.data == nil {
		return sql.NullString{}, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		if val == nil {
			return sql.NullString{Valid: false}, nil
		}
		switch v := val.(type) {
		case string:
			return sql.NullString{String: v, Valid: true}, nil
		case []byte:
			return sql.NullString{String: string(v), Valid: true}, nil
		default:
			return sql.NullString{String: fmt.Sprintf("%v", v), Valid: true}, nil
		}
	}
	return sql.NullString{}, fmt.Errorf("column %s not found", name)
}

// GetNullInt64 returns the sql.NullInt64 value for the given column name
func (r *DataRow) GetNullInt64(name string) (sql.NullInt64, error) {
	if r.data == nil {
		return sql.NullInt64{}, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		if val == nil {
			return sql.NullInt64{Valid: false}, nil
		}
		switch v := val.(type) {
		case int64:
			return sql.NullInt64{Int64: v, Valid: true}, nil
		case int:
			return sql.NullInt64{Int64: int64(v), Valid: true}, nil
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return sql.NullInt64{Int64: i, Valid: true}, nil
			}
		case []byte:
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				return sql.NullInt64{Int64: i, Valid: true}, nil
			}
		}
		return sql.NullInt64{Valid: false}, nil
	}
	return sql.NullInt64{}, fmt.Errorf("column %s not found", name)
}

// GetNullFloat64 returns the sql.NullFloat64 value for the given column name
func (r *DataRow) GetNullFloat64(name string) (sql.NullFloat64, error) {
	if r.data == nil {
		return sql.NullFloat64{}, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		if val == nil {
			return sql.NullFloat64{Valid: false}, nil
		}
		switch v := val.(type) {
		case float64:
			return sql.NullFloat64{Float64: v, Valid: true}, nil
		case float32:
			return sql.NullFloat64{Float64: float64(v), Valid: true}, nil
		case int64:
			return sql.NullFloat64{Float64: float64(v), Valid: true}, nil
		case int:
			return sql.NullFloat64{Float64: float64(v), Valid: true}, nil
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return sql.NullFloat64{Float64: f, Valid: true}, nil
			}
		case []byte:
			if f, err := strconv.ParseFloat(string(v), 64); err == nil {
				return sql.NullFloat64{Float64: f, Valid: true}, nil
			}
		}
		return sql.NullFloat64{Valid: false}, nil
	}
	return sql.NullFloat64{}, fmt.Errorf("column %s not found", name)
}

// GetNullBool returns the sql.NullBool value for the given column name
func (r *DataRow) GetNullBool(name string) (sql.NullBool, error) {
	if r.data == nil {
		return sql.NullBool{}, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		if val == nil {
			return sql.NullBool{Valid: false}, nil
		}
		switch v := val.(type) {
		case bool:
			return sql.NullBool{Bool: v, Valid: true}, nil
		case int64:
			return sql.NullBool{Bool: v != 0, Valid: true}, nil
		case int:
			return sql.NullBool{Bool: v != 0, Valid: true}, nil
		case string:
			s := strings.ToLower(v)
			if s == "true" || s == "1" || s == "yes" || s == "on" {
				return sql.NullBool{Bool: true, Valid: true}, nil
			}
			if s == "false" || s == "0" || s == "no" || s == "off" {
				return sql.NullBool{Bool: false, Valid: true}, nil
			}
			if b, err := strconv.ParseBool(v); err == nil {
				return sql.NullBool{Bool: b, Valid: true}, nil
			}
			return sql.NullBool{Valid: false}, nil
		case []byte:
			if len(v) == 1 {
				return sql.NullBool{Bool: v[0] != 0, Valid: true}, nil
			}
			s := strings.ToLower(string(v))
			if s == "true" || s == "1" || s == "yes" || s == "on" {
				return sql.NullBool{Bool: true, Valid: true}, nil
			}
			if s == "false" || s == "0" || s == "no" || s == "off" {
				return sql.NullBool{Bool: false, Valid: true}, nil
			}
			if b, err := strconv.ParseBool(s); err == nil {
				return sql.NullBool{Bool: b, Valid: true}, nil
			}
			return sql.NullBool{Valid: false}, nil
		}
		return sql.NullBool{Valid: false}, nil
	}
	return sql.NullBool{}, fmt.Errorf("column %s not found", name)
}

// GetNullTime returns the sql.NullTime value for the given column name
func (r *DataRow) GetNullTime(name string) (sql.NullTime, error) {
	if r.data == nil {
		return sql.NullTime{}, fmt.Errorf("no data available")
	}
	if val, ok := r.data[name]; ok {
		if val == nil {
			return sql.NullTime{Valid: false}, nil
		}
		switch v := val.(type) {
		case time.Time:
			return sql.NullTime{Time: v, Valid: true}, nil
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return sql.NullTime{Time: t, Valid: true}, nil
			}
		case []byte:
			if t, err := time.Parse(time.RFC3339, string(v)); err == nil {
				return sql.NullTime{Time: t, Valid: true}, nil
			}
		}
		return sql.NullTime{Valid: false}, nil
	}
	return sql.NullTime{}, fmt.Errorf("column %s not found", name)
}
