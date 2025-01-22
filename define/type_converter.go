package define

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
)

// ConversionError represents an error that occurs during type conversion
type ConversionError struct {
	From    string
	To      string
	Value   interface{}
	Message string
}

func (e *ConversionError) Error() string {
	return fmt.Sprintf("cannot convert %v (type %s) to type %s: %s", e.Value, e.From, e.To, e.Message)
}

// ITypeConverter defines the interface for type conversion
type ITypeConverter interface {
	Convert(value interface{}, targetType reflect.Type) (interface{}, error)
}

// typeConverter implements ITypeConverter
type typeConverter struct{}

// NewTypeConverter creates a new ITypeConverter instance
func NewTypeConverter() ITypeConverter {
	return &typeConverter{}
}

// Convert converts a value to the target type
func (c *typeConverter) Convert(value interface{}, targetType reflect.Type) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	valueType := reflect.TypeOf(value)
	if valueType == targetType {
		return value, nil
	}

	switch targetType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return c.convertToInt(value, targetType)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return c.convertToUint(value, targetType)
	case reflect.Float32, reflect.Float64:
		return c.convertToFloat(value, targetType)
	case reflect.String:
		return c.convertToString(value)
	case reflect.Bool:
		return c.convertToBool(value)
	case reflect.Struct:
		if targetType == reflect.TypeOf(time.Time{}) {
			return c.convertToTime(value)
		}
	case reflect.Slice:
		if targetType.Elem().Kind() == reflect.Uint8 {
			return c.convertToBytes(value)
		}
	}

	return nil, &ConversionError{
		From:    valueType.String(),
		To:      targetType.String(),
		Value:   value,
		Message: "unsupported type conversion",
	}
}

func (c *typeConverter) convertToInt(value interface{}, targetType reflect.Type) (interface{}, error) {
	var i int64
	var err error

	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		i = reflect.ValueOf(v).Int()
	case uint, uint8, uint16, uint32, uint64:
		u := reflect.ValueOf(v).Uint()
		if u > math.MaxInt64 {
			return nil, &ConversionError{
				From:    reflect.TypeOf(v).String(),
				To:      targetType.String(),
				Value:   v,
				Message: "unsigned integer value exceeds maximum signed integer",
			}
		}
		i = int64(u)
	case float32, float64:
		f := reflect.ValueOf(v).Float()
		if f > math.MaxInt64 || f < math.MinInt64 {
			return nil, &ConversionError{
				From:    reflect.TypeOf(v).String(),
				To:      targetType.String(),
				Value:   v,
				Message: "float value out of integer range",
			}
		}
		i = int64(f)
	case string:
		i, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, &ConversionError{
				From:    "string",
				To:      targetType.String(),
				Value:   v,
				Message: err.Error(),
			}
		}
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      targetType.String(),
			Value:   v,
			Message: "unsupported type for integer conversion",
		}
	}

	switch targetType.Kind() {
	case reflect.Int:
		if i > math.MaxInt || i < math.MinInt {
			return nil, &ConversionError{
				From:    "int64",
				To:      "int",
				Value:   i,
				Message: "value out of range",
			}
		}
		return int(i), nil
	case reflect.Int8:
		if i > math.MaxInt8 || i < math.MinInt8 {
			return nil, &ConversionError{
				From:    "int64",
				To:      "int8",
				Value:   i,
				Message: "value out of range",
			}
		}
		return int8(i), nil
	case reflect.Int16:
		if i > math.MaxInt16 || i < math.MinInt16 {
			return nil, &ConversionError{
				From:    "int64",
				To:      "int16",
				Value:   i,
				Message: "value out of range",
			}
		}
		return int16(i), nil
	case reflect.Int32:
		if i > math.MaxInt32 || i < math.MinInt32 {
			return nil, &ConversionError{
				From:    "int64",
				To:      "int32",
				Value:   i,
				Message: "value out of range",
			}
		}
		return int32(i), nil
	case reflect.Int64:
		return i, nil
	}

	return nil, &ConversionError{
		From:    "int64",
		To:      targetType.String(),
		Value:   i,
		Message: "unsupported integer target type",
	}
}

func (c *typeConverter) convertToUint(value interface{}, targetType reflect.Type) (interface{}, error) {
	var u uint64
	var err error

	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		i := reflect.ValueOf(v).Int()
		if i < 0 {
			return nil, &ConversionError{
				From:    reflect.TypeOf(v).String(),
				To:      targetType.String(),
				Value:   v,
				Message: "negative integer cannot be converted to unsigned",
			}
		}
		u = uint64(i)
	case uint, uint8, uint16, uint32, uint64:
		u = reflect.ValueOf(v).Uint()
	case float32, float64:
		f := reflect.ValueOf(v).Float()
		if f < 0 || f > math.MaxUint64 {
			return nil, &ConversionError{
				From:    reflect.TypeOf(v).String(),
				To:      targetType.String(),
				Value:   v,
				Message: "float value out of unsigned integer range",
			}
		}
		u = uint64(f)
	case string:
		u, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, &ConversionError{
				From:    "string",
				To:      targetType.String(),
				Value:   v,
				Message: err.Error(),
			}
		}
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      targetType.String(),
			Value:   v,
			Message: "unsupported type for unsigned integer conversion",
		}
	}

	switch targetType.Kind() {
	case reflect.Uint:
		if u > math.MaxUint {
			return nil, &ConversionError{
				From:    "uint64",
				To:      "uint",
				Value:   u,
				Message: "value out of range",
			}
		}
		return uint(u), nil
	case reflect.Uint8:
		if u > math.MaxUint8 {
			return nil, &ConversionError{
				From:    "uint64",
				To:      "uint8",
				Value:   u,
				Message: "value out of range",
			}
		}
		return uint8(u), nil
	case reflect.Uint16:
		if u > math.MaxUint16 {
			return nil, &ConversionError{
				From:    "uint64",
				To:      "uint16",
				Value:   u,
				Message: "value out of range",
			}
		}
		return uint16(u), nil
	case reflect.Uint32:
		if u > math.MaxUint32 {
			return nil, &ConversionError{
				From:    "uint64",
				To:      "uint32",
				Value:   u,
				Message: "value out of range",
			}
		}
		return uint32(u), nil
	case reflect.Uint64:
		return u, nil
	}

	return nil, &ConversionError{
		From:    "uint64",
		To:      targetType.String(),
		Value:   u,
		Message: "unsupported unsigned integer target type",
	}
}

func (c *typeConverter) convertToFloat(value interface{}, targetType reflect.Type) (interface{}, error) {
	var f float64

	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		f = float64(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		f = float64(reflect.ValueOf(v).Uint())
	case float32, float64:
		f = reflect.ValueOf(v).Float()
	case string:
		var err error
		f, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, &ConversionError{
				From:    "string",
				To:      targetType.String(),
				Value:   v,
				Message: err.Error(),
			}
		}
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      targetType.String(),
			Value:   v,
			Message: "unsupported type for float conversion",
		}
	}

	switch targetType.Kind() {
	case reflect.Float32:
		if f > math.MaxFloat32 || f < -math.MaxFloat32 {
			return nil, &ConversionError{
				From:    "float64",
				To:      "float32",
				Value:   f,
				Message: "value out of range",
			}
		}
		return float32(f), nil
	case reflect.Float64:
		return f, nil
	}

	return nil, &ConversionError{
		From:    "float64",
		To:      targetType.String(),
		Value:   f,
		Message: "unsupported float target type",
	}
}

func (c *typeConverter) convertToString(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return fmt.Sprintf("%v", v), nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      "string",
			Value:   v,
			Message: "unsupported type for string conversion",
		}
	}
}

func (c *typeConverter) convertToBool(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		i := reflect.ValueOf(v).Int()
		return i != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		u := reflect.ValueOf(v).Uint()
		return u != 0, nil
	case float32, float64:
		f := reflect.ValueOf(v).Float()
		return f != 0, nil
	case string:
		return strconv.ParseBool(v)
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      "bool",
			Value:   v,
			Message: "unsupported type for boolean conversion",
		}
	}
}

func (c *typeConverter) convertToTime(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// Try common time formats
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return nil, &ConversionError{
			From:    "string",
			To:      "time.Time",
			Value:   v,
			Message: "unsupported time format",
		}
	case int64:
		return time.Unix(v, 0), nil
	case float64:
		sec, dec := math.Modf(v)
		return time.Unix(int64(sec), int64(dec*1e9)), nil
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      "time.Time",
			Value:   v,
			Message: "unsupported type for time conversion",
		}
	}
}

func (c *typeConverter) convertToBytes(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, &ConversionError{
			From:    reflect.TypeOf(v).String(),
			To:      "[]byte",
			Value:   v,
			Message: "unsupported type for bytes conversion",
		}
	}
}
