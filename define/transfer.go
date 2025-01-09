package define

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FieldInfo stores the mapping information for a single field
type FieldInfo struct {
	Index      int          // Field index in struct
	Name       string       // Field name in struct
	Column     string       // Column name in database
	Type       reflect.Type // Field type
	IsAuto     bool         // Is auto-increment field
	IsPrimary  bool         // Is primary key
	HasDefault bool         // Has default value
}

// ScannerInfo stores information for scanning database columns
type ScannerInfo struct {
	Index   int                           // Field index in struct
	Scanner interface{}                   // Pre-allocated scanner
	Convert func(interface{}) interface{} // Value converter function
}

// Transfer caches the mapping between struct and database table
type Transfer struct {
	TableName    string                    // Table name
	Fields       map[string]*FieldInfo     // Map of column name to field info
	FieldOrder   []string                  // Order of fields for consistent operations
	PrimaryKey   *FieldInfo                // Primary key field info
	model        interface{}               // Original model
	scannerCache map[string][]*ScannerInfo // Cache of column scanners
	mu           sync.RWMutex              // Mutex for concurrent access
}

// TypeConverter is the interface that wraps the basic type conversion methods
type TypeConverter interface {
	// FromDB converts a value from database format to Go type
	FromDB(value interface{}) error
	// ToDB converts a Go type to database format
	ToDB() (interface{}, error)
}

// cache for Transfer objects
var (
	transferCache = make(map[reflect.Type]*Transfer)
	cacheMutex    sync.RWMutex
)

// GetTransfer gets or creates a Transfer for the given struct type
func GetTransfer(model interface{}) *Transfer {
	if model == nil {
		return nil
	}

	modelType := reflect.TypeOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	// Try to get from cache first
	cacheMutex.RLock()
	if transfer, ok := transferCache[modelType]; ok {
		cacheMutex.RUnlock()
		return transfer
	}
	cacheMutex.RUnlock()

	// Create new Transfer
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	// Double check after acquiring write lock
	if transfer, ok := transferCache[modelType]; ok {
		return transfer
	}

	transfer := &Transfer{
		Fields:       make(map[string]*FieldInfo),
		FieldOrder:   make([]string, 0),
		scannerCache: make(map[string][]*ScannerInfo),
		model:        model,
	}

	// Set table name
	if namer, ok := model.(interface{ TableName() string }); ok {
		transfer.TableName = namer.TableName()
	} else {
		transfer.TableName = strings.ToLower(modelType.Name())
	}

	// Parse struct fields
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" {
			continue
		}

		parts := strings.Split(tag, ",")
		columnName := parts[0]
		if columnName == "" {
			continue
		}

		fieldInfo := &FieldInfo{
			Index:  i,
			Name:   field.Name,
			Column: columnName,
			Type:   field.Type,
		}

		// Parse tag options
		for _, opt := range parts[1:] {
			switch opt {
			case "auto":
				fieldInfo.IsAuto = true
			case "@":
				fieldInfo.IsPrimary = true
				transfer.PrimaryKey = fieldInfo
			case "default":
				fieldInfo.HasDefault = true
			}
		}

		transfer.Fields[columnName] = fieldInfo
		transfer.FieldOrder = append(transfer.FieldOrder, columnName)
	}

	transferCache[modelType] = transfer
	return transfer
}

// ToMap converts a struct to map using cached field information
func (t *Transfer) ToMap(model interface{}) map[string]interface{} {
	if model == nil {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	result := make(map[string]interface{})

	for _, columnName := range t.FieldOrder {
		fieldInfo := t.Fields[columnName]

		// Skip auto-increment fields
		if fieldInfo.IsAuto {
			continue
		}

		fieldValue := modelValue.Field(fieldInfo.Index)
		if !fieldValue.IsValid() {
			continue
		}

		// Handle zero values based on tag options
		isZero := reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldInfo.Type).Interface())
		if isZero && fieldInfo.HasDefault {
			continue
		}

		result[columnName] = fieldValue.Interface()
	}

	return result
}

// GetPrimaryKeyValue gets the primary key value from the model
func (t *Transfer) GetPrimaryKeyValue(model interface{}) (interface{}, error) {
	if model == nil || t.PrimaryKey == nil {
		return nil, nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		modelValue = modelValue.Elem()
	}

	value := modelValue.Field(t.PrimaryKey.Index)
	if !value.IsValid() || value.IsZero() {
		return nil, nil
	}

	return value.Interface(), nil
}

// GetTableName returns the cached table name
func (t *Transfer) GetTableName() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.TableName
}

// GetFieldNames returns all field names in order
func (t *Transfer) GetFieldNames() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]string{}, t.FieldOrder...)
}

// ClearCache clears the transfer cache
func ClearCache() {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	transferCache = make(map[reflect.Type]*Transfer)
}

// CreateScanners creates a list of scanners for the given columns
func (t *Transfer) CreateScanners(columns []string) []interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	scanners := make([]interface{}, len(columns))
	for i, column := range columns {
		if fieldInfo, ok := t.Fields[column]; ok {
			switch fieldInfo.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				scanners[i] = new(sql.NullInt64)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				scanners[i] = new(sql.NullInt64)
			case reflect.Float32, reflect.Float64:
				scanners[i] = new(sql.NullFloat64)
			case reflect.Bool:
				scanners[i] = new(sql.NullBool)
			case reflect.String:
				scanners[i] = new(sql.NullString)
			case reflect.Struct:
				if fieldInfo.Type == reflect.TypeOf(time.Time{}) {
					scanners[i] = new(sql.NullTime)
				} else {
					scanners[i] = new(sql.RawBytes)
				}
			default:
				scanners[i] = new(sql.RawBytes)
			}
		} else {
			scanners[i] = new(sql.RawBytes)
		}
	}
	return scanners
}

// ScanRow scans a single row into a new struct instance
func (t *Transfer) ScanRow(rows *sql.Rows, columns []string) (interface{}, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Get or create scanners for these columns
	cacheKey := strings.Join(columns, ",")
	scannerInfos, ok := t.scannerCache[cacheKey]
	if !ok {
		scannerInfos = make([]*ScannerInfo, len(columns))
		for i, column := range columns {
			if fieldInfo, ok := t.Fields[column]; ok {
				scannerInfo := &ScannerInfo{Index: fieldInfo.Index}

				// Create type-specific scanner and converter
				switch fieldInfo.Type.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					scannerInfo.Scanner = new(sql.NullInt64)
					scannerInfo.Convert = func(v interface{}) interface{} {
						if n, ok := v.(*sql.NullInt64); ok && n.Valid {
							return n.Int64
						}
						return 0
					}
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					scannerInfo.Scanner = new(sql.NullInt64)
					scannerInfo.Convert = func(v interface{}) interface{} {
						if n, ok := v.(*sql.NullInt64); ok && n.Valid {
							return uint64(n.Int64)
						}
						return uint64(0)
					}
				case reflect.Float32, reflect.Float64:
					scannerInfo.Scanner = new(sql.NullFloat64)
					scannerInfo.Convert = func(v interface{}) interface{} {
						if n, ok := v.(*sql.NullFloat64); ok && n.Valid {
							return n.Float64
						}
						return 0.0
					}
				case reflect.Bool:
					scannerInfo.Scanner = new(sql.NullBool)
					scannerInfo.Convert = func(v interface{}) interface{} {
						if n, ok := v.(*sql.NullBool); ok && n.Valid {
							return n.Bool
						}
						return false
					}
				case reflect.String:
					scannerInfo.Scanner = new(sql.NullString)
					scannerInfo.Convert = func(v interface{}) interface{} {
						if n, ok := v.(*sql.NullString); ok && n.Valid {
							return n.String
						}
						return ""
					}
				case reflect.Struct:
					if fieldInfo.Type == reflect.TypeOf(time.Time{}) {
						scannerInfo.Scanner = new(sql.NullTime)
						scannerInfo.Convert = func(v interface{}) interface{} {
							if n, ok := v.(*sql.NullTime); ok && n.Valid {
								return n.Time
							}
							return time.Time{}
						}
					} else {
						scannerInfo.Scanner = new(sql.RawBytes)
						scannerInfo.Convert = func(v interface{}) interface{} {
							if n, ok := v.(*sql.RawBytes); ok && *n != nil {
								return string(*n)
							}
							return ""
						}
					}
				default:
					scannerInfo.Scanner = new(sql.RawBytes)
					scannerInfo.Convert = func(v interface{}) interface{} {
						if n, ok := v.(*sql.RawBytes); ok && *n != nil {
							return string(*n)
						}
						return ""
					}
				}
				scannerInfos[i] = scannerInfo
			}
		}
		t.scannerCache[cacheKey] = scannerInfos
	}

	// Create scanners slice for this row
	scanners := make([]interface{}, len(columns))
	for i, info := range scannerInfos {
		if info != nil {
			scanners[i] = info.Scanner
		} else {
			scanners[i] = new(sql.RawBytes)
		}
	}

	// Scan the row
	if err := rows.Scan(scanners...); err != nil {
		return nil, err
	}

	// Create new struct instance
	modelType := reflect.TypeOf(t.model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	modelValue := reflect.New(modelType).Elem()

	// Fill struct fields
	for i, info := range scannerInfos {
		if info != nil {
			fieldValue := modelValue.Field(info.Index)
			if fieldValue.CanSet() {
				convertedValue := info.Convert(scanners[i])
				if convertedValue != nil {
					if err := setFieldValue(fieldValue, convertedValue); err != nil {
						return nil, fmt.Errorf("error setting field %s: %v", columns[i], err)
					}
				}
			}
		}
	}

	return modelValue.Addr().Interface(), nil
}

// ScanAll scans all rows into a slice of struct instances
func (t *Transfer) ScanAll(rows *sql.Rows) (interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create slice to hold results
	modelType := reflect.TypeOf(t.model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	sliceType := reflect.SliceOf(reflect.PtrTo(modelType))
	slice := reflect.MakeSlice(sliceType, 0, 10)

	// Scan each row
	for rows.Next() {
		instance, err := t.ScanRow(rows, columns)
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(instance))
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return slice.Interface(), nil
}

// setFieldValue handles type conversion and setting of field values
func setFieldValue(field reflect.Value, value interface{}) error {
	if value == nil {
		return nil
	}

	// Try custom type converter first
	if field.CanAddr() {
		if converter, ok := field.Addr().Interface().(TypeConverter); ok {
			return converter.FromDB(value)
		}
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int64:
			if field.OverflowInt(v) {
				return fmt.Errorf("value %v overflows %s", v, field.Type())
			}
			field.SetInt(v)
		case float64:
			if float64(int64(v)) != v {
				return fmt.Errorf("value %v has fractional part and cannot be converted to %s", v, field.Type())
			}
			if field.OverflowInt(int64(v)) {
				return fmt.Errorf("value %v overflows %s", v, field.Type())
			}
			field.SetInt(int64(v))
		case string:
			// Handle different number formats
			str := strings.TrimSpace(v)
			base := 10
			if strings.HasPrefix(str, "0x") || strings.HasPrefix(str, "0X") {
				str = str[2:]
				base = 16
			} else if strings.HasPrefix(str, "0b") || strings.HasPrefix(str, "0B") {
				str = str[2:]
				base = 2
			} else if strings.HasPrefix(str, "0") && len(str) > 1 {
				str = str[1:]
				base = 8
			}

			if i, err := strconv.ParseInt(str, base, 64); err == nil {
				if field.OverflowInt(i) {
					return fmt.Errorf("value %v overflows %s", i, field.Type())
				}
				field.SetInt(i)
			} else {
				return fmt.Errorf("cannot convert %q to %s: %v", v, field.Type(), err)
			}
		case []uint8:
			return setFieldValue(field, string(v))
		default:
			return fmt.Errorf("cannot convert %T(%v) to %s", value, value, field.Type())
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case int64:
			if v < 0 {
				return fmt.Errorf("negative value %v cannot be converted to %s", v, field.Type())
			}
			if field.OverflowUint(uint64(v)) {
				return fmt.Errorf("value %v overflows %s", v, field.Type())
			}
			field.SetUint(uint64(v))
		case float64:
			if v < 0 {
				return fmt.Errorf("negative value %v cannot be converted to %s", v, field.Type())
			}
			if float64(uint64(v)) != v {
				return fmt.Errorf("value %v has fractional part and cannot be converted to %s", v, field.Type())
			}
			if field.OverflowUint(uint64(v)) {
				return fmt.Errorf("value %v overflows %s", v, field.Type())
			}
			field.SetUint(uint64(v))
		case string:
			str := strings.TrimSpace(v)
			base := 10
			if strings.HasPrefix(str, "0x") || strings.HasPrefix(str, "0X") {
				str = str[2:]
				base = 16
			} else if strings.HasPrefix(str, "0b") || strings.HasPrefix(str, "0B") {
				str = str[2:]
				base = 2
			} else if strings.HasPrefix(str, "0") && len(str) > 1 {
				str = str[1:]
				base = 8
			}

			if i, err := strconv.ParseUint(str, base, 64); err == nil {
				if field.OverflowUint(i) {
					return fmt.Errorf("value %v overflows %s", i, field.Type())
				}
				field.SetUint(i)
			} else {
				return fmt.Errorf("cannot convert %q to %s: %v", v, field.Type(), err)
			}
		case []uint8:
			return setFieldValue(field, string(v))
		default:
			return fmt.Errorf("cannot convert %T(%v) to %s", value, value, field.Type())
		}

	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float64:
			if field.OverflowFloat(v) {
				return fmt.Errorf("value %v overflows %s", v, field.Type())
			}
			field.SetFloat(v)
		case int64:
			field.SetFloat(float64(v))
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				if field.OverflowFloat(f) {
					return fmt.Errorf("value %v overflows %s", f, field.Type())
				}
				field.SetFloat(f)
			} else {
				return fmt.Errorf("cannot convert %q to %s: %v", v, field.Type(), err)
			}
		case []uint8:
			return setFieldValue(field, string(v))
		default:
			return fmt.Errorf("cannot convert %T(%v) to %s", value, value, field.Type())
		}

	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case int64:
			field.SetBool(v != 0)
		case float64:
			field.SetBool(v != 0)
		case string:
			str := strings.ToLower(strings.TrimSpace(v))
			field.SetBool(str == "true" || str == "1" || str == "yes" || str == "on")
		case []uint8:
			return setFieldValue(field, string(v))
		default:
			return fmt.Errorf("cannot convert %T(%v) to bool", value, value)
		}

	case reflect.String:
		switch v := value.(type) {
		case string:
			field.SetString(v)
		case []uint8:
			field.SetString(string(v))
		case int64, float64, bool:
			field.SetString(fmt.Sprint(v))
		default:
			return fmt.Errorf("cannot convert %T(%v) to string", value, value)
		}

	case reflect.Slice:
		if field.Type() == reflect.TypeOf([]byte(nil)) {
			switch v := value.(type) {
			case []byte:
				field.SetBytes(v)
			case string:
				field.SetBytes([]byte(v))
			default:
				return fmt.Errorf("cannot convert %T to []byte", value)
			}
			return nil
		}

		switch v := value.(type) {
		case []byte:
			// Try to parse as JSON array first
			var arr []interface{}
			if err := json.Unmarshal(v, &arr); err == nil {
				// Create a new slice with the correct length
				newSlice := reflect.MakeSlice(field.Type(), len(arr), len(arr))

				// Convert each element
				for i, elem := range arr {
					if err := setFieldValue(newSlice.Index(i), elem); err != nil {
						return fmt.Errorf("error setting array element %d: %v", i, err)
					}
				}
				field.Set(newSlice)
				return nil
			}

			// Try to parse as comma-separated string
			return setFieldValue(field, string(v))
		case string:
			// Handle comma-separated values
			if field.Type().Elem().Kind() != reflect.String {
				// Try to parse as JSON if not string slice
				var arr []interface{}
				if err := json.Unmarshal([]byte(v), &arr); err == nil {
					newSlice := reflect.MakeSlice(field.Type(), len(arr), len(arr))
					for i, elem := range arr {
						if err := setFieldValue(newSlice.Index(i), elem); err != nil {
							return fmt.Errorf("error setting array element %d: %v", i, err)
						}
					}
					field.Set(newSlice)
					return nil
				}
			}

			// Split by comma for string slices
			parts := strings.Split(v, ",")
			newSlice := reflect.MakeSlice(field.Type(), len(parts), len(parts))
			for i, part := range parts {
				if err := setFieldValue(newSlice.Index(i), strings.TrimSpace(part)); err != nil {
					return fmt.Errorf("error setting array element %d: %v", i, err)
				}
			}
			field.Set(newSlice)
		case []interface{}:
			// Direct array assignment
			newSlice := reflect.MakeSlice(field.Type(), len(v), len(v))
			for i, elem := range v {
				if err := setFieldValue(newSlice.Index(i), elem); err != nil {
					return fmt.Errorf("error setting array element %d: %v", i, err)
				}
			}
			field.Set(newSlice)
		default:
			// Try to handle single value as one-element array
			newSlice := reflect.MakeSlice(field.Type(), 1, 1)
			if err := setFieldValue(newSlice.Index(0), value); err != nil {
				return fmt.Errorf("cannot convert %T to %s", value, field.Type())
			}
			field.Set(newSlice)
		}

	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			switch v := value.(type) {
			case time.Time:
				field.Set(reflect.ValueOf(v))
			case string:
				formats := []string{
					"2006-01-02 15:04:05",
					"2006-01-02T15:04:05Z",
					time.RFC3339,
					"2006-01-02",                // 日期
					"15:04:05",                  // 时间
					"2006-01-02 15:04:05.999",   // 带毫秒
					"2006-01-02T15:04:05.999Z",  // ISO带毫秒
					"2006-01-02 15:04:05-07:00", // 带时区
					"2006/01/02 15:04:05",       // 斜线日期
					"20060102150405",            // 紧凑格式
				}
				var lastErr error
				for _, format := range formats {
					if t, err := time.ParseInLocation(format, v, time.Local); err == nil {
						field.Set(reflect.ValueOf(t))
						return nil
					} else {
						lastErr = err
					}
				}
				return fmt.Errorf("cannot parse %q as time using any known format: %v", v, lastErr)
			case []uint8:
				return setFieldValue(field, string(v))
			case int64:
				// Unix timestamp
				t := time.Unix(v, 0)
				field.Set(reflect.ValueOf(t))
			case float64:
				// Unix timestamp with fractional seconds
				sec := int64(v)
				nsec := int64((v - float64(sec)) * 1e9)
				t := time.Unix(sec, nsec)
				field.Set(reflect.ValueOf(t))
			default:
				return fmt.Errorf("cannot convert %T(%v) to time.Time", value, value)
			}
			return nil
		}
	}
	return nil
}

// SetModel sets the model type for scanning
func (t *Transfer) SetModel(model interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.model = model
}
