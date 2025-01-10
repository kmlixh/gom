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
		// Convert CamelCase to snake_case
		name := modelType.Name()
		var result []rune
		for i, r := range name {
			if i > 0 && r >= 'A' && r <= 'Z' {
				result = append(result, '_')
			}
			result = append(result, []rune(strings.ToLower(string(r)))...)
		}
		transfer.TableName = string(result)
	}

	// Parse struct fields
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tag := field.Tag.Get("gom")
		if tag == "" {
			continue
		}

		parts := strings.Split(tag, ",")
		columnName := strings.TrimSpace(parts[0])
		if columnName == "" {
			// Use field name as column name if tag is empty
			columnName = strings.ToLower(field.Name)
		}

		fieldInfo := &FieldInfo{
			Index:  i,
			Name:   field.Name,
			Column: columnName,
			Type:   field.Type,
		}

		// Parse tag options
		for _, opt := range parts[1:] {
			opt = strings.TrimSpace(opt)
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
func (t *Transfer) ToMap(model interface{}, isUpdate ...bool) map[string]interface{} {
	if model == nil {
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	modelValue := reflect.ValueOf(model)
	if modelValue.Kind() == reflect.Ptr {
		if modelValue.IsNil() {
			return nil
		}
		modelValue = modelValue.Elem()
	}

	result := make(map[string]interface{})
	forUpdate := len(isUpdate) > 0 && isUpdate[0]

	for _, columnName := range t.FieldOrder {
		fieldInfo := t.Fields[columnName]
		if fieldInfo == nil {
			continue
		}

		fieldValue := modelValue.Field(fieldInfo.Index)
		if !fieldValue.IsValid() {
			continue
		}

		// For updates, include only fields that have changed
		if forUpdate {
			// Skip primary key field for updates
			if fieldInfo.IsPrimary {
				continue
			}
			// Include field if it has been explicitly set (non-zero value)
			if !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldInfo.Type).Interface()) {
				if value := t.convertFieldToDBValue(fieldValue); value != nil {
					result[columnName] = value
				}
			}
		} else {
			// For inserts, include all fields except auto-increment fields
			if !fieldInfo.IsAuto {
				if value := t.convertFieldToDBValue(fieldValue); value != nil {
					result[columnName] = value
				}
			} else if !fieldValue.IsZero() {
				// Include auto-increment field if it has a non-zero value
				result[columnName] = fieldValue.Int()
			}
		}
	}

	// Always set updated_at for updates if it exists in the fields
	if forUpdate {
		if _, ok := t.Fields["updated_at"]; ok {
			result["updated_at"] = time.Now()
		}
	}

	return result
}

// convertFieldToDBValue converts a field value to its database representation
func (t *Transfer) convertFieldToDBValue(field reflect.Value) interface{} {
	if !field.IsValid() {
		return nil
	}

	// Handle pointer types
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			return nil
		}
		return t.convertFieldToDBValue(field.Elem())
	}

	// Handle zero values
	if field.IsZero() {
		return nil
	}

	switch field.Kind() {
	case reflect.Bool:
		if field.Bool() {
			return int(1)
		}
		return int(0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(field.Uint())
	case reflect.Float32, reflect.Float64:
		return field.Float()
	case reflect.String:
		if s := field.String(); s != "" {
			return s
		}
		return nil
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			t := field.Interface().(time.Time)
			if !t.IsZero() {
				return t
			}
			return nil
		}
		// Try to convert struct to JSON
		if data, err := json.Marshal(field.Interface()); err == nil {
			return string(data)
		}
	case reflect.Slice, reflect.Array:
		if field.Len() > 0 {
			// Try to convert slice/array to JSON
			if data, err := json.Marshal(field.Interface()); err == nil {
				return string(data)
			}
		}
	case reflect.Map:
		if field.Len() > 0 {
			// Try to convert map to JSON
			if data, err := json.Marshal(field.Interface()); err == nil {
				return string(data)
			}
		}
	case reflect.Complex64, reflect.Complex128:
		// Convert complex numbers to JSON string
		c := field.Complex()
		if c != complex(0, 0) {
			data := map[string]float64{
				"real": real(c),
				"imag": imag(c),
			}
			if jsonStr, err := json.Marshal(data); err == nil {
				return string(jsonStr)
			}
		}
	}
	return nil
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
				scanners[i] = new(sql.NullInt64)
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

// createBoolScanner creates a scanner for boolean values
func createBoolScanner() (*ScannerInfo, error) {
	scanner := &ScannerInfo{
		Scanner: new(sql.NullInt64),
		Convert: func(v interface{}) interface{} {
			if n, ok := v.(*sql.NullInt64); ok && n.Valid {
				return n.Int64 == 1
			}
			return false
		},
	}
	return scanner, nil
}

// ScanRow scans a database row into a new instance of the model
func (t *Transfer) ScanRow(rows *sql.Rows) (interface{}, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	scanners := t.CreateScanners(columns)
	if err := rows.Scan(scanners...); err != nil {
		return nil, err
	}

	modelType := reflect.TypeOf(t.model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	model := reflect.New(modelType)
	elem := model.Elem()

	for i, column := range columns {
		if fieldInfo, ok := t.Fields[column]; ok {
			field := elem.Field(fieldInfo.Index)
			scanner := scanners[i]

			if scanner == nil {
				continue
			}

			var value interface{}
			switch s := scanner.(type) {
			case *sql.NullString:
				if s.Valid {
					value = s.String
				}
			case *sql.NullInt64:
				if s.Valid {
					if field.Kind() == reflect.Bool {
						value = s.Int64 == 1
					} else {
						value = s.Int64
					}
				}
			case *sql.NullFloat64:
				if s.Valid {
					value = s.Float64
				}
			case *sql.NullTime:
				if s.Valid {
					value = s.Time
				}
			case *sql.RawBytes:
				if s != nil && len(*s) > 0 {
					value = string(*s)
				}
			}

			if value != nil {
				if err := setFieldValue(field, value); err != nil {
					return nil, err
				}
			}
		}
	}

	return model.Interface(), nil
}

// ScanAll scans all rows into a slice of struct instances
func (t *Transfer) ScanAll(rows *sql.Rows) (interface{}, error) {

	// Create slice to hold results
	modelType := reflect.TypeOf(t.model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	sliceType := reflect.SliceOf(reflect.PtrTo(modelType))
	slice := reflect.MakeSlice(sliceType, 0, 10)

	// Scan each row
	for rows.Next() {
		instance, err := t.ScanRow(rows)
		if err != nil {
			return nil, err
		}
		slice = reflect.Append(slice, reflect.ValueOf(instance))
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice.Interface(), nil
}

// convertToBool converts various types to boolean value
func convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int64:
		return v == 1, nil
	case int32:
		return v == 1, nil
	case int16:
		return v == 1, nil
	case int8:
		return v == 1, nil
	case int:
		return v == 1, nil
	case uint64:
		return v == 1, nil
	case uint32:
		return v == 1, nil
	case uint16:
		return v == 1, nil
	case uint8:
		return v == 1, nil
	case uint:
		return v == 1, nil
	case float64:
		return v == 1.0, nil
	case float32:
		return v == 1.0, nil
	case string:
		str := strings.ToLower(strings.TrimSpace(v))
		return str == "true" || str == "1" || str == "yes" || str == "on", nil
	case []uint8:
		if len(v) == 1 {
			return v[0] == 1, nil
		}
		str := strings.ToLower(strings.TrimSpace(string(v)))
		return str == "true" || str == "1" || str == "yes" || str == "on", nil
	default:
		return false, fmt.Errorf("cannot convert %T(%v) to bool", value, value)
	}
}

// setFieldValue handles type conversion and setting of field values
func setFieldValue(field reflect.Value, value interface{}) error {
	if !field.CanSet() {
		return fmt.Errorf("field cannot be set")
	}

	if value == nil {
		return nil
	}

	switch field.Kind() {
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case int64:
			field.SetBool(v == 1)
		case float64:
			field.SetBool(v == 1)
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			field.SetBool(b)
		case []uint8:
			if len(v) > 0 {
				field.SetBool(v[0] == 1)
			}
		default:
			return fmt.Errorf("cannot convert %T to bool", value)
		}
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

// ScanRows scans multiple rows into a slice of model instances
func (t *Transfer) ScanRows(rows *sql.Rows) (interface{}, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var results []interface{}
	for rows.Next() {
		result, err := t.ScanRow(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Result scans the rows into a slice of model instances
func (t *Transfer) Result(rows *sql.Rows) (interface{}, error) {
	defer rows.Close()
	return t.ScanRows(rows)
}

// First scans a single row into a model instance
func (t *Transfer) First(rows *sql.Rows) (interface{}, error) {
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sql.ErrNoRows
	}

	result, err := t.ScanRow(rows)
	if err != nil {
		return nil, err
	}

	return result, nil
}
