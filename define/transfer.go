package define

import (
	"database/sql"
	"reflect"
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
	mu           sync.RWMutex              // For concurrent access
	scannerCache map[string][]*ScannerInfo // Cache of column scanners
	model        interface{}
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
	}

	// Set table name
	transfer.TableName = strings.ToLower(modelType.Name())

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
					fieldValue.Set(reflect.ValueOf(convertedValue))
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

// SetModel sets the model type for scanning
func (t *Transfer) SetModel(model interface{}) {
	t.model = model
}
