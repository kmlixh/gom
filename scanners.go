package gom

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"
)

type ScannerGenerateFunc func(colName string, col interface{}) IScanner

type ScanFunc func(src interface{}) (interface{}, error)

var EMPTY_SCANNER *EmptyScanner = &EmptyScanner{}

type IScanner interface {
	Value() (driver.Value, error)
	Scan(src interface{}) error
}

type ScannerImpl struct {
	Object driver.Value
	ScanFunc
}

func (scanner *ScannerImpl) Scan(src interface{}) error {
	result, er := scanner.ScanFunc(src)
	if er != nil {
		return er
	}
	scanner.Object = result
	return nil
}
func (scanner ScannerImpl) Value() (driver.Value, error) {
	return scanner.Object, nil
}

type CountScanner struct {
}
type EmptyScanner struct {
	ColName string
}

func (e EmptyScanner) Scan(_ interface{}) error {
	return nil
}
func (e EmptyScanner) Value() (driver.Value, error) {
	return nil, nil
}

func IntScan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result = 0
	var err error
	switch src.(type) {
	case int:
		result = src.(int)
	case int32:
		result = (int)(src.(int32))
	case int64:
		result = (int)(src.(int64))
	}
	return result, err

}
func UIntScan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result any
	var err error
	switch src.(type) {
	case uint:
		result = src.(uint)
	case uint8:
		result = src.(uint8)
	case uint16:
		result = src.(uint16)
	case uint32:
		result = src.(uint32)
	case uint64:
		result = src.(uint64)
	}
	return result, err

}
func Float32Scan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result float32 = 0
	var err error
	switch src.(type) {
	case float64:
		result = float32(src.(float64))
	case float32:
		result = src.(float32)
	}
	return result, err
}
func ByteArrayScan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result = []byte{}
	switch src.(type) {
	case []byte:
		result = src.([]byte)
	}
	return result, nil

}
func GetIScannerOfSimpleType(p reflect.Type) IScanner {
	switch p.Kind() {
	case reflect.Int:
		return &ScannerImpl{0, IntScan}
	case reflect.Int16:
		return &sql.NullInt16{}
	case reflect.Int32:
		return &sql.NullInt32{}
	case reflect.Int64:
		return &sql.NullInt64{}
	case reflect.Float32:
		return &ScannerImpl{float32(0), Float32Scan}
	case reflect.Float64:
		return &sql.NullFloat64{}
	case reflect.String:
		return &sql.NullString{}
	case reflect.Uint:
		return &ScannerImpl{uint(0), UIntScan}
	case reflect.Uint8:
		return &ScannerImpl{uint8(0), UIntScan}
	case reflect.Uint16:
		return &ScannerImpl{uint16(0), UIntScan}
	case reflect.Uint32:
		return &ScannerImpl{uint32(0), UIntScan}
	case reflect.Uint64:
		return &ScannerImpl{uint64(0), UIntScan}
	case reflect.TypeOf([]byte{}).Kind():
		return &ScannerImpl{[]byte{}, ByteArrayScan}
	case reflect.TypeOf(time.Time{}).Kind():
		return &sql.NullTime{}
	case reflect.TypeOf(byte(0)).Kind():
		return &sql.NullByte{}
	case reflect.Bool:
		return &sql.NullBool{}

	default:
		return EMPTY_SCANNER
	}
}
func GetIScannerOfSimple(col interface{}) IScanner {
	scanner, ok := col.(IScanner)
	if ok {
		return scanner
	}
	switch col.(type) {
	case int:
		return &ScannerImpl{0, IntScan}
	case int16:
		return &sql.NullInt16{}
	case int32:
		return &sql.NullInt32{}
	case int64:
		return &sql.NullInt64{}
	case float32:
		return &ScannerImpl{float32(0), Float32Scan}
	case float64:
		return &sql.NullFloat64{}
	case string:
		return &sql.NullString{}
	case []byte:
		return &ScannerImpl{[]byte{}, ByteArrayScan}
	case time.Time:
		return &sql.NullTime{}
	case bool:
		return &sql.NullBool{}
	case uint:
		return &ScannerImpl{uint(0), UIntScan}
	case uint8:
		return &ScannerImpl{uint8(0), UIntScan}
	case uint16:
		return &ScannerImpl{uint16(0), UIntScan}
	case uint32:
		return &ScannerImpl{uint32(0), UIntScan}
	case uint64:
		return &ScannerImpl{uint64(0), UIntScan}
	default:
		return nil
	}
}
