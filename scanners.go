package gom

import (
	"database/sql"
	"database/sql/driver"
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
	case string:
		result = []byte(src.(string))
	case []byte:
		result = src.([]byte)
	}
	return result, nil

}
func GetIScannerOfColumn(col interface{}) IScanner {
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
	default:
		return nil
	}
}
