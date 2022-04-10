package structs

import (
	"database/sql/driver"
	"errors"
	"strconv"
	"time"
)

type ScannerGenerateFunc func(colName string, col interface{}) IScanner

type ScanFunc func(src interface{}) (interface{}, error)

type IScanner interface {
	ColumnName() string
	Value() (driver.Value, error)
	Scan(src interface{}) error
}

type ScannerImpl struct {
	ColName string
	Object  driver.Value
	ScanFunc
}

func (s *ScannerImpl) ColumnName() string {
	return s.ColName
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

type EmptyScanner struct {
	ColName string
}

func (e EmptyScanner) Scan(_ interface{}) error {
	return nil
}
func (e EmptyScanner) ColumnName() string {
	return e.ColName
}
func (e EmptyScanner) Value() (driver.Value, error) {
	return nil, nil
}

func StringScan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result = ""
	var err error
	switch src.(type) {
	case string:
		result = src.(string)
	case []byte:
		result = string(src.([]byte))
	case time.Time:
		result = src.(time.Time).String()
	}
	return result, err
}
func Int64Scan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result int64 = 0
	var err error
	switch src.(type) {
	case int, int32, int64:
		result = src.(int64)
	case string:
		result, _ = Int64fromString(src.(string))
	case []uint8:
		result = Int64FromBytes(src.([]byte))
	case time.Time:
		result = src.(time.Time).Unix()
	}
	return result, err

}
func Int32Scan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result = 0
	switch src.(type) {
	case string:
		result, _ = IntfromString(src.(string))
	case int, int32:
		result = src.(int)
	case int64:
		result = int(src.(int64))
	case []byte:
		result = int(Int32FromBytes(src.([]byte)))
	case time.Time:
		result = int(src.(time.Time).Unix())
	}
	return result, nil
}
func Float32Scan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result float32 = 0
	var err error
	switch src.(type) {
	case string:
		result, _ = Float32fromString(src.(string))
	case []byte:
		result = Float32fromBytes(src.([]byte))
	case time.Time:
		err = errors.New("can't parse time.Time to float32")
	case float64:
		result = float32(src.(float64))
	case float32:
		result = src.(float32)
	}
	return result, err

}
func Float64Scan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result float64 = 0
	switch src.(type) {
	case string:
		result, _ = Float64fromString(src.(string))
	case []byte:
		result = Float64fromBytes(src.([]byte))
	case time.Time:
		result = float64(src.(time.Time).Unix())
	case float32:
		result = float64(src.(float32))
	case float64:
		result = src.(float64)
	}
	return result, nil

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
	case time.Time:
		result = Int64ToBytes(src.(time.Time).Unix())
	}
	return result, nil

}
func TimeScan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result = time.Time{}
	switch src.(type) {
	case string:
		result, _ = TimeFromString(src.(string))
	case []byte:
		result = time.Unix(Int64FromBytes(src.([]byte)), 0)
	case time.Time:
		result = src.(time.Time)
	}
	return result, nil

}
func BoolScan(src interface{}) (interface{}, error) {
	if src == nil {
		return nil, nil
	}
	var result = false
	var err error
	switch src.(type) {
	case string:
		result, _ = strconv.ParseBool(src.(string))
	case []byte:
		temp := Int64FromBytes(src.([]byte))
		result = temp > 0
	case time.Time:
		err = errors.New("can't parse time.Time to Boolean")
	}
	return result, err

}
func GetIScannerOfColumn(colName string, col interface{}) IScanner {
	scanner, ok := col.(IScanner)
	if ok {
		return scanner
	}
	switch col.(type) {
	case int, int32:
		return &ScannerImpl{colName, 0, Int32Scan}
	case int64:
		return &ScannerImpl{colName, int64(0), Int64Scan}
	case float32:
		return &ScannerImpl{colName, float32(0), Float32Scan}
	case float64:
		return &ScannerImpl{colName, float64(0), Float64Scan}
	case string:
		return &ScannerImpl{colName, "", StringScan}
	case []byte:
		return &ScannerImpl{colName, []byte{}, ByteArrayScan}
	case time.Time:
		return &ScannerImpl{colName, time.Time{}, TimeScan}
	case bool:
		return &ScannerImpl{colName, false, BoolScan}
	default:
		return nil
	}
}
func GetDataScanners(rowColumns []string, dataMap map[string]int, columns []Column, scannerFuncs ...ScannerGenerateFunc) []interface{} {
	//TODO 未考虑简单对象传入的情况，将结果集的列和model的列做拟合的时候,必然会存在表列和columns不一致的情况.这个时候需要我们创造一个DataTransfer,ColumnDataMap,并且将datatransfer缓存到静态map中,后续直接从map中取用,无需再次优化
	var scanners []interface{}
	for _, colName := range rowColumns {
		var scanner IScanner
		col, ok := dataMap[colName]
		if ok {
			scanner = GetIScannerOfColumn(colName, columns[col].Data)
			if scanner == nil && len(scannerFuncs) > 0 {
				for _, scannerFunc := range scannerFuncs {
					scanner = scannerFunc(colName, columns[col].Data)
					if scanner != nil {
						break
					}
				}
			}
		}
		if scanner == nil {
			scanner = &EmptyScanner{colName}
		}
		scanners = append(scanners, scanner)
	}
	return scanners
}
