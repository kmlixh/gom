package structs

import (
	"database/sql/driver"
	"errors"
	"strconv"
	"time"
)

type ScanFunc func(src interface{}) (interface{}, error)

type IScanner interface {
	Value() (driver.Value, error)
	Scan(src interface{}) error
}

type ScannerImpl struct {
	Object driver.Value
	ScanFunc
}

func (scanner *ScannerImpl) Scan(src interface{}) error {
	result, error := scanner.ScanFunc(src)
	if error != nil {
		return error
	}
	scanner.Object = result
	return nil
}
func (scanner ScannerImpl) Value() (driver.Value, error) {
	return scanner.Object, nil
}
func EmptyScanner() IScanner {
	return &ScannerImpl{0, func(src interface{}) (interface{}, error) {
		return nil, nil
	}}
}

func StringScan(src interface{}) (interface{}, error) {
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
	var result int = 0
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
