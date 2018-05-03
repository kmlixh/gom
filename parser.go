package gom

import (

	"strconv"
	"time"
)

func Stringfrombytes(data []byte) string {
	return string(data)
}
func Float32fromString(data string) (float32,error){
	var f float64
	f, err := strconv.ParseFloat(data, 32)
	return float32(f),err
}
func Float64fromString(data string) (float64,error){
	var f float64
	f, err := strconv.ParseFloat(data, 64)
	return f,err
}
func IntfromString(data string) (int ,error){
	return strconv.Atoi(data)
}
func Int8fromString(data string)(int8,error){
	result,err:= IntfromString(data)
	return int8(result),err
}
func Int16fromString(data string)(int16,error){
	result,err:= IntfromString(data)
	return int16(result),err
}
func Int32fromString(data string)(int32,error){
	result,err:= IntfromString(data)
	return int32(result),err
}
func Int64fromString(data string)(int64,error){
	result,err:= IntfromString(data)
	return int64(result),err
}
func UInt8fromString(data string)(uint8,error){
	result,err:= IntfromString(data)
	return uint8(result),err
}
func UInt16fromString(data string)(uint16,error){
	result,err:= IntfromString(data)
	return uint16(result),err
}
func UInt32fromString(data string)(uint32,error){
	result,err:= IntfromString(data)
	return uint32(result),err
}
func UIntfromString(data string)(uint,error){
	result,err:= IntfromString(data)
	return uint(result),err
}
func UInt64fromString(data string)(uint64,error){
	result,err:= IntfromString(data)
	return uint64(result),err
}
func TimeFromString(data string)(time.Time,error){
	TimeFormat:= "2006-01-02 03:04:05"
	return time.ParseInLocation(TimeFormat,data,time.Local)
}