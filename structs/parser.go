package structs

import (
	"encoding/binary"
	"math"
	"strconv"
	"time"
)

func Stringfrombytes(data []byte) string {
	return string(data)
}
func Float32fromString(data string) (float32, error) {
	var f float64
	f, err := strconv.ParseFloat(data, 32)
	return float32(f), err
}
func Float64fromString(data string) (float64, error) {
	var f float64
	f, err := strconv.ParseFloat(data, 64)
	return f, err
}
func IntfromString(data string) (int, error) {
	return strconv.Atoi(data)
}

func Int8fromString(data string) (int8, error) {
	result, err := IntfromString(data)
	return int8(result), err
}
func Int16fromString(data string) (int16, error) {
	result, err := IntfromString(data)
	return int16(result), err
}
func Int32fromString(data string) (int32, error) {
	i, er := strconv.ParseInt(data, 10, 32)
	if er != nil {
		return 0, er
	}
	return int32(i), nil

}
func Int64fromString(data string) (int64, error) {
	return strconv.ParseInt(data, 10, 64)
}
func UInt8fromString(data string) (uint8, error) {
	result, err := IntfromString(data)
	return uint8(result), err
}
func UInt16fromString(data string) (uint16, error) {
	result, err := IntfromString(data)
	return uint16(result), err
}
func UInt32fromString(data string) (uint32, error) {
	result, err := IntfromString(data)
	return uint32(result), err
}
func UIntfromString(data string) (uint, error) {
	result, err := IntfromString(data)
	return uint(result), err
}
func UInt64fromString(data string) (uint64, error) {
	result, err := IntfromString(data)
	return uint64(result), err
}
func TimeFromString(data string) (time.Time, error) {
	TimeFormat := "2006-01-02 03:04:05"
	return time.ParseInLocation(TimeFormat, data, time.Local)
}

func Float64fromBytes(bytes []byte) float64 {
	float, _ := strconv.ParseFloat(string(bytes), 64)
	return float
}

func Float64ToBytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}
func Float32fromBytes(bytes []byte) float32 {
	float, _ := strconv.ParseFloat(string(bytes), 32)
	return float32(float)
}
func Float32ToBytes(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}
func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func Int64FromBytes(buf []byte) int64 {
	if len(buf) == 8 {
		return int64(binary.BigEndian.Uint64(buf))
	}
	r, _ := Int64fromString(string(buf))
	return r
}
func Int32FromBytes(buf []byte) int32 {
	if len(buf) == 4 {
		return int32(binary.BigEndian.Uint32(buf))
	}
	r, _ := Int32fromString(string(buf))
	return r
}
