package gom

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

func IsEmpty(v interface{}) bool {
	times := time.Time{}
	if times == v {
		return true
	}
	if v == "" {
		return true
	}
	if v == 0 {
		return true
	}
	if v == 0.0 {
		return true
	}
	if v == nil {
		return true
	}
	return false
}
func getType(v interface{}) (reflect.Type, bool, bool) {
	tt := reflect.TypeOf(v)
	isPtr := false
	islice := false
	if tt.Kind() == reflect.Ptr {
		tt = tt.Elem()
		isPtr = true
	}
	if tt.Kind() == reflect.Slice || tt.Kind() == reflect.Array {
		tt = tt.Elem()
		islice = true
	}
	if debug {
		fmt.Println("Test getType, result:", tt, isPtr, islice)
	}
	return tt, isPtr, islice
}

var mutex sync.Mutex
var tableModelCache map[string]StructModel

func getStructModel(v interface{}, choosedColumns ...string) (StructModel, error) {
	//防止重复创建map，需要对map创建过程加锁
	mutex.Lock()
	if tableModelCache == nil {
		tableModelCache = make(map[string]StructModel)
	}
	mutex.Unlock()
	tt, isPtr, isSlice := getType(v)
	_, hasTable := reflect.New(tt).Interface().(Table)
	tableName := camelToSnakeString(tt.Name())
	if hasTable {
		tableName = reflect.New(tt).Interface().(Table).TableName()
	}
	if tt.Kind() != reflect.Struct || (tt.Kind() == reflect.Struct && !hasTable) || tt.NumField() == 0 {
		return StructModel{}, errors.New(tt.Name() + " is not a valid struct")
	}

	if v != nil && tt.Kind() != reflect.Interface {
		value := reflect.ValueOf(v)
		if isPtr {
			value = value.Elem()
		}
		if isSlice {
			value = reflect.Indirect(reflect.New(tt))
		}
		if debug {
			fmt.Println("model info:", tt, isPtr, isSlice, value)
		}
		var model StructModel
		cachedModel, ok := tableModelCache[tt.String()]
		if ok {
			model = cachedModel.Clone(value, choosedColumns...)
		} else {
			columnNames, columnMap, primary := getColumns(value)
			temp := StructModel{Type: tt, Value: reflect.New(tt), ColumnNames: columnNames, Columns: columnMap, TableName: tableName, Primary: primary}
			tableModelCache[tt.String()] = temp
			model = temp.Clone(value, choosedColumns...)
		}
		return model, nil

	} else {
		return StructModel{}, errors.New("can't use interface")
	}
}

func getColumns(v reflect.Value) ([]string, map[string]Column, Column) {
	var columnNames []string
	columns := make(map[string]Column)
	var primary Column
	oo := v.Type()
	for i := 0; i < oo.NumField(); i++ {
		field := oo.Field(i)
		col, tps := getColumnFromField(field)
		if tps != -1 {
			columns[col.ColumnName] = col
			columnNames = append(columnNames, col.ColumnName)
			if tps == 1 || tps == 2 {
				primary = col
			}
		}
	}
	if debug {
		fmt.Println("columns is:", columns)
	}
	return columnNames, columns, primary
}
func md5V(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
func getColumnFromField(filed reflect.StructField) (Column, int) {
	colName, tps := getColumnNameAndTypeFromField(filed)
	if debug {
		fmt.Println("Tag is:", colName, "type is:", tps)
	}
	v := reflect.New(filed.Type)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if tps != -1 {
		return Column{Type: v.Type(), ColumnName: colName, FieldName: filed.Name, Auto: tps == 2, IsPrimary: tps == 1 || tps == 2}, tps
	} else {
		return Column{}, -1
	}

}
func getColumnNameAndTypeFromField(field reflect.StructField) (string, int) {
	tag, hasTag := field.Tag.Lookup("gom")
	if hasTag {
		if len(tag) == 0 {
			tag = camelToSnakeString(field.Name)
		}
		if strings.EqualFold(tag, "-") {
			return "", -1
		} else if len(tag) == 1 {
			tps := 0
			if strings.EqualFold(tag, "@") {
				tps = 2
			}
			if strings.EqualFold(tag, "!") {
				tps = 1
			}
			return camelToSnakeString(field.Name), tps
		} else {
			if strings.Contains(tag, ",") {
				tags := strings.Split(tag, ",")
				if len(tags) == 2 {
					if strings.EqualFold(tags[0], "!") || strings.EqualFold(tags[0], "primary") {
						return tags[1], 1
					} else if strings.EqualFold(tags[0], "@") || strings.EqualFold(tags[0], "auto") {
						return tags[1], 2
					} else if strings.EqualFold(tags[0], "#") || strings.EqualFold(tags[0], "column") {
						return tags[1], 0
					} else {
						return "", -1
					}
				} else {
					return "", -1
				}
			} else {
				return tag, 0
			}
		}
	} else {
		return "", -1
	}
}
func StructToMap(vs interface{}) (map[string]interface{}, error) {
	t, _, isSlice := getType(vs)
	v := reflect.ValueOf(vs)
	if isSlice {
		return nil, errors.New("can't convert slice or array to map")
	}
	var maps map[string]interface{}
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			vv := v.Field(i)
			maps[f.Name] = vv
		}
		return maps, nil
	}
	return nil, errors.New("can't convert slice or array to map")

}
func StructToCondition(vs interface{}) Condition {
	maps, err := StructToMap(vs)
	if err != nil {
		panic(err)
	}
	return MapToCondition(maps)
}
func MapToCondition(maps map[string]interface{}) Condition {
	var cnd Condition
	for k, v := range maps {
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if (t.Kind() != reflect.Struct && t.Kind() != reflect.Slice) || t.Kind() == reflect.TypeOf(time.Now()).Kind() {
			value := v
			//if t.Kind() == reflect.TypeOf(time.Now()).Kind() {
			//	value = v.(time.Time).Format("2006-01-02 15:04:05")
			//}
			if cnd == nil {
				cnd = CndAnd(k, Eq, value)
			} else {
				cnd.And(k, Eq, value)
			}
		}
	}
	return cnd
}
func getValueOfType(c Column) IScanner {
	vs := reflect.New(c.Type)
	scanner, ojbk := vs.Interface().(IScanner)
	if ojbk {
		return scanner
	}
	vi := reflect.Indirect(vs)

	switch vi.Interface().(type) {
	case int, int32:
		return &ScannerImpl{0, Int32Scan}
	case int64:
		return &ScannerImpl{int64(0), Int64Scan}
	case float32:
		return &ScannerImpl{float32(0), Float32Scan}
	case float64:
		return &ScannerImpl{float64(0), Float64Scan}
	case string:
		return &ScannerImpl{"", StringScan}
	case []byte:
		return &ScannerImpl{[]byte{}, ByteArrayScan}
	case time.Time:
		return &ScannerImpl{time.Time{}, TimeScan}
	case bool:
		return &ScannerImpl{false, BoolScan}
	default:
		return emptyScanner()
	}
}
func UnZipSlice(vs interface{}) []interface{} {
	var result []interface{}
	t := reflect.TypeOf(vs)
	isPtr := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		isPtr = true
	}
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(vs)
		if isPtr {
			v = v.Elem()
		}
		if v.Len() > 0 {
			for i := 0; i < v.Len(); i++ { //m为上述切片
				item := v.Index(i)
				result = append(result, UnZipSlice(item.Interface())...)
			}

		}
	}
	if t.Kind() == reflect.Struct {
		result = append(result, vs)
	}
	return result
}
func SliceToMapSlice(vs interface{}) map[string][]interface{} {
	var result map[string][]interface{}
	slice := UnZipSlice(vs)
	for _, v := range slice {
		t := reflect.TypeOf(v).Name()
		lst, ok := result[t]
		if !ok {
			lst = make([]interface{}, 0)
		}
		lst = append(lst, v)
		result[t] = lst
	}
	return result
}
