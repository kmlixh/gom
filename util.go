package gom

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var columnToFieldNameMapCache = make(map[reflect.Type]map[string]FieldInfo)
var columnsCache = make(map[reflect.Type][]string)

func getDefaultsColumnFieldMap(v reflect.Type) (map[string]FieldInfo, []string) {
	columns := make([]string, 0)
	columnMap := make(map[string]FieldInfo)
	cc, ok := columnToFieldNameMapCache[v]
	cols, okk := columnsCache[v]
	if ok && okk {
		return cc, cols
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		colName := getColumnName(field)
		if len(colName) > 0 {

			columnMap[colName] = FieldInfo{
				FieldName: field.Name,
				FieldType: field.Type,
			}
			columns = append(columns, colName)
		}
	}
	columnToFieldNameMapCache[v] = columnMap
	columnsCache[v] = columns
	return columnMap, columns
}
func GetRawTableInfo(v interface{}) RawMetaInfo {
	tt := reflect.TypeOf(v)
	isStruct := false
	isPtr := false
	isSlice := false
	if tt.Kind() == reflect.Ptr {
		tt = tt.Elem()
		isPtr = true
	}
	if tt.Kind() == reflect.Slice || tt.Kind() == reflect.Array {
		tt = tt.Elem()
		isSlice = true
	}
	isStruct = tt.Kind() == reflect.Struct

	if Debug {
		fmt.Println("Test GetRawTableInfo, result:", tt, isPtr, isSlice)
	}
	tableName := ""
	if isStruct {
		tableName = CamelToSnakeString(tt.Name())
	}
	vs := reflect.Indirect(reflect.New(tt))
	iTable, ok := vs.Interface().(ITableName)
	if ok {
		tableName = iTable.TableName()
	}

	return RawMetaInfo{tt, tableName, isSlice, isPtr, isStruct, reflect.Indirect(reflect.ValueOf(v))}
}

func getColumnName(field reflect.StructField) string {
	tag, hasTag := field.Tag.Lookup("gom")
	if !hasTag {
		tag = CamelToSnakeString(field.Name)
	} else if strings.EqualFold(tag, "-") {
		return ""
	}
	return tag
}

func StructToMap(vs interface{}, columns ...string) (map[string]interface{}, error) {
	if vs == nil {
		return nil, errors.New("nil can't be used to create Map")
	}
	rawInfo := GetRawTableInfo(vs)
	if rawInfo.IsSlice {
		return nil, errors.New("can't convert slice or array to map")
	}
	colMap := make(map[string]int)
	if len(columns) > 0 {
		for idx, col := range columns {
			colMap[col] = idx
		}
	}
	if rawInfo.Kind() == reflect.Struct {
		if rawInfo.Type.NumField() == 0 {
			//
			return nil, errors.New(fmt.Sprintf("[%s] was a \"empty struct\",it has no field or All fields has been ignored", rawInfo.Type.Name()))
		}
		newMap := make(map[string]interface{})
		cMap, _ := getDefaultsColumnFieldMap(rawInfo.Type)
		for key, column := range cMap {
			if len(columns) > 0 {
				_, ok := colMap[key]
				if ok {
					newMap[key] = reflect.ValueOf(vs).FieldByName(column.FieldName).Interface()
				}
			} else {
				val := reflect.ValueOf(vs).FieldByName(column.FieldName)
				if !val.IsZero() {
					newMap[key] = val.Interface()
				}
			}
		}
		return newMap, nil
	}
	return nil, errors.New(fmt.Sprintf("can't convert %s to map", rawInfo.Name()))

}
func StructToCondition(vs interface{}, columns ...string) Condition {
	maps, err := StructToMap(vs, columns...)
	if err != nil {
		panic(err)
	}
	return MapToCondition(maps)
}
func MapToCondition(maps map[string]interface{}) Condition {
	if maps == nil {
		return nil
	}
	var cnd Condition
	for k, v := range maps {
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct || t.Kind() == reflect.TypeOf(time.Now()).Kind() || ((t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() != reflect.Struct) {
			value := v
			if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() != reflect.Struct {
				if cnd == nil {
					cnd = CndIn(k, UnZipSlice(value)...)
				} else {
					cnd.In(k, UnZipSlice(value)...)
				}
			} else {
				if cnd == nil {
					cnd = Cnd(k, Eq, value)
				} else {
					cnd.And(k, Eq, value)
				}
			}

		}
	}
	return cnd
}
func UnZipSlice(vs interface{}) []interface{} {
	var result = make([]interface{}, 0)
	t := reflect.TypeOf(vs)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		v := reflect.ValueOf(vs)

		if v.Len() > 0 {
			for i := 0; i < v.Len(); i++ { //m为上述切片
				item := v.Index(i)
				if item.Interface() != nil {
					result = append(result, UnZipSlice(item.Interface())...)
				}
			}

		}
	} else {
		result = append(result, vs)
	}
	return result
}
func SliceToGroupSlice(vs interface{}) map[string][]interface{} {
	result := make(map[string][]interface{})
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

func getGrouteId() int64 {
	var (
		buf [64]byte
		n   = runtime.Stack(buf[:], false)
		stk = strings.TrimPrefix(string(buf[:n]), "goroutine ")
	)

	idField := strings.Fields(stk)[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Errorf("can not get goroutine id: %v", err))
	}

	return int64(id)
}

func ScannerResultToStruct(t reflect.Type, scanners []interface{}, columnNames []string) reflect.Value {
	v := reflect.Indirect(reflect.New(t))
	colsMap, _ := getDefaultsColumnFieldMap(t)
	for i, name := range columnNames {
		if _, ok := scanners[i].(EmptyScanner); !ok { //不能时空扫描器
			val, er := scanners[i].(IScanner).Value()
			if er != nil {
				panic(er)
			}
			colData, ok := colsMap[name]
			if ok && val != nil {
				v.FieldByName(colData.FieldName).Set(reflect.ValueOf(val))
			}
		}

	}
	return v
}
