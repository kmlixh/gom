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

type RawTableInfo struct {
	reflect.Type
	RawTableName string
	IsSlice      bool
	IsPtr        bool
	IsStruct     bool
	RawData      interface{}
}

func getScanners(v interface{}) []IScanner {

}

var columnToFieldNameMapCache = make(map[reflect.Type]map[string]string)

func getColumnToFieldNameMap(v RawTableInfo) map[string]string {
	columnMap := make(map[string]string)
	oo := v.Type
	cc, ok := columnToFieldNameMapCache[oo]
	if ok {
		return cc
	}
	for i := 0; i < oo.NumField(); i++ {
		field := oo.Field(i)
		colName := getColumnName(field)
		columnMap[colName] = field.Name
	}
	return columnMap
}
func GetRawTableInfo(v interface{}) RawTableInfo {
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

	return RawTableInfo{tt, tableName, isSlice, isPtr, isStruct, v}
}

func getColumns(v reflect.Value) ([]string, []Column, map[string]int) {
	var columnNames []string
	var columns []Column
	var columnIdxMap = make(map[string]int)
	oo := v.Type()

	for i := 0; i < oo.NumField(); i++ {
		field := oo.Field(i)
		col, tps := getColumnFromField(v.Field(i), field)
		columns = append(columns, col) //默认都插入一个
		columnIdxMap[col.ColumnName] = i
		if tps != -1 {
			columnNames = append(columnNames, col.ColumnName)
		}
	}
	if Debug {
		fmt.Println("columns are:", columns)
	}
	return columnNames, columns, columnIdxMap
}

//	func Md5Text(str string) string {
//		h := md5.New()
//		h.Write([]byte(str))
//		return hex.EncodeToString(h.Sum(nil))
//	}
func getColumnFromField(v reflect.Value, filed reflect.StructField) (Column, int) {
	colName, tps := getColumnName(filed)
	if Debug {
		fmt.Println("Tag is:", colName, "type is:", tps)
	}
	return Column{Data: v.Interface(), ColumnName: colName, FieldName: filed.Name, PrimaryAuto: tps == 2, Primary: tps == 1 || tps == 2}, tps

}
func getColumnName(field reflect.StructField) string {
	tag, hasTag := field.Tag.Lookup("gom")
	if !hasTag || strings.EqualFold(tag, "-") {
		tag = CamelToSnakeString(field.Name)
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
		cMap := getColumnToFieldNameMap(rawInfo)
		for key, fieldName := range cMap {
			if len(columns) > 0 {
				_, ok := colMap[key]
				if ok {
					newMap[key] = reflect.ValueOf(vs).FieldByName(fieldName).Interface()
				}
			} else {
				val := reflect.ValueOf(vs).FieldByName(fieldName)
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

func ScannerResultToStruct(t reflect.Type, scanners []interface{}, columnNames []string, columnIdxMap map[string]int) reflect.Value {
	v := reflect.Indirect(reflect.New(t))
	for i, name := range columnNames {
		if _, ok := scanners[i].(EmptyScanner); !ok { //不能时空扫描器
			val, er := scanners[i].(IScanner).Value()
			if er != nil {
				panic(er)
			}
			idx, ok := columnIdxMap[name]
			if ok && val != nil {
				v.Field(idx).Set(reflect.ValueOf(val))
			}
		}

	}
	return v
}
