package structs

import (
	"errors"
	"fmt"
	"gitee.com/janyees/gom/arrays"
	"gitee.com/janyees/gom/cnds"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

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

	return RawTableInfo{tt, tableName, isSlice, isPtr, isStruct}
}

var mutex sync.Mutex
var tableModelCache map[string]TableModel

func GetTableModel(v interface{}, choosedColumns ...string) (TableModel, error) {
	//防止重复创建map，需要对map创建过程加锁
	if v == nil {
		return &DefaultTableModel{}, nil
	}
	mutex.Lock()
	if tableModelCache == nil {
		tableModelCache = make(map[string]TableModel)
	}
	mutex.Unlock()
	rawTableInfo := GetRawTableInfo(v)
	if !rawTableInfo.IsStruct && (choosedColumns == nil || len(choosedColumns) != 1) {
		return nil, errors.New("basic Type Only Support [1] Column Or2 nil")
	}

	var model TableModel
	cachedModel, ok := tableModelCache[rawTableInfo.PkgPath()+"-"+rawTableInfo.String()]
	if ok {
		model = cachedModel.Clone()
	} else {

		var temp TableModel
		var scanners []IScanner
		tempVal := reflect.Indirect(reflect.New(rawTableInfo.Type))
		if rawTableInfo.IsStruct {
			if rawTableInfo.IsStruct && rawTableInfo.Type.NumField() == 0 {
				_, ok := reflect.Indirect(reflect.New(rawTableInfo.Type)).Interface().(DefaultStruct)
				if !ok {
					return nil, errors.New(fmt.Sprintf("[%s] was a \"empty struct\",it has no field or All fields has been ignored", rawTableInfo.Type.Name()))
				} else {
					return &DefaultTableModel{}, nil
				}
			}
			columnNames, columns, columnIdxMap := getColumns(tempVal)
			for _, column := range columns {
				scanners = append(scanners, GetIScannerOfColumn(column.ColumnName, column.Data))
			}
			temp = &DefaultTableModel{rawScanners: scanners, rawType: rawTableInfo.Type, rawTable: rawTableInfo.RawTableName, rawColumns: columns, rawColumnNames: columnNames, rawColumnIdxMap: columnIdxMap, primaryAuto: columns[0].PrimaryAuto}
		} else {
			scanners = append(scanners, GetIScannerOfColumn("", reflect.Indirect(reflect.New(rawTableInfo.Type)).Interface()))
			temp = &DefaultTableModel{rawScanners: scanners, rawType: rawTableInfo.Type, rawTable: "", primaryAuto: false}
		}
		tableModelCache[rawTableInfo.PkgPath()+"-"+rawTableInfo.String()] = temp
		model = temp.Clone()
	}
	model.SetData(v, reflect.Indirect(reflect.ValueOf(v)), rawTableInfo.IsStruct, rawTableInfo.IsPtr, rawTableInfo.IsSlice)
	er := model.SetColumns(choosedColumns)
	return model, er
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

//func Md5Text(str string) string {
//	h := md5.New()
//	h.Write([]byte(str))
//	return hex.EncodeToString(h.Sum(nil))
//}
func getColumnFromField(v reflect.Value, filed reflect.StructField) (Column, int) {
	colName, tps := getColumnNameAndTypeFromField(filed)
	if Debug {
		fmt.Println("Tag is:", colName, "type is:", tps)
	}
	return Column{Data: v.Interface(), ColumnName: colName, FieldName: filed.Name, PrimaryAuto: tps == 2, Primary: tps == 1 || tps == 2}, tps

}
func getColumnNameAndTypeFromField(field reflect.StructField) (string, int) {
	tag, hasTag := field.Tag.Lookup("gom")
	if !hasTag {
		tag = CamelToSnakeString(field.Name)
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
		return CamelToSnakeString(field.Name), tps
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
}

func StructToMap(vs interface{}, columns ...string) (map[string]interface{}, []string, error) {
	if vs == nil {
		return nil, nil, errors.New("nil can't be used to create Map")
	}
	rawInfo := GetRawTableInfo(vs)
	if rawInfo.IsSlice {
		return nil, nil, errors.New("can't convert slice or array to map")
	}
	if rawInfo.Kind() == reflect.Struct {
		if rawInfo.Type.NumField() == 0 {
			//
			return nil, nil, errors.New(fmt.Sprintf("[%s] was a \"empty struct\",it has no field or All fields has been ignored", rawInfo.Type.Name()))
		}
		//TODO 下面的方法过于复杂
		colNames, cols, _ := getColumns(reflect.ValueOf(vs))
		if colNames == nil || len(colNames) == 0 {
			panic(fmt.Sprintf("can't get any data from Type [%s]", rawInfo.Name()))
		}
		columns = arrays.Intersect(columns, colNames)
		newMap := make(map[string]interface{})
		for i, column := range columns {
			newMap[column] = cols[i].Data
		}
		return newMap, columns, nil
	}
	return nil, nil, errors.New(fmt.Sprintf("can't convert %s to map", rawInfo.Name()))

}
func StructToCondition(vs interface{}, columns ...string) cnds.Condition {
	maps, _, err := StructToMap(vs, columns...)
	if err != nil {
		panic(err)
	}
	return MapToCondition(maps)
}
func MapToCondition(maps map[string]interface{}) cnds.Condition {
	if maps == nil {
		return nil
	}
	var cnd cnds.Condition
	for k, v := range maps {
		t := reflect.TypeOf(v)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if (t.Kind() != reflect.Struct && t.Kind() != reflect.Slice) || t.Kind() == reflect.TypeOf(time.Now()).Kind() {
			value := v
			if cnd == nil {
				cnd = cnds.New(k, cnds.Eq, value)
			} else {
				cnd.And(k, cnds.Eq, arrays.Of(value))
			}
		}
	}
	return cnd
}
func UnZipSlice(vs interface{}) []interface{} {
	var result = make([]interface{}, 0)
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

func GetGoid() int64 {
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

//func Difference(slice1, slice2 []string) []string {
//	m := make(map[string]int)
//	nn := make([]string, 0)
//	inter := Intersect(slice1, slice2)
//	for _, v := range inter {
//		m[v]++
//	}
//
//	for _, value := range slice1 {
//		times, _ := m[value]
//		if times == 0 {
//			nn = append(nn, value)
//		}
//	}
//	return nn
//}
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
