package structs

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"gom/err"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func IsEmpty(v interface{}) bool {
	vv := reflect.ValueOf(v)
	return vv.IsZero()
}
func GetType(v interface{}) (reflect.Type, bool, bool) {
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
	if Debug {
		fmt.Println("Test GetType, result:", tt, isPtr, islice)
	}
	return tt, isPtr, islice
}

var mutex sync.Mutex
var tableModelCache map[string]StructModel

func GetStructModel(v interface{}, choosedColumns ...string) (StructModel, error) {
	//防止重复创建map，需要对map创建过程加锁
	mutex.Lock()
	if tableModelCache == nil {
		tableModelCache = make(map[string]StructModel)
	}
	mutex.Unlock()
	tt, isPtr, isSlice := GetType(v)
	_, hasTable := reflect.New(tt).Interface().(Table)
	tableName := CamelToSnakeString(tt.Name())
	if hasTable {
		tableName = reflect.New(tt).Interface().(Table).TableName()
	}
	if tt.Kind() != reflect.Struct || (tt.Kind() == reflect.Struct && tt.NumField() == 0) {
		return StructModel{}, errors.New(tt.Name() + " is not a valid structs")
	}

	if v != nil && tt.Kind() != reflect.Interface {
		dstValue := reflect.ValueOf(v)
		if isPtr {
			dstValue = dstValue.Elem()
		}

		if Debug {
			fmt.Println("model info:", tt, isPtr, isSlice, dstValue)
		}
		var model StructModel
		cachedModel, ok := tableModelCache[tt.String()]
		if ok {
			model = cachedModel.Clone(dstValue, choosedColumns...)
		} else {
			tempVal := dstValue
			if isSlice {
				tempVal = reflect.Indirect(reflect.New(tt))
			}
			columnNames, columnMap, primary := getColumns(tempVal)
			temp := StructModel{Type: tt, Value: tempVal, ColumnNames: columnNames, ColumnMap: columnMap, TableName: tableName, Primary: primary}
			tableModelCache[tt.String()] = temp
			model = temp.Clone(dstValue, choosedColumns...)
		}
		return model, nil

	} else {
		return StructModel{}, errors.New("can't use interface")
	}
}

func getColumns(v reflect.Value) ([]string, map[string]Column, Column) {
	var columnNames []string
	columnMap := make(map[string]Column)
	var primary Column
	oo := v.Type()
	for i := 0; i < oo.NumField(); i++ {
		field := oo.Field(i)
		col, tps := getColumnFromField(field)
		if tps != -1 {
			columnMap[col.ColumnName] = col
			columnNames = append(columnNames, col.ColumnName)
			if tps == 1 || tps == 2 {
				primary = col
			}
		}
	}
	if Debug {
		fmt.Println("columnMap is:", columnMap)
	}
	return columnNames, columnMap, primary
}
func Md5Text(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
func getColumnFromField(filed reflect.StructField) (Column, int) {
	colName, tps := getColumnNameAndTypeFromField(filed)
	if Debug {
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
func ModelToMap(model StructModel) (map[string]interface{}, []string, error) {
	maps := make(map[string]interface{})
	var keys []string

	for _, col := range model.ColumnNames {
		vv := model.Value.FieldByName(model.ColumnMap[col].FieldName)
		result := vv.Interface()
		ignore := false
		switch result.(type) {
		case time.Time:
			if !model.HasColumnFilter && vv.Interface().(time.Time).IsZero() {
				ignore = true
			}
		default:
			if !model.HasColumnFilter && vv.IsZero() {
				ignore = true
			}
		}
		if !ignore {
			keys = append(keys, col)
			maps[col] = result
		}
	}

	return maps, keys, nil
}
func StructToMap(vs interface{}, columns ...string) (map[string]interface{}, []string, error) {
	t, _, isSlice := GetType(vs)
	if isSlice {
		return nil, nil, err.Error("can't convert slice or array to map")
	}

	if t.Kind() == reflect.Struct {
		model, err := GetStructModel(vs, columns...)
		if err != nil {
			panic(err)
		}
		return ModelToMap(model)
	}
	return nil, nil, err.Error(fmt.Sprintf("can't convert %s to map", t.Name()))

}
func StructToCondition(vs interface{}, columns ...string) Condition {
	maps, _, err := StructToMap(vs, columns...)
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
				cnd = Cnd(k, Eq, value)
			} else {
				cnd.And(k, Eq, value)
			}
		}
	}
	return cnd
}
func GetValueOfType(c Column) IScanner {
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
		return EmptyScanner()
	}
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
				result = append(result, UnZipSlice(item.Interface())...)
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
func Intersect(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times, _ := m[v]
		if times == 1 {
			nn = append(nn, v)
		}
	}
	return nn
}
func Difference(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	inter := Intersect(slice1, slice2)
	for _, v := range inter {
		m[v]++
	}

	for _, value := range slice1 {
		times, _ := m[value]
		if times == 0 {
			nn = append(nn, value)
		}
	}
	return nn
}
