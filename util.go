package gom

import (
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

func GetTableModel(v interface{}, columns ...string) (TableModel, error) {
	return getTableModel(v, columns...)

}
func CreateSingleValueTableModel(v interface{}, table string, field string) TableModel {
	tt, _, _ := getType(v)
	vals := reflect.New(tt).Elem()
	columns := make(map[string]Column)
	columns[field] = Column{ColumnName: field, Type: tt, IsPrimary: false, Auto: false}
	return TableModel{Columns: columns, ColumnNames: []string{field}, TableName: table, Type: tt, Value: vals}
}

var mutex sync.Mutex
var tableModelCache map[string]TableModel

func getTableModel(v interface{}, choosedColumns ...string) (TableModel, error) {
	//防止重复创建map，需要对map创建过程加锁
	mutex.Lock()
	if tableModelCache == nil {
		tableModelCache = make(map[string]TableModel)
	}
	mutex.Unlock()
	tt, isPtr, isSlice := getType(v)
	_, hasMethod := tt.MethodByName("TableName")
	if tt.Kind() != reflect.Struct || (tt.Kind() == reflect.Struct && !hasMethod) || tt.NumField() == 0 {
		return TableModel{}, errors.New(tt.Name() + " is not a valid struct")
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
		var model TableModel
		cachedModel, ok := tableModelCache[tt.String()]
		if ok {
			model = cachedModel.Clone(value, choosedColumns...)
		} else {
			nameMethod := value.MethodByName("TableName")
			if debug {
				fmt.Println(nameMethod)
			}
			tableName := nameMethod.Call(nil)[0].String()
			columnNames, columnMap, primary := getColumns(value)
			temp := TableModel{Type: tt, Value: reflect.New(tt), ColumnNames: columnNames, Columns: columnMap, TableName: tableName, Primary: primary}
			tableModelCache[tt.String()] = temp
			model = temp.Clone(value, choosedColumns...)
		}
		return model, nil

	} else {
		return TableModel{}, errors.New("can't use interface")
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
		columns[col.ColumnName] = col
		columnNames = append(columnNames, col.ColumnName)
		if tps != -1 {
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
func getColumnFromField(filed reflect.StructField) (Column, int) {
	tag, tps := getTagFromField(filed)
	if debug {
		fmt.Println("Tag is:", tag, "type is:", tps)
	}
	v := reflect.New(filed.Type)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if tps != -1 {
		return Column{Type: v.Type(), ColumnName: tag, FieldName: filed.Name, Auto: tps == 2, IsPrimary: tps == 1 || tps == 2}, tps
	} else {
		return Column{}, -1
	}

}
func getTagFromField(field reflect.StructField) (string, int) {
	tag, hasTag := field.Tag.Lookup("gom")
	if hasTag {
		if strings.EqualFold(tag, "-") || len(tag) == 0 {
			return "", -1
		} else if len(tag) == 1 {
			tps := 0
			if strings.EqualFold(tag, "@") {
				tps = 2
			}
			if strings.EqualFold(tag, "!") {
				tps = 1
			}
			return strings.ToLower(field.Name), tps
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
func getValueOfTableRow(model TableModel, row RowChooser) reflect.Value {
	maps := getDataMap(model, row)
	vv := reflect.New(model.Type).Elem()
	isStruct := model.Type.Kind() == reflect.Struct && model.Type != reflect.TypeOf(time.Time{})
	for _, name := range model.ColumnNames {
		c := model.Columns[name]
		if debug {
			fmt.Println("column is:", c.ColumnName, ",column type is:", c.Type, ",value type is:", reflect.TypeOf(maps[c.ColumnName]))
		}
		scanner, ok := maps[c.ColumnName]
		if ok {
			result, _ := scanner.Value()
			if isStruct {
				if reflect.Indirect(reflect.ValueOf(scanner)).Type() == c.Type {
					//如果列本身就是IScanner的话，那么直接赋值
					vv.FieldByName(c.FieldName).Set(reflect.Indirect(reflect.ValueOf(scanner)))
				} else {
					vv.FieldByName(c.FieldName).Set(reflect.Indirect(reflect.ValueOf(result)))
				}
			} else {
				//如果对象本身就是一个基础类型，那么直接赋值
				vv.Set(reflect.Indirect(reflect.ValueOf(result)))
			}
		} else {
			panic("can't transfer data of type:" + c.Type.Name())
		}
	}
	return vv
}
func getDataMap(model TableModel, row RowChooser) map[string]IScanner {
	var dest []interface{} // A temporary interface{} slice
	for _, name := range model.ColumnNames {
		v := model.Columns[name]
		result := getValueOfType(v)
		dest = append(dest, result)
	}
	err := row.Scan(dest...)
	if err != nil {
		fmt.Println(err)
	}
	result := make(map[string]IScanner, len(model.ColumnNames))
	for i, dd := range dest {
		result[model.ColumnNames[i]] = dd.(IScanner)
	}
	return result

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
		return &Scanner{c.ColumnName, 0, Int32Scan}
	case int64:
		return &Scanner{c.ColumnName, int64(0), Int64Scan}
	case float32:
		return &Scanner{c.ColumnName, float32(0), Float32Scan}
	case float64:
		return &Scanner{c.ColumnName, float64(0), Float64Scan}
	case string:
		return &Scanner{c.ColumnName, "", StringScan}
	case []byte:
		return &Scanner{c.ColumnName, []byte{}, ByteArrayScan}
	case time.Time:
		return &Scanner{c.ColumnName, time.Time{}, TimeScan}
	case bool:
		return &Scanner{c.ColumnName, false, BoolScan}
	default:
		panic(errors.New("unsupported type '" + reflect.New(c.Type).String() + "' you would changed it!"))
	}
}
