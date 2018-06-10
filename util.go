package gom

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"encoding/json"
	"strconv"
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

func GetTableModel(v interface{}, names ...string) (TableModel, error) {
	tms, err := getTableModel(v)
	if err != nil {
		return TableModel{}, nil
	}
	tm := tms[0]
	var cc []Column
	for _, ct := range tm.Columns {
		for _, name := range names {
			if ct.ColumnName == name {
				cc = append(cc, ct)
			}
		}
	}
	tm.Columns = cc
	return tm, nil

}
func CreateSingleValueTableModel(v interface{}, table string, field string) TableModel {
	tt, _, _ := getType(v)
	vals := reflect.New(tt).Elem()
	columns := []Column{{ColumnName: field, Type: tt, IsPrimary: false, Auto: false}}
	return TableModel{Columns: columns, TableName: table, ModelType: tt, ModelValue: vals}
}
func getTableModels(vs ...interface{}) ([]TableModel, error) {
	tablemodels := []TableModel{}
	for _, v := range vs {
		tt, _, _ := getType(v)
		if tt.Kind() == reflect.Interface {
			return tablemodels, errors.New("can't use interface as struct")
		}
		tbs, err := getTableModel(v)
		if err != nil {
			return tablemodels, err
		}
		tablemodels = append(tablemodels, tbs...)
	}
	return tablemodels, nil
}
func getTableModel(v interface{}) ([]TableModel, error) {
	var tableModels []TableModel
	var values []reflect.Value
	tt, isPtr, isSlice := getType(v)
	if tt.NumField() == 0 || tt.NumMethod() == 0 {
		return tableModels, errors.New(tt.Name() + " is not a valid struct")
	}
	value := reflect.ValueOf(v)
	if v != nil && tt.Kind() != reflect.Interface {

		if isPtr {
			value = value.Elem()
		}
		if debug {
			fmt.Println("model info:", tt, isPtr, isSlice, value)
		}
		if isSlice {
			if value.Len() > 0 {
				for i := 0; i < value.Len(); i++ {
					val := value.Index(i)
					values = append(values, val)
				}
			} else {
				values = append(values, reflect.New(tt).Elem())
			}
		} else {
			values = append(values, value)
		}
		if debug {
			fmt.Println(values)
		}
		for _, val := range values {
			nameMethod := val.MethodByName("TableName")
			if debug {
				fmt.Println(nameMethod)
			}
			tableName := nameMethod.Call(nil)[0].String()
			columns, primary := getColumns(val)
			ccs := []Column{primary}
			ccs = append(ccs, columns...)
			tableModels = append(tableModels, TableModel{ModelType: tt, ModelValue: val, Columns: ccs, TableName: tableName, Primary: primary})
		}
		return tableModels, nil

	} else {
		return tableModels, errors.New("can't use interface to build TableModel")
	}
}
func getColumns(v reflect.Value) ([]Column, Column) {
	var columns []Column
	var primary Column
	results := reflect.Indirect(reflect.ValueOf(&columns))
	oo := v.Type()

	for i := 0; i < oo.NumField(); i++ {
		field := oo.Field(i)
		col, tps := getColumnFromField(field)
		if tps != -1 {
			if tps == 1 || tps == 2 {
				primary = col
			} else {
				n := reflect.Indirect(reflect.ValueOf(&col))
				if results.Kind() == reflect.Ptr {
					results.Set(reflect.Append(results, n.Addr()))
				} else {
					results.Set(reflect.Append(results, n))
				}
			}

		}
	}
	if debug {
		fmt.Println("columns is:", columns)
	}
	return columns, primary
}
func getColumnFromField(filed reflect.StructField) (Column, int) {
	tag, tps := getTagFromField(filed)
	if debug {
		fmt.Println("Tag is:", tag, "type is:", tps)
	}
	v:=reflect.New(filed.Type)
	if v.Kind()==reflect.Ptr{
		v=v.Elem()
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
	vv := reflect.New(model.ModelType).Elem()
	isStruct := model.ModelType.Kind() == reflect.Struct && model.ModelType != reflect.TypeOf(time.Time{})
	if debug {
		fmt.Println("vv kind is:",vv.Kind())
	}
	for _, c := range model.Columns {
		if isStruct {
			vv.FieldByName(c.FieldName).Set(reflect.ValueOf(maps[c.FieldName]).Elem())
		} else {
			vv.Set(reflect.ValueOf(maps[c.FieldName]).Elem())
		}
	}
	return vv
}
func getDataMap(model TableModel, row RowChooser) map[string]interface{} {
	dest := getArrayFromColumns(model.Columns)
	err := row.Scan(dest...)
	if err != nil {
		fmt.Println(err)
		return map[string]interface{}{}
	}
	result := make(map[string]interface{}, len(model.Columns))
	ccs := model.Columns
	for i, dd := range ccs {
		result[dd.FieldName] = dest[i]
	}
	if debug {
		fmt.Println(json.Marshal(result))
	}
	return result

}
func getArrayFromColumns(columns []Column) []interface{}{
	dest := make([]interface{}, len(columns)) // A temporary interface{} slice
	for i,v:=range columns{
		result:=getValueOfType(v)
		dest[i]=&result
	}
	return dest
}
func getValueOfType(c Column) interface{} {
	vi := reflect.New(c.Type).Elem()
	switch vi.Interface().(type) {
	case CustomScanner,BinaryUnmarshaler:
		result,ok:=vi.Interface().(CustomScanner)
		if ok{
			res,_:=result.Value()
			return res
		}else{
			return []byte{}
		}
	case uint:
		return uint(0)
	case uint16:
		return uint16(0)
	case uint32:
		return uint32(0)
	case uint64:
		return uint64(0)
	case int:
		return int(0)
	case int8:
		return int8(0)
	case int16:
		return int16(0)
	case int32:
		return int32(0)
	case int64:
		return int64(0)
	case float32:
		return float32(0)
	case float64:
		return float64(0)
	case string:
		return ""
	case []byte:
		return []byte{}
	case time.Time:
		return time.Now()
	case bool:
		return false
	default:
		return []byte{}
	}
}
