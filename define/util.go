package define

import (
	"crypto/md5"
	"encoding/hex"
	"reflect"
	"strings"
)

func Md5Text(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
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

func GetDefaultsColumnFieldMap(rawType reflect.Type) (map[string]FieldInfo, []string) {
	columns := make([]string, 0)
	columnMap := make(map[string]FieldInfo)
	cc, ok := columnToFieldNameMapCache[rawType]
	cols, okk := columnsCache[rawType]
	if ok && okk {
		return cc, cols
	}
	for i := 0; i < rawType.NumField(); i++ {
		field := rawType.Field(i)
		colName, _ := getColumnNameAndTypeFromField(field)
		if len(colName) > 0 {

			columnMap[colName] = FieldInfo{
				FieldName: field.Name,
				FieldType: field.Type,
			}
			columns = append(columns, colName)
		}
	}
	columnToFieldNameMapCache[rawType] = columnMap
	columnsCache[rawType] = columns
	return columnMap, columns
}

func ScannerResultToStruct(t reflect.Type, scanners []interface{}, columnNames []string) reflect.Value {
	v := reflect.Indirect(reflect.New(t))
	colsMap, _ := GetDefaultsColumnFieldMap(t)
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
