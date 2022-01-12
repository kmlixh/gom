package structs

import (
	"database/sql"
	"reflect"
	"time"
)

var dataTransferCache = make(map[string]DataTransfer)

type DataTransfer struct {
	scanners []interface{}
	columns  []string
	dataIdx  []int
	model    StructModel
}

func (dd DataTransfer) Model() StructModel {
	return dd.model
}

func (dd DataTransfer) GetValueOfTableRow(rows *sql.Rows) reflect.Value {
	model := dd.model
	rows.Scan(dd.scanners...)
	vv := reflect.New(model.Type).Elem()
	isStruct := model.Type.Kind() == reflect.Struct && model.Type != reflect.TypeOf(time.Time{})
	for _, idx := range dd.dataIdx {
		c := model.Columns[dd.columns[idx]]

		scanner := dd.scanners[idx].(IScanner)
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
	}
	return vv
}

//key是查询类sql自动生成的md5校验值,即相同的sql就会有相同的key值,key+tableModel指定唯一一个datatransfer
func GetDataTransfer(key string, columns []string, model StructModel) DataTransfer {
	//将结果集的列和model的列做拟合的时候,必然会存在表列和columns不一致的情况.这个时候需要我们创造一个DataTransfer,Data,并且将datatransfer缓存到静态map中,后续直接从map中取用,无需再次优化
	dd, ok := dataTransferCache[key+"_"+model.Type.Name()]
	if !ok {
		//手工初始化
		var scanners []interface{}
		var dataIdx []int
		for i, col := range columns {
			var scanner IScanner
			cc, ok := model.Columns[col]
			if ok {
				dataIdx = append(dataIdx, i)
				scanner = GetValueOfType(cc)
			} else {
				scanner = EmptyScanner()
			}
			scanners = append(scanners, scanner)
		}
		dd = DataTransfer{scanners: scanners, columns: columns, model: model, dataIdx: dataIdx}
		dataTransferCache[key+"_"+model.Type.Name()] = dd
	}
	return dd
}
