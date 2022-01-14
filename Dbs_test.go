package gom

import (
	"database/sql"
	"gitee.com/janyees/gom/structs"
	"reflect"
	"testing"
)

func TestDB_CleanOrders(t *testing.T) {
	db1 := DB{}
	db2 := DB{}
	db3 := DB{}
	db2.OrderBy("name", structs.Desc)
	db2.OrderBy("name", structs.Desc).OrderByDesc("use")
	tests := []struct {
		name string
		raw  DB
		want []structs.OrderBy
	}{
		{"empty orders clean", db1, []structs.OrderBy{}},
		{"有一个时除去", db2, []structs.OrderBy{}},
		{"有多个时清空", db3, []structs.OrderBy{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := tt.raw
			if got := this.CleanOrders().orderBys; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CleanOrders() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Columns(t *testing.T) {

	type args struct {
		cols []string
	}
	tests := []struct {
		name string
		args []string
	}{
		{"set columns", []string{"name", "age", "test"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{}
			if got := this.Columns(tt.args...); !reflect.DeepEqual(got.cols, tt.args) {
				t.Errorf("Columns() = %v, want %v", got, tt.args)
			}
		})
	}
}

func TestDB_Count(t *testing.T) {

	type args struct {
		tableName  string
		columnName string
	}
	tests := []struct {
		name string
		db   *DB
		args args
		want int64
	}{
		// TODO: Add test cases.
		{"Count测试", db, args{"tb_record", "id"}, 243},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := tt.db
			if got := this.Table(tt.args.tableName).Count(tt.args.columnName); !reflect.DeepEqual(got.Count, tt.want) {
				t.Errorf("Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Delete(t *testing.T) {

	type args struct {
		vs []interface{}
	}
	tests := []struct {
		name    string
		db      *DB
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thiz := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := thiz.Delete(tt.args.vs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Execute(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		sqlType structs.SqlType
		vs      []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thiz := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := thiz.Execute(tt.args.sqlType, tt.args.vs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_First(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		vs interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.First(tt.args.vs)
			if (err != nil) != tt.wantErr {
				t.Errorf("First() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("First() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Insert(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		vs []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thiz := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := thiz.Insert(tt.args.vs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Insert() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_OrderBy(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		field string
		t     structs.OrderType
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.OrderBy(tt.args.field, tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OrderBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_OrderByAsc(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		field string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.OrderByAsc(tt.args.field); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OrderByAsc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_OrderByDesc(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		field string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.OrderByDesc(tt.args.field); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("OrderByDesc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Page(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		index    int
		pageSize int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.Page(tt.args.index, tt.args.pageSize); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Page() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Raw(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		sql   string
		datas []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.Raw(tt.args.sql, tt.args.datas...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Raw() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_RawDb(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
		want   *sql.DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.RawDb(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RawDb() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Select(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		vs interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.Select(tt.args.vs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Select() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Select() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_SelectByModel(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		model structs.StructModel
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.SelectByModel(tt.args.model)
			if (err != nil) != tt.wantErr {
				t.Errorf("SelectByModel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SelectByModel() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Sum(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		columnName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   structs.CountResult
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.Sum(tt.args.columnName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Table(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		table string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.Table(tt.args.table); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Table() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Transaction(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		work TransactionWork
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.Transaction(tt.args.work)
			if (err != nil) != tt.wantErr {
				t.Errorf("Transaction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Transaction() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Update(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		vs []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			thiz := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := thiz.Update(tt.args.vs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Update() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Where(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		cnd structs.Condition
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.Where(tt.args.cnd); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Where() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Where2(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		sql     string
		patches []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.Where2(tt.args.sql, tt.args.patches...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Where2() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_clone(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
		want   DB
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.clone(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clone() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_cloneIfOriginRoutine(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := &DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			this.cloneIfOriginRoutine()
		})
	}
}

func TestDB_execute(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		sql  string
		data []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    sql.Result
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.execute(tt.args.sql, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("execute() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_getInsertColumns(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		model structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.getInsertColumns(tt.args.model); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getInsertColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_getQueryColumns(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.getQueryColumns(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getQueryColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_getTableName(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.getTableName(); got != tt.want {
				t.Errorf("getTableName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_getUpdateColumns(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			if got := this.getUpdateColumns(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getUpdateColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_query(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		sql   string
		data  []interface{}
		model structs.StructModel
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.query(tt.args.sql, tt.args.data, tt.args.model)
			if (err != nil) != tt.wantErr {
				t.Errorf("query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("query() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_subExecute(t *testing.T) {
	type fields struct {
		id       int64
		factory  structs.SqlFactory
		db       *sql.DB
		cnd      structs.Condition
		table    string
		rawSql   string
		rawData  []interface{}
		tx       *sql.Tx
		orderBys []structs.OrderBy
		cols     []string
		page     structs.Page
		model    structs.StructModel
	}
	type args struct {
		sqlType structs.SqlType
		vs      []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			this := DB{
				id:       tt.fields.id,
				factory:  tt.fields.factory,
				db:       tt.fields.db,
				cnd:      tt.fields.cnd,
				table:    tt.fields.table,
				rawSql:   tt.fields.rawSql,
				rawData:  tt.fields.rawData,
				tx:       tt.fields.tx,
				orderBys: tt.fields.orderBys,
				cols:     tt.fields.cols,
				page:     tt.fields.page,
				model:    tt.fields.model,
			}
			got, err := this.subExecute(tt.args.sqlType, tt.args.vs...)
			if (err != nil) != tt.wantErr {
				t.Errorf("subExecute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("subExecute() got = %v, want %v", got, tt.want)
			}
		})
	}
}
