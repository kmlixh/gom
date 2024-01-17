package gom

import "testing"

func TestGenTableModel(t *testing.T) {
	type args struct {
		db          *DB
		packageName string
		fileName    string
		tables      []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"测试生成默认的Struct", args{
			db:          mysqlDb,
			packageName: "main",
			fileName:    "test.go",
			tables:      []string{"user"},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := GenDefaultStructFromDatabase(tt.args.db, tt.args.packageName, tt.args.fileName, tt.args.tables...); (err != nil) != tt.wantErr {
				t.Errorf("GenDefaultStructFromDatabase() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
