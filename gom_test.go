package gom

import (
	_ "github.com/kmlixh/gom/v2/factory/mysql"
	"testing"
)

func TestOpen(t *testing.T) {

	tests := []struct {
		name string
		t    func(t *testing.T)
	}{
		{"默认创建测试", func(t *testing.T) {
			db, er := Open("mysql", dsn, false)
			if er != nil {
				t.Error(er, db)
			}
		}},
		{"带配置的创建", func(t *testing.T) {
			db, er := OpenWithConfig("mysql", dsn, 10, 20, false)
			if er != nil {
				t.Error(er, db)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.t(t)
		})
	}
}
