package gom

import (
	"errors"
	"testing"
	"time"

	"github.com/kmlixh/gom/v4/define"
)

var (
	ErrTest = errors.New("test error")
)

// initTestDB 设置测试数据库连接
func initTestDB(t *testing.T) *DB {
	opts := define.DBOptions{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: 30 * time.Second,
		Debug:           true,
	}

	db, err := Open("mysql", "root:123456@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=true", &opts)
	if err != nil {
		t.Logf("Failed to connect to database: %v", err)
		return nil
	}
	return db
}
