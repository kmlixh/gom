package gom

import (
	"time"
)

// TestModel 测试用的模型结构
type TestModel struct {
	ID        int64     `gom:"id,@,auto"`
	Name      string    `gom:"name,notnull"`
	Age       int       `gom:"age,notnull"`
	CreatedAt time.Time `gom:"created_at,notnull,default"`
}

func (m *TestModel) TableName() string {
	return "tests"
}
