package gom

import (
	"time"
)

// TestModelForModels 测试用的模型结构
type TestModelForModels struct {
	ID        int64     `gom:"id,primary_key,auto_increment"`
	Name      string    `gom:"name,size:255"`
	Age       int       `gom:"age"`
	CreatedAt time.Time `gom:"created_at"`
}

func (m *TestModelForModels) TableName() string {
	return "test_models"
}
