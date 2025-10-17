package gom

import "time"

// 示例：新的标签使用方式

// 自增主键示例
type AutoIncrementUser struct {
	ID        int64     `gom:"id,@"` // @ 表示自增主键
	Name      string    `gom:"name"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
}

// 或者使用 auto 标签
type AutoIncrementUser2 struct {
	ID        int64     `gom:"id,auto"` // auto 也表示自增主键
	Name      string    `gom:"name"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
}

// 普通主键示例（非自增）
type ManualKeyUser struct {
	ID        int64     `gom:"id,!"` // ! 表示普通主键（非自增）
	Name      string    `gom:"name"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
}

// 或者使用 pk 标签
type ManualKeyUser2 struct {
	ID        int64     `gom:"id,pk"` // pk 也表示普通主键（非自增）
	Name      string    `gom:"name"`
	Email     string    `gom:"email"`
	CreatedAt time.Time `gom:"created_at"`
}

// 使用示例
func ExampleUsage() {
	// 注意：Save 方法已删除，请使用 Insert 或 Update 方法

	// 自增主键的使用
	autoUser := &AutoIncrementUser{
		Name:  "张三",
		Email: "zhangsan@example.com",
	}

	// 插入自增主键记录
	// result := db.Chain().Insert(autoUser)
	// 插入后，autoUser.ID 会被自动设置为数据库生成的值

	// 普通主键的使用
	manualUser := &ManualKeyUser{
		ID:    1001, // 手动指定ID
		Name:  "李四",
		Email: "lisi@example.com",
	}

	// 插入普通主键记录
	// result := db.Chain().Insert(manualUser)

	// 更新记录
	// result := db.Chain().Update(manualUser)
}
