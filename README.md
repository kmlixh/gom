# GOM - Go ORM and Model Generator

GOM 是一个强大的 Go 语言 ORM 框架和模型生成器，支持 PostgreSQL 和 MySQL 数据库。

## 特性

- 支持 PostgreSQL 和 MySQL 数据库
- 自动生成 Go 结构体模型
- 支持自定义表名和字段映射
- 支持多种数据类型（包括 Decimal、UUID、IP 等）
- 自动处理创建时间和更新时间
- 支持自定义标签风格
- 支持表名前缀和后缀处理
- 生成完整的 CRUD 方法
- 支持事务处理
- 内置模型注册机制

## 安装

```bash
go get -u github.com/kmlixh/gom/v4
```

## 快速开始

### 1. 使用代码生成器

```bash
# 安装代码生成器
go install github.com/kmlixh/gom/v4/gomen/cmd/gomen@latest

# PostgreSQL 示例
gomen -type postgres \
      -url "postgres://user:password@localhost:5432/dbname?sslmode=disable" \
      -pattern "public.user*" \
      -out "./models"

# MySQL 示例
gomen -type mysql \
      -url "user:password@tcp(localhost:3306)/dbname" \
      -prefix "t_" \
      -out "./models"
```

### 2. 使用生成的模型

```go
package main

import (
    "log"
    "github.com/kmlixh/gom/v4"
    "your/project/models"
)

func main() {
    // 连接数据库
    db, err := gom.Open("postgres", "postgres://user:password@localhost:5432/dbname?sslmode=disable", true)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 创建记录
    user := &models.User{
        Username: "john",
        Email:    "john@example.com",
    }
    if err := db.Chain().Create(user); err != nil {
        log.Fatal(err)
    }

    // 查询记录
    var users []*models.User
    if err := db.Chain().Where("age", ">", 18).Find(&users); err != nil {
        log.Fatal(err)
    }

    // 更新记录
    if err := db.Chain().Where("id", "=", 1).Update(map[string]interface{}{
        "status": "active",
    }); err != nil {
        log.Fatal(err)
    }

    // 删除记录
    if err := db.Chain().Where("id", "=", 1).Delete(); err != nil {
        log.Fatal(err)
    }
}
```

## 代码生成器选项

```
选项:
  -type string
        数据库类型 (mysql/postgres)
  -url string
        数据库连接URL
  -out string
        输出目录 (默认 "models")
  -package string
        包名 (默认 "models")
  -pattern string
        表名匹配模式 (PostgreSQL 可用 schema.table* 格式)
  -tag string
        标签风格 (gom/db) (默认 "gom")
  -prefix string
        表名前缀（生成时会去掉）
  -suffix string
        表名后缀（生成时会去掉）
  -db
        生成db标签
  -debug
        开启调试模式
```

## 数据类型映射

| 数据库类型 | Go 类型 | 说明 |
|------------|---------|------|
| INT/INTEGER | int | 32位整数 |
| BIGINT | int64 | 64位整数 |
| SMALLINT | int16 | 16位整数 |
| TINYINT | int8 | 8位整数 |
| DECIMAL/NUMERIC | decimal.Decimal | 精确小数 |
| FLOAT | float32 | 32位浮点数 |
| DOUBLE | float64 | 64位浮点数 |
| BOOLEAN/BOOL | bool | 布尔值 |
| VARCHAR/TEXT | string | 字符串 |
| TIME/TIMESTAMP | time.Time | 时间类型 |
| JSON | json.RawMessage | JSON数据 |
| UUID | uuid.UUID | UUID类型 |
| INET | net.IP | IP地址 |

## 标签说明

生成的结构体字段包含以下标签：

- `gom:"column_name"`: 字段映射
- `gom:"column_name,@"`: 主键
- `gom:"column_name,auto"`: 自增
- `gom:"column_name,notnull"`: 非空
- `json:"column_name"`: JSON标签
- `db:"column_name"`: 数据库标签（可选）

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License

## 统计方法

GOM 提供了以下统计方法：

```go
// 计算记录总数
count, err := db.Chain().Table("users").Count()

// 计算字段平均值
avgAge, err := db.Chain().Table("users").Eq("active", true).Avg("age")

// 计算字段总和
sumAge, err := db.Chain().Table("users").Eq("role", "admin").Sum("age")
```

这些统计方法都支持：
- 与条件查询方法配合使用
- 处理 NULL 值和空结果集
- 支持复杂条件组合

## 分页查询

GOM 提供了便捷的分页查询方法：

```go
// 使用模型的分页查询
pageInfo, err := db.Chain().
    Table("users").
    Eq("active", true).
    OrderBy("created_at").
    Page(1, 10).  // 第1页，每页10条
    PageInfo(&User{})

// 不使用模型的分页查询（返回原始数据）
rawPageInfo, err := db.Chain().
    Table("users").
    Fields("id", "username").
    Page(2, 5).   // 第2页，每页5条
    PageInfo(nil)
```

PageInfo 结构包含以下信息：
- `PageNum`: 当前页码
- `PageSize`: 每页大小
- `Total`: 总记录数
- `Pages`: 总页数
- `HasPrev`: 是否有上一页
- `HasNext`: 是否有下一页
- `List`: 当前页数据
- `IsFirstPage`: 是否是第一页
- `IsLastPage`: 是否是最后页

## 版本历史

### v4.1.3 (2025-01-03)

修复和改进：
- 修复 `test_types.go` 中的包名问题
- 改进 SQL 查询生成逻辑
  - 优化 ORDER BY 子句的处理
  - 修复条件组合时的括号处理
  - 统一 MySQL 和 PostgreSQL 的查询生成逻辑

### v4.1.0-ai (2024-12-31 10:15 UTC+8)

新特性：
- 增强 `Save`、`Update` 和 `Delete` 方法
  - 支持不定长参数，可同时处理多个对象
  - 自动事务支持，确保多对象操作的原子性
  - 智能事务管理：单对象操作不使用事务，多对象自动使用事务
  - 详细的错误信息，包含失败对象的序号
  - 自动回滚机制，任何操作失败时回滚整个事务
  - 影响行数检查，确保操作成功执行

使用示例：
```go
// 保存多个对象（自动使用事务）
user1 := &User{Name: "user1", Age: 20}
user2 := &User{Name: "user2", Age: 25}
result, err := db.Chain().
    Table("users").
    Save(user1, user2)

// 更新多个不同类型的对象（自动使用事务）
type UserRole struct {
    Role string `gom:"role"`
}
type UserStatus struct {
    Active bool `gom:"active"`
}
role := &UserRole{Role: "admin"}
status := &UserStatus{Active: true}
result, err = db.Chain().
    Table("users").
    Eq("id", 1).
    Update(role, status)

// 删除多个对象（自动使用事务）
result, err = db.Chain().
    Table("users").
    Delete(user1, user2)
```

### v4.0.9-ai (2024-01-02 22:00 UTC+8)

新特性：
- 增强 `Update` 方法
  - 支持传入不定长结构体参数
  - 支持不同类型的结构体更新
  - 每个结构体独立执行更新
  - 自动解析非空字段
  - 自动排除主键字段

使用示例：
```go
// 使用完整结构体更新
updateUser := &User{
    Username: "new_name",
    Email:    "new@example.com",
}
result1, err := db.Chain().
    Table("users").
    Eq("id", 1).
    Update(updateUser)

// 使用不同类型的结构体更新
type UserRole struct {
    Role string `gom:"role"`
}
type UserStatus struct {
    Active    bool      `gom:"active"`
    UpdatedAt time.Time `gom:"updated_at"`
}

// 分别更新角色和状态
updateRole := &UserRole{Role: "admin"}
updateStatus := &UserStatus{
    Active:    true,
    UpdatedAt: time.Now(),
}
result2, err := db.Chain().
    Table("users").
    Eq("id", 1).
    Update(updateRole, updateStatus)
```

### v4.0.8-ai (2024-01-02 21:50 UTC+8)

新特性：
- 添加 `GetTableName` 方法
  - 支持从结构体获取表名
  - 支持自定义表名接口
  - 自动处理命名转换（驼峰转蛇形）
  - 严格的类型检查
  - 返回错误信息

更新：
- 更新 MySQL 驱动到 v1.8.1
- 移除 lib/pq 依赖，使用 pgx 作为 PostgreSQL 驱动
- 优化代码生成器的数据库连接

### v4.0.6-ai (2024-01-02 21:35 UTC+8)

新特性：
- 添加分页查询方法
  - `PageInfo(model)`: 支持模型和原始数据的分页查询
  - 提供完整的分页信息（总数、页数、导航等）
- 支持与现有查询方法完美集成
- 支持自动处理默认值和边界情况

### v4.0.5-ai (2024-01-02 21:27 UTC+8)

新特性：
- 添加统计相关方法
  - `Count()`: 计算记录总数
  - `Sum(field)`: 计算字段总和
  - `Avg(field)`: 计算字段平均值
- 所有统计方法支持条件过滤
- 优化了 NULL 值和空结果集的处理

