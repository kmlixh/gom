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

## 版本历史

### v4.0.5 (2024-12-27 13:35 UTC+8)

新特性：
- 添加统计相关方法
  - `Count()`: 计算记录总数
  - `Sum(field)`: 计算字段总和
  - `Avg(field)`: 计算字段平均值
- 所有统计方法支持条件过滤
- 优化了 NULL 值和空结果集的处理

