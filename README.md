# GOM - Go ORM Made by Cursor

[English](#english) | [中文](#chinese)

<a name="english"></a>
## English

GOM is a lightweight and flexible ORM framework for Go, developed with guidance from Cursor - an AI-powered IDE. It provides an intuitive chain-style API for database operations and supports both MySQL and PostgreSQL.

### Features

- Chain-style API for fluent database operations
- Support for MySQL and PostgreSQL
- Automatic table creation from struct definitions
- Custom table name and creation SQL through interfaces
- Flexible query building with struct tags
- Support for complex queries and relationships
- Built-in pagination
- Raw SQL execution capability
- Debug mode for SQL logging

### Installation

```bash
go get github.com/kmlixh/gom/v4
```

### Quick Start

1. Define your model:

```go
type User struct {
    ID        int64     `gom:"id,@"`              // @ means auto-increment primary key
    Username  string    `gom:"username,unique,notnull"`
    Email     string    `gom:"email,unique,notnull"`
    Age       int       `gom:"age,notnull,default:18"`
    Active    bool      `gom:"active,notnull,default:true"`
    CreatedAt time.Time `gom:"created_at"`
    UpdatedAt time.Time `gom:"updated_at"`
}
```

2. Connect to database:

```go
db, err := gom.Open("postgres", "postgres://user:pass@localhost:5432/dbname?sslmode=disable", true)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

3. Create table:

```go
err = db.Chain().CreateTable(&User{})
if err != nil {
    log.Fatal(err)
}
```

4. Basic CRUD operations:

```go
// Using struct
user := &User{
    Username: "john_doe",
    Email:    "john@example.com",
    Age:      30,
    Active:   true,
}
result, err := db.Chain().From(user).Save()

// Using table name directly
result, err := db.Chain().From("users").Where("age", ">", 25).List()

// Query
var users []User
queryResult, err := db.Chain().From(&User{}).Where("age", ">", 25).List()
err = queryResult.Into(&users)

// Update
_, err = db.Chain().From(&User{}).Set("age", 31).Where("id", "=", 1).Save()

// Delete
_, err = db.Chain().From(&User{}).Where("id", "=", 1).Delete()
```

### Advanced Features

1. Custom Table Model:

```go
type CustomUser struct {
    ID       int64  `gom:"id,primaryAuto"`
    Username string `gom:"username,notnull"`
}

func (u *CustomUser) TableName() string {
    return "custom_users"
}

func (u *CustomUser) CreateSql() string {
    return `CREATE TABLE IF NOT EXISTS custom_users (...)`
}
```

2. Complex Queries:

```go
type UserQuery struct {
    MinAge   *int  `gom:"min_age"`
    MaxAge   *int  `gom:"max_age"`
    IsActive *bool `gom:"is_active"`
}

queryModel := &UserQuery{
    MinAge:   &minAge,
    IsActive: &isActive,
}
result, err := db.Chain().From(queryModel).List()
```

3. Pagination:

```go
result, err := db.Chain().From(&User{}).Page(1, 10).List()
```

4. Raw SQL:

```go
result, err := db.Chain().RawQuery("SELECT * FROM users WHERE age > $1", 25)
```

5. Transaction Support:

```go
// Method 1: Using Transaction callback (Recommended)
err := db.Chain().Transaction(func(chain *Chain) error {
    // Create user
    user := &User{
        Username: "john_doe",
        Email:    "john@example.com",
    }
    _, err := chain.From(user).Save()
    if err != nil {
        return err // Will automatically rollback
    }

    // Create profile
    profile := &UserProfile{
        UserID: user.ID,
        Bio:    "Software Engineer",
    }
    _, err = chain.From(profile).Save()
    if err != nil {
        return err // Will automatically rollback
    }

    return nil // Will automatically commit
})

// Method 2: Manual transaction control
chain := db.Chain()
err := chain.Begin()
if err != nil {
    log.Fatal(err)
}

// Perform operations within transaction
_, err = chain.From(user).Save()
if err != nil {
    chain.Rollback()
    return err
}

_, err = chain.From(profile).Save()
if err != nil {
    chain.Rollback()
    return err
}

// Commit the transaction
err = chain.Commit()
```

### Contributing

This project was developed with guidance from Cursor, an AI-powered IDE. Contributions are welcome!

### License

MIT License

### 代码生成器

GOM 提供了一个代码生成器，可以从数据库表自动生成 Go 结构体。

#### 命令行使用

```bash
# 安装生成器
go install github.com/kmlixh/gom/v4/generator/cmd@latest

# MySQL 示例
generator -type mysql \
  -url "user:password@tcp(localhost:3306)/dbname" \
  -out ./models \
  -package models

# PostgreSQL 示例
generator -type postgres \
  -url "postgres://user:password@localhost:5432/dbname?sslmode=disable" \
  -out ./models \
  -package models \
  -schema public

# 生成指定表的结构体
generator -type mysql \
  -url "user:password@tcp(localhost:3306)/dbname" \
  -table user_info \
  -out ./models \
  -package models
```

#### 参数说明

- `-type`: 数据库类型 (mysql 或 postgres)
- `-url`: 数据库连接URL
- `-out`: 输出目录 (默认: models)
- `-package`: 生成的Go包名 (默认: models)
- `-table`: 指定要生成的表名 (可选)
- `-schema`: 指定schema名称 (PostgreSQL专用，可选)

#### 在代码中使用

```go
import "github.com/kmlixh/gom/v4/generator"

config := generator.Config{
    OutputDir:   "./models",
    PackageName: "models",
    DBType:     "mysql",
    DB:         db, // *sql.DB 实例
    TableName:  "user_info", // 可选
}

if err := generator.NewGenerator(config).Generate(); err != nil {
    log.Fatal(err)
}
```

生成的结构体将包含：
- 表字段映射到 Go 类型
- gom 标签（包含字段名、主键、自增、非空等信息）
- 字段注释（如果数据库中有定义）
- TableName() 方法

---

<a name="chinese"></a>
## 中文

GOM 是一个轻量级且灵活的 Go ORM 框架，在 Cursor（一个 AI 驱动的 IDE）的指导下开发。它为数据库操作提供了直观的链式 API，并支持 MySQL 和 PostgreSQL。

### 特性

- 链式 API，流畅的数据库操作
- 支持 MySQL 和 PostgreSQL
- 从结构体定义自动创建表
- 通过接口自定义表名和创建 SQL
- 使用结构体标签灵活构建查询
- 支持复杂查询和关系
- 内置分页功能
- 原生 SQL 执行能力
- 调试模式下的 SQL 日志

### 安装

```bash
go get github.com/kmlixh/gom/v4
```

### 快速开始

1. 定义模型：

```go
type User struct {
    ID        int64     `gom:"id,@"`              // @ 表示自增主键
    Username  string    `gom:"username,unique,notnull"`
    Email     string    `gom:"email,unique,notnull"`
    Age       int       `gom:"age,notnull,default:18"`
    Active    bool      `gom:"active,notnull,default:true"`
    CreatedAt time.Time `gom:"created_at"`
    UpdatedAt time.Time `gom:"updated_at"`
}
```

2. 连接数据库：

```go
db, err := gom.Open("postgres", "postgres://user:pass@localhost:5432/dbname?sslmode=disable", true)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

3. 创建表：

```go
err = db.Chain().CreateTable(&User{})
if err != nil {
    log.Fatal(err)
}
```

4. 基本的 CRUD 操作：

```go
// 使用结构体
user := &User{
    Username: "john_doe",
    Email:    "john@example.com",
    Age:      30,
    Active:   true,
}
result, err := db.Chain().From(user).Save()

// 直接使用表名
result, err := db.Chain().From("users").Where("age", ">", 25).List()

// 查询
var users []User
queryResult, err := db.Chain().From(&User{}).Where("age", ">", 25).List()
err = queryResult.Into(&users)

// 更新
_, err = db.Chain().From(&User{}).Set("age", 31).Where("id", "=", 1).Save()

// 删除
_, err = db.Chain().From(&User{}).Where("id", "=", 1).Delete()
```

### 高级特性

1. 自定义表模型：

```go
type CustomUser struct {
    ID       int64  `gom:"id,primaryAuto"`
    Username string `gom:"username,notnull"`
}

func (u *CustomUser) TableName() string {
    return "custom_users"
}

func (u *CustomUser) CreateSql() string {
    return `CREATE TABLE IF NOT EXISTS custom_users (...)`
}
```

2. 复杂查询：

```go
type UserQuery struct {
    MinAge   *int  `gom:"min_age"`
    MaxAge   *int  `gom:"max_age"`
    IsActive *bool `gom:"is_active"`
}

queryModel := &UserQuery{
    MinAge:   &minAge,
    IsActive: &isActive,
}
result, err := db.Chain().From(queryModel).List()
```

3. 分页：

```go
result, err := db.Chain().From(&User{}).Page(1, 10).List()
```

4. 原生 SQL：

```go
result, err := db.Chain().RawQuery("SELECT * FROM users WHERE age > $1", 25)
```

5. 事务支持：

```go
// Method 1: Using Transaction callback (Recommended)
err := db.Chain().Transaction(func(chain *Chain) error {
    // Create user
    user := &User{
        Username: "john_doe",
        Email:    "john@example.com",
    }
    _, err := chain.From(user).Save()
    if err != nil {
        return err // Will automatically rollback
    }

    // Create profile
    profile := &UserProfile{
        UserID: user.ID,
        Bio:    "Software Engineer",
    }
    _, err = chain.From(profile).Save()
    if err != nil {
        return err // Will automatically rollback
    }

    return nil // Will automatically commit
})

// Method 2: Manual transaction control
chain := db.Chain()
err := chain.Begin()
if err != nil {
    log.Fatal(err)
}

// Perform operations within transaction
_, err = chain.From(user).Save()
if err != nil {
    chain.Rollback()
    return err
}

_, err = chain.From(profile).Save()
if err != nil {
    chain.Rollback()
    return err
}

// Commit the transaction
err = chain.Commit()
```

### 贡献

本项目在 Cursor（一个 AI 驱动的 IDE）的指导下开发。欢迎贡献！

### 许可证

Apache License 2.0

### 代码生成器

GOM 提供了一个代码生成器，可以从数据库表自动生成 Go 结构体。

#### 命令行使用

```bash
# 安装生成器
go install github.com/kmlixh/gom/v4/generator/cmd@latest

# MySQL 示例
generator -type mysql \
  -url "user:password@tcp(localhost:3306)/dbname" \
  -out ./models \
  -package models

# PostgreSQL 示例
generator -type postgres \
  -url "postgres://user:password@localhost:5432/dbname?sslmode=disable" \
  -out ./models \
  -package models \
  -schema public

# 生成指定表的结构体
generator -type mysql \
  -url "user:password@tcp(localhost:3306)/dbname" \
  -table user_info \
  -out ./models \
  -package models
```

#### 参数说明

- `-type`: 数据库类型 (mysql 或 postgres)
- `-url`: 数据库连接URL
- `-out`: 输出目录 (默认: models)
- `-package`: 生成的Go包名 (默认: models)
- `-table`: 指定要生成的表名 (可选)
- `-schema`: 指定schema名称 (PostgreSQL专用，可选)

#### 在代码中使用

```go
import "github.com/kmlixh/gom/v4/generator"

config := generator.Config{
    OutputDir:   "./models",
    PackageName: "models",
    DBType:     "mysql",
    DB:         db, // *sql.DB 实例
    TableName:  "user_info", // 可选
}

if err := generator.NewGenerator(config).Generate(); err != nil {
    log.Fatal(err)
}
```

生成的结构体将包含：
- 表字段映射到 Go 类型
- gom 标签（包含字段名、主键、自增、非空等信息）
- 字段注释（如果数据库中有定义）
- TableName() 方法

