# GOM - Go ORM Made Simple

GOM 是一个简单、灵活且功能强大的 Go 语言 ORM 框架，支持 MySQL 和 PostgreSQL 数据库。

## 特性

- 支持 MySQL 和 PostgreSQL
- 链式操作 API
- 自动生成结构体代码
- 完整的事务支持
- 丰富的查询功能
- 支持数据库特有功能
- 类型安全的查询构建器
- 自动处理时间戳
- 支持多种数据类型

## 安装

```bash
go get github.com/kmlixh/gom/v4
```

## 快速开始

### 连接数据库

```go
// MySQL
db, err := gom.Open("mysql", "user:password@tcp(localhost:3306)/dbname?parseTime=true", true)

// PostgreSQL
db, err := gom.Open("postgres", "postgres://user:password@localhost:5432/dbname?sslmode=disable", true)

if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### 定义模型

```go
type User struct {
    ID        int64     `gom:"id,@,auto"`        // 主键，自增
    Username  string    `gom:"username,notnull"`  // 非空字段
    Email     string    `gom:"email,notnull"`     // 非空字段
    Age       int       `gom:"age"`
    Active    bool      `gom:"active"`
    CreatedAt time.Time `gom:"created_at"`
    UpdatedAt time.Time `gom:"updated_at"`
}

// 可选：自定义表名
func (u *User) TableName() string {
    return "users"
}
```

### 基本操作

#### 插入数据

```go
user := &User{
    Username: "test_user",
    Email:    "test@example.com",
    Age:      25,
    Active:   true,
}

// 插入单条记录
result, err := db.Chain().From(user).Save()

// 批量插入
users := []map[string]interface{}{
    {
        "username": "user1",
        "email":    "user1@example.com",
        "age":      25,
    },
    {
        "username": "user2",
        "email":    "user2@example.com",
        "age":      30,
    },
}
result, err := db.Chain().From("users").BatchValues(users).Save()
```

#### 查询数据

```go
// 简单查询
var users []User
err := db.Chain().From("users").
    Where("age", ">", 18).
    OrderBy("age").
    Limit(10).
    List().
    Into(&users)

// 复杂查询
var results []User
err := db.Chain().From("users").
    Where("age", ">=", 25).
    And("age", "<=", 35).
    And("active", "=", true).
    OrderByDesc("age").
    OrderBy("username").
    Page(1, 10).
    List().
    Into(&results)
```

#### 更新数据

```go
// 更新单个字段
_, err := db.Chain().From("users").
    Set("age", 26).
    Where("id", "=", 1).
    Update()

// 更新多个字段
_, err := db.Chain().From("users").
    Set("age", 26).
    Set("active", false).
    Where("id", "=", 1).
    Update()
```

#### 删除数据

```go
_, err := db.Chain().From("users").
    Where("id", "=", 1).
    Delete()
```

### 事务支持

```go
err := db.Chain().Transaction(func(chain *gom.Chain) error {
    // 在事务中执行操作
    _, err := chain.From("users").
        Set("age", 26).
        Where("id", "=", 1).
        Update()
    if err != nil {
        return err // 返回错误会自动回滚
    }

    // 更多操作...
    return nil // 返回 nil 会自动提交
})
```

### PostgreSQL 特有功能

#### RETURNING 子句

```go
type ReturnResult struct {
    ID        int64     `gom:"id"`
    CreatedAt time.Time `gom:"created_at"`
}

var results []ReturnResult
err := db.Chain().RawQuery(`
    INSERT INTO users (username, email)
    VALUES ($1, $2)
    RETURNING id, created_at
`, "test_user", "test@example.com").Into(&results)
```

#### JSONB 操作

```go
// 创建带 JSONB 列的表
_, err := db.Chain().RawExecute(`
    CREATE TABLE user_settings (
        id SERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(id),
        preferences JSONB
    )
`)

// 查询 JSONB 数据
var results []UserTheme
err := db.Chain().RawQuery(`
    SELECT u.username, s.preferences->>'theme' as theme
    FROM users u
    JOIN user_settings s ON u.id = s.user_id
    WHERE s.preferences @> '{"notifications": true}'::jsonb
`).Into(&results)
```

#### 全文搜索

```go
// 添加全文搜索列
_, err := db.Chain().RawExecute(`
    ALTER TABLE users ADD COLUMN search_vector tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(username,'')), 'A') ||
        setweight(to_tsvector('english', coalesce(email,'')), 'B')
    ) STORED
`)

// 创建全文搜索索引
_, err = db.Chain().RawExecute(`
    CREATE INDEX users_search_idx ON users USING GIN (search_vector)
`)

// 执行全文搜索
var results []SearchResult
err = db.Chain().RawQuery(`
    SELECT username, email, ts_rank(search_vector, query) as rank
    FROM users, plainto_tsquery('english', $1) query
    WHERE search_vector @@ query
    ORDER BY rank DESC
`, "search_term").Into(&results)
```

#### 递归查询

```go
var results []CategoryTree
err := db.Chain().RawQuery(`
    WITH RECURSIVE category_tree AS (
        SELECT id, name, parent_id, 1 as level, name::text as path
        FROM categories
        WHERE parent_id IS NULL
        
        UNION ALL
        
        SELECT c.id, c.name, c.parent_id, ct.level + 1,
            (ct.path || ' > ' || c.name::text)
        FROM categories c
        JOIN category_tree ct ON ct.id = c.parent_id
    )
    SELECT path, level
    FROM category_tree
    ORDER BY path
`).Into(&results)
```

### 代码生成

GOM 提供了强大的代码生成功能，可以从数据库表自动生成 Go 结构体。

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
```

#### 在代码中使用

```go
// 生成单个表的结构体
err := db.GenerateStruct("users", "./models", "models")

// 生成多个表的结构体
err := db.GenerateStructs(gom.GenerateOptions{
    OutputDir:   "./models",
    PackageName: "models",
    Pattern:     "user*", // 支持通配符
})
```

生成的结构体将包含：
- 表字段映射到 Go 类型
- gom 标签（包含字段名、主键、自增、非空等信息）
- 字段注释（如果数据库中有定义）
- TableName() 方法

### 支持的数据类型

#### MySQL
- 整数类型：TINYINT, SMALLINT, INT, BIGINT
- 浮点类型：FLOAT, DOUBLE, DECIMAL
- 字符串类型：CHAR, VARCHAR, TEXT
- 时间类型：DATE, TIME, DATETIME, TIMESTAMP
- 布尔类型：BOOLEAN
- 二进制类型：BLOB, BINARY, VARBINARY

#### PostgreSQL
- 整数类型：SMALLINT, INTEGER, BIGINT
- 浮点类型：REAL, DOUBLE PRECISION, DECIMAL/NUMERIC
- 字符串类型：CHAR, VARCHAR, TEXT
- 时间类型：DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL
- 布尔类型：BOOLEAN
- JSON 类型：JSON, JSONB
- 数组类型：INTEGER[], TEXT[] 等
- 网络类型：INET, CIDR
- UUID 类型
- 几何类型：POINT, LINE, LSEG, BOX 等

## 示例代码

完整的示例代码可以在 `example` 目录下找到：
- `example/mysql/main.go`: MySQL 示例
- `example/postgres/main.go`: PostgreSQL 示例

## 贡献

欢迎提交 Pull Request 和 Issue！

## 许可证

Apache License 2.0

