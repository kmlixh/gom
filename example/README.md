# GOM 示例

本目录包含了GOM (Go ORM)的使用示例，展示了如何在MySQL和PostgreSQL数据库中使用GOM进行常见的数据库操作。

## 目录结构

```
example/
├── mysql/      # MySQL示例
├── postgres/   # PostgreSQL示例
└── README.md   # 本文件
```

## 准备工作

1. 创建测试数据库和表：

### MySQL

```sql
CREATE DATABASE test;
USE test;

CREATE TABLE users (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSON
);
```

### PostgreSQL

```sql
CREATE DATABASE test;

CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data JSONB
);
```

2. 安装依赖：

```bash
go get github.com/go-sql-driver/mysql  # MySQL驱动
go get github.com/lib/pq               # PostgreSQL驱动
```

## 示例说明

这些示例展示了GOM的主要功能：

1. 基本CRUD操作
   - 插���数据（单条和批量）
   - 查询数据（单条、多条、条件查询）
   - 更新数据（单条和批量）
   - 删除数据（单条和条件删除）

2. 高级功能
   - 事务处理
   - 分页查询
   - 复杂条件查询
   - 批量操作
   - 模型映射

3. 数据库特定功能
   - MySQL: AUTO_INCREMENT, ON DUPLICATE KEY UPDATE
   - PostgreSQL: RETURNING, ILIKE, JSON操作, 窗口函数

## 运行示例

### MySQL示例

```bash
cd mysql
go run main.go
```

### PostgreSQL示例

```bash
cd postgres
go run main.go
```

## 注意事项

1. 运行示例前请确保：
   - 数据库服务已启动
   - 已创建测试数据库和表
   - 已正确配置数据库连接参数

2. 示例中的数据库连接参数需要根据实际环境修改：
   - 主机名
   - 端口
   - 用户名
   - 密码
   - 数据库名

3. 示例代码主要用于演示目的，生产环境中请：
   - 使用环境变量或配置文件管理数据库连接信息
   - 添加适当的错误处理
   - 实现完整的日志记录
   - 注意SQL注入防护
   - 优化查询性能

## 更多信息

- [GOM文档](https://github.com/yourusername/gom)
- [MySQL文档](https://dev.mysql.com/doc/)
- [PostgreSQL文档](https://www.postgresql.org/docs/) 