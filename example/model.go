package example

import "time"

// User represents a user model
type User struct {
	ID        int64     `gom:"id,@"`
	Username  string    `gom:"username,unique,notnull"`
	Email     string    `gom:"email,unique,notnull"`
	Age       int       `gom:"age,notnull,default:18"`
	Active    bool      `gom:"active,notnull,default:true"`
	Role      string    `gom:"role,notnull,default:'user'"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at"`
}

// UserRole represents a role model to demonstrate foreign key
type UserRole struct {
	ID          int64     `gom:"id,@"`
	Name        string    `gom:"name,unique,notnull"`
	Description string    `gom:"description,notnull"`
	CreatedAt   time.Time `gom:"created_at"`
	UpdatedAt   time.Time `gom:"updated_at"`
}

// UserProfile demonstrates more complex relationships
type UserProfile struct {
	ID        int64     `gom:"id,@"`
	UserID    int64     `gom:"user_id,notnull,foreignkey:users.id"`
	Avatar    string    `gom:"avatar,notnull,default:'/default.png'"`
	Bio       string    `gom:"bio"`
	Location  string    `gom:"location"`
	CreatedAt time.Time `gom:"created_at"`
	UpdatedAt time.Time `gom:"updated_at"`
}

// UserQuery represents a query model for users
type UserQuery struct {
	MinAge    *int       `gom:"min_age"`
	MaxAge    *int       `gom:"max_age"`
	Username  string     `gom:"username"`
	Email     string     `gom:"email"`
	IsActive  *bool      `gom:"is_active"`
	CreatedAt *time.Time `gom:"created_at"`
}

// CreateUserTableSQL returns the SQL for creating the users table
func CreateUserTableSQL(driver string) string {
	switch driver {
	case "mysql":
		return `CREATE TABLE IF NOT EXISTS users (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			username VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			age INT NOT NULL,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY idx_username (username),
			UNIQUE KEY idx_email (email)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	case "postgres":
		return `CREATE TABLE IF NOT EXISTS users (
			id BIGSERIAL PRIMARY KEY,
			username VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			age INT NOT NULL,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT idx_username UNIQUE (username),
			CONSTRAINT idx_email UNIQUE (email)
		);
		
		CREATE OR REPLACE FUNCTION update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = CURRENT_TIMESTAMP;
			RETURN NEW;
		END;
		$$ language 'plpgsql';
		
		DROP TRIGGER IF EXISTS update_users_updated_at ON users;
		
		CREATE TRIGGER update_users_updated_at
			BEFORE UPDATE ON users
			FOR EACH ROW
			EXECUTE FUNCTION update_updated_at_column();`
	default:
		return ""
	}
}

// CustomUser implements ITableModel interface
type CustomUser struct {
	ID        int64     `gom:"id,primaryAuto"`
	Username  string    `gom:"username,notnull"`
	Email     string    `gom:"email,notnull"`
	Age       int       `gom:"age,notnull"`
	Active    bool      `gom:"active,notnull"`
	Role      string    `gom:"role,notnull"`
	CreatedAt time.Time `gom:"created_at,notnull"`
	UpdatedAt time.Time `gom:"updated_at,notnull"`
}

// TableName returns a custom table name
func (u *CustomUser) TableName() string {
	return "custom_users"
}

// CreateSql returns a custom CREATE TABLE SQL statement
func (u *CustomUser) CreateSql() string {
	return `CREATE TABLE IF NOT EXISTS custom_users (
		id BIGSERIAL PRIMARY KEY,
		username VARCHAR(255) NOT NULL UNIQUE,
		email VARCHAR(255) NOT NULL UNIQUE,
		age INTEGER NOT NULL DEFAULT 18,
		active BOOLEAN NOT NULL DEFAULT true,
		role VARCHAR(255) NOT NULL DEFAULT 'user',
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		CONSTRAINT uq_custom_users_username UNIQUE (username),
		CONSTRAINT uq_custom_users_email UNIQUE (email)
	);

	CREATE OR REPLACE FUNCTION update_custom_users_updated_at()
	RETURNS TRIGGER AS $$
	BEGIN
		NEW.updated_at = CURRENT_TIMESTAMP;
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;

	DROP TRIGGER IF EXISTS update_custom_users_updated_at ON custom_users;

	CREATE TRIGGER update_custom_users_updated_at
		BEFORE UPDATE ON custom_users
		FOR EACH ROW
		EXECUTE FUNCTION update_custom_users_updated_at();`
}
