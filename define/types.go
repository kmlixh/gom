package define

import (
	"database/sql"
	"time"
)

// IsolationLevel represents the transaction isolation level
type IsolationLevel sql.IsolationLevel

const (
	// LevelDefault is the default isolation level for the database
	LevelDefault IsolationLevel = IsolationLevel(sql.LevelDefault)
	// LevelReadUncommitted is the read uncommitted isolation level
	LevelReadUncommitted IsolationLevel = IsolationLevel(sql.LevelReadUncommitted)
	// LevelReadCommitted is the read committed isolation level
	LevelReadCommitted IsolationLevel = IsolationLevel(sql.LevelReadCommitted)
	// LevelWriteCommitted is the write committed isolation level
	LevelWriteCommitted IsolationLevel = IsolationLevel(sql.LevelWriteCommitted)
	// LevelRepeatableRead is the repeatable read isolation level
	LevelRepeatableRead IsolationLevel = IsolationLevel(sql.LevelRepeatableRead)
	// LevelSnapshot is the snapshot isolation level
	LevelSnapshot IsolationLevel = IsolationLevel(sql.LevelSnapshot)
	// LevelSerializable is the serializable isolation level
	LevelSerializable IsolationLevel = IsolationLevel(sql.LevelSerializable)
	// LevelLinearizable is the linearizable isolation level
	LevelLinearizable IsolationLevel = IsolationLevel(sql.LevelLinearizable)
)

// TransactionPropagation defines transaction propagation behavior
type TransactionPropagation int

const (
	// PropagationRequired starts a new transaction if none exists
	PropagationRequired TransactionPropagation = iota
	// PropagationRequiresNew always starts a new transaction
	PropagationRequiresNew
	// PropagationNested starts a nested transaction if possible
	PropagationNested
	// PropagationSupports uses existing transaction if available
	PropagationSupports
	// PropagationNotSupported suspends current transaction if exists
	PropagationNotSupported
	// PropagationNever throws exception if transaction exists
	PropagationNever
	// PropagationMandatory throws exception if no transaction exists
	PropagationMandatory
)

// TransactionOptions represents options for transaction
type TransactionOptions struct {
	Timeout         time.Duration
	IsolationLevel  IsolationLevel
	PropagationMode TransactionPropagation
	ReadOnly        bool
}

// JoinType represents the type of JOIN operation
type JoinType int

const (
	// JoinAnd represents AND join
	JoinAnd JoinType = iota
	// JoinOr represents OR join
	JoinOr
)

// LockType represents the type of row locking
type LockType int

const (
	// LockNone represents no locking
	LockNone LockType = iota
	// LockForUpdate represents FOR UPDATE lock
	LockForUpdate
	// LockForShare represents FOR SHARE lock
	LockForShare
)

// OpType represents the type of operation
type OpType int

const (
	// OpEq represents equals operation
	OpEq OpType = iota
	// OpNe represents not equals operation
	OpNe
	// OpGt represents greater than operation
	OpGt
	// OpGe represents greater than or equals operation
	OpGe
	// OpLt represents less than operation
	OpLt
	// OpLe represents less than or equals operation
	OpLe
	// OpLike represents LIKE operation
	OpLike
	// OpNotLike represents NOT LIKE operation
	OpNotLike
	// OpIn represents IN operation
	OpIn
	// OpNotIn represents NOT IN operation
	OpNotIn
	// OpIsNull represents IS NULL operation
	OpIsNull
	// OpIsNotNull represents IS NOT NULL operation
	OpIsNotNull
	// OpBetween represents BETWEEN operation
	OpBetween
	// OpNotBetween represents NOT BETWEEN operation
	OpNotBetween
	// OpCustom represents custom operation
	OpCustom
)

// SQLQuery represents a SQL query with its arguments
type SQLQuery struct {
	Query string
	Args  []interface{}
}
