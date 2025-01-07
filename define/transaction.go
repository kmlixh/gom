package define

// TransactionFunc represents a function that executes within a transaction
type TransactionFunc func(tx *Transaction) Result

// Transaction represents a database transaction
type Transaction struct {
	db     interface{}
	tx     interface{}
	driver string
}

// NewTransaction creates a new transaction
func NewTransaction(db interface{}, tx interface{}, driver string) *Transaction {
	return &Transaction{
		db:     db,
		tx:     tx,
		driver: driver,
	}
}

// GetDB returns the database instance
func (t *Transaction) GetDB() interface{} {
	return t.db
}

// GetTx returns the transaction instance
func (t *Transaction) GetTx() interface{} {
	return t.tx
}

// GetDriver returns the database driver name
func (t *Transaction) GetDriver() string {
	return t.driver
}
