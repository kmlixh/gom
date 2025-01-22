package define

import (
	"testing"
)

func TestNewTransaction(t *testing.T) {
	db := "test_db"
	tx := "test_tx"
	driver := "mysql"

	transaction := NewTransaction(db, tx, driver)

	if transaction == nil {
		t.Error("NewTransaction() returned nil")
	}

	if transaction.GetDB() != db {
		t.Errorf("GetDB() = %v, want %v", transaction.GetDB(), db)
	}

	if transaction.GetTx() != tx {
		t.Errorf("GetTx() = %v, want %v", transaction.GetTx(), tx)
	}

	if transaction.GetDriver() != driver {
		t.Errorf("GetDriver() = %v, want %v", transaction.GetDriver(), driver)
	}
}

func TestTransactionGetters(t *testing.T) {
	tests := []struct {
		name   string
		db     interface{}
		tx     interface{}
		driver string
	}{
		{
			name:   "string values",
			db:     "test_db",
			tx:     "test_tx",
			driver: "mysql",
		},
		{
			name:   "nil values",
			db:     nil,
			tx:     nil,
			driver: "",
		},
		{
			name:   "integer values",
			db:     123,
			tx:     456,
			driver: "postgres",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transaction := NewTransaction(tt.db, tt.tx, tt.driver)

			if transaction.GetDB() != tt.db {
				t.Errorf("GetDB() = %v, want %v", transaction.GetDB(), tt.db)
			}

			if transaction.GetTx() != tt.tx {
				t.Errorf("GetTx() = %v, want %v", transaction.GetTx(), tt.tx)
			}

			if transaction.GetDriver() != tt.driver {
				t.Errorf("GetDriver() = %v, want %v", transaction.GetDriver(), tt.driver)
			}
		})
	}
}

func TestTransactionFunc(t *testing.T) {
	transaction := NewTransaction("test_db", "test_tx", "mysql")

	var called bool
	expectedResult := Result{
		ID:       1,
		Affected: 1,
		Error:    nil,
		Data:     []map[string]any{{"id": 1}},
		Columns:  []string{"id"},
	}

	fn := TransactionFunc(func(tx *Transaction) Result {
		called = true
		if tx != transaction {
			t.Error("Transaction passed to function is not the same as the one created")
		}
		return expectedResult
	})

	result := fn(transaction)

	if !called {
		t.Error("TransactionFunc was not called")
	}

	// Test Result interface methods
	id, err := result.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId() error = %v", err)
	}
	if id != 1 {
		t.Errorf("LastInsertId() = %v, want %v", id, 1)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected() error = %v", err)
	}
	if affected != 1 {
		t.Errorf("RowsAffected() = %v, want %v", affected, 1)
	}

	// Test additional Result methods
	if result.Empty() {
		t.Error("Empty() = true, want false")
	}

	if result.Size() != 1 {
		t.Errorf("Size() = %v, want %v", result.Size(), 1)
	}
}
