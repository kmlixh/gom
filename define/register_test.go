package define

import (
	"fmt"
	"testing"
)

func TestRegisterFactory(t *testing.T) {
	// Clear any existing factories
	factories = make(map[string]SQLFactory)

	mockFactory := &MockSQLFactory{}
	driver := "mock_driver"

	// Test registration
	RegisterFactory(driver, mockFactory)

	// Verify registration
	if len(factories) != 1 {
		t.Errorf("Expected 1 factory, got %d", len(factories))
	}

	if _, ok := factories[driver].(*MockSQLFactory); !ok {
		t.Errorf("Factory not properly registered for driver %s", driver)
	}

	// Test overwriting existing factory
	newMockFactory := &MockSQLFactory{}
	RegisterFactory(driver, newMockFactory)

	if registeredFactory, ok := factories[driver].(*MockSQLFactory); !ok || registeredFactory != newMockFactory {
		t.Error("Factory not properly overwritten")
	}
}

func TestGetFactory(t *testing.T) {
	// Clear any existing factories
	factories = make(map[string]SQLFactory)

	mockFactory := &MockSQLFactory{}
	driver := "mock_driver"

	// Test getting non-existent factory
	_, err := GetFactory(driver)
	if err == nil {
		t.Error("Expected error when getting non-existent factory")
	}

	// Register factory
	RegisterFactory(driver, mockFactory)

	// Test getting existing factory
	factory, err := GetFactory(driver)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if _, ok := factory.(*MockSQLFactory); !ok {
		t.Error("Retrieved factory does not match registered factory")
	}
}

func TestUnregisterFactory(t *testing.T) {
	// Clear any existing factories
	factories = make(map[string]SQLFactory)

	mockFactory := &MockSQLFactory{}
	driver := "mock_driver"

	// Register factory
	RegisterFactory(driver, mockFactory)

	// Test unregistration
	UnregisterFactory(driver)

	if len(factories) != 0 {
		t.Errorf("Expected 0 factories after unregistration, got %d", len(factories))
	}

	// Verify factory is removed
	_, err := GetFactory(driver)
	if err == nil {
		t.Error("Expected error when getting unregistered factory")
	}

	// Test unregistering non-existent factory (should not panic)
	UnregisterFactory("non_existent_driver")
}

func TestConcurrentFactoryOperations(t *testing.T) {
	// Clear any existing factories
	factories = make(map[string]SQLFactory)

	const numGoroutines = 10
	done := make(chan bool)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			driver := fmt.Sprintf("mock_driver_%d", id)
			mockFactory := &MockSQLFactory{}

			// Register
			RegisterFactory(driver, mockFactory)

			// Get
			factory, err := GetFactory(driver)
			if err != nil {
				t.Errorf("Error getting factory: %v", err)
			}
			if _, ok := factory.(*MockSQLFactory); !ok {
				t.Error("Retrieved factory does not match registered factory")
			}

			// Unregister
			UnregisterFactory(driver)

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify final state
	if len(factories) != 0 {
		t.Errorf("Expected 0 factories after all operations, got %d", len(factories))
	}
}
