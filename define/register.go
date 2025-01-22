package define

import (
	"fmt"
	"sync"
)

var (
	factoryMutex sync.RWMutex
	factories    = make(map[string]SQLFactory)
)

// RegisterFactory registers a SQL factory for a specific driver
func RegisterFactory(driver string, factory SQLFactory) {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	factories[driver] = factory
}

// GetFactory returns the SQL factory for a specific driver
func GetFactory(driver string) (SQLFactory, error) {
	factoryMutex.RLock()
	defer factoryMutex.RUnlock()
	if factory, ok := factories[driver]; ok {
		return factory, nil
	}
	return nil, fmt.Errorf("no factory registered for driver: %s", driver)
}

// UnregisterFactory removes a SQL factory for a specific driver
func UnregisterFactory(driver string) {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	delete(factories, driver)
}
