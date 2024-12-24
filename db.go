package gom

import (
	"fmt"

	"github.com/kmlixh/gom/define"
)

// Connect creates a new DB instance with the specified driver and DSN
func Connect(driverName, dsn string) (*define.DB, error) {
	factory, ok := define.GetFactory(driverName)
	if !ok {
		return nil, fmt.Errorf("no SQL factory registered for driver: %s", driverName)
	}
	return factory.Connect(dsn)
}
