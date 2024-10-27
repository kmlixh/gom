package gom

import (
	"database/sql"
	"errors"
	"fmt"
	factory2 "github.com/kmlixh/gom/v3/factory"
	"time"
)

func OpenWithConfigV4(driverName string, dsn string, maxOpen int, maxIdle int, debugs bool) (*DBv4, error) {
	Debug = debugs
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		factory, ok := factory2.Get(driverName)
		if !ok {
			return nil, errors.New(fmt.Sprintf("can't find '%s' SqlFactory", driverName))
		}
		return &DBv4{id: defaultDBId, db: db, factory: factory}, nil
	}
}

func OpenV4(driverName string, dsn string, debugs bool) (*DBv4, error) {
	Debug = debugs
	factory, ok := factory2.Get(driverName)
	if !ok {
		return nil, errors.New(fmt.Sprintf("driver [%s] not factory", driverName))
	}
	db, err := factory.OpenDb(dsn)

	if err != nil {
		return nil, err
	} else {
		err = db.Ping()
		if err != nil {
			return nil, err
		}
		db.SetConnMaxLifetime(time.Minute * 1)
		factory, ok := factory2.Get(driverName)
		if !ok {
			return nil, errors.New(fmt.Sprintf("can't find '%s' SqlFactory", driverName))
		}
		return &DBv4{id: defaultDBId, db: db, factory: factory}, nil
	}
}
