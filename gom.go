package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/kmlixh/gom/v4/define"
)

func OpenWithConfig(driverName string, dsn string, maxOpen int, maxIdle int, debugs bool) (*DB, error) {
	define.Debug = debugs
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		factory, ok := define.GetFactory(driverName)
		if !ok {
			return nil, errors.New(fmt.Sprintf("can't find '%s' SqlFactory", driverName))
		}
		return &DB{db: db, factory: factory}, nil
	}
}

func Open(driverName string, dsn string, debugs bool) (*DB, error) {
	define.Debug = debugs
	factory, ok := define.GetFactory(driverName)
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
		factory, ok := define.GetFactory(driverName)
		if !ok {
			return nil, errors.New(fmt.Sprintf("can't find '%s' SqlFactory", driverName))
		}
		return &DB{db: db, factory: factory}, nil
	}
}
