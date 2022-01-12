package gom

import (
	"database/sql"
	"errors"
	"fmt"
	"gitee.com/janyees/gom/register"
	"gitee.com/janyees/gom/structs"
	"time"
)

var Debug bool

const defaultDBId = -1000

func OpenWithConfig(driverName string, dsn string, maxOpen int, maxIdle int, debugs bool) (*DB, error) {
	Debug = debugs
	structs.Debug = debugs
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetMaxOpenConns(maxOpen)
		db.SetMaxIdleConns(maxIdle)
		factory, ok := register.Get(driverName)
		if !ok {
			panic(errors.New(fmt.Sprintf("can't find '%s' SqlFactory", driverName)))
		}
		return &DB{id: defaultDBId, db: db, factory: factory}, nil
	}
}

func Open(driverName string, dsn string, debugs bool) (*DB, error) {
	Debug = debugs
	structs.Debug = debugs
	db, err := sql.Open(driverName, dsn)
	db.SetConnMaxLifetime(time.Minute * 1)
	if err != nil {
		return nil, err
	} else {
		factory, ok := register.Get(driverName)
		if !ok {
			panic(errors.New(fmt.Sprintf("can't find '%s' SqlFactory", driverName)))
		}
		return &DB{id: defaultDBId, db: db, factory: factory}, nil
	}
}
