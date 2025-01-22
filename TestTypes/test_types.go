// Code generated by gom at 2025-01-06 17:53:08. DO NOT EDIT.
package main

import (
	"time"
)

// TestTypes 
type TestTypes struct {
	Id int `gom:"id,@,notnull"` 
	IntCol *int `gom:"int_col"` 
	BigintCol *int64 `gom:"bigint_col"` 
	TextCol *string `gom:"text_col"` 
	VarcharCol *string `gom:"varchar_col"` 
	BoolCol *bool `gom:"bool_col"` 
	FloatCol *string `gom:"float_col"` 
	DoubleCol *float64 `gom:"double_col"` 
	DecimalCol *string `gom:"decimal_col"` 
	DateCol *time.Time `gom:"date_col"` 
	TimestampCol *time.Time `gom:"timestamp_col"` 
	JsonCol *string `gom:"json_col"` 
	JsonbCol *string `gom:"jsonb_col"` 
	UuidCol *string `gom:"uuid_col"` 
	ArrayCol *int `gom:"array_col"` 
}

// TableName returns the table name
func (m *TestTypes) TableName() string {
	return "test_types"
}
