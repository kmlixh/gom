package define

import "database/sql"

// Result implements sql.Result interface
type Result struct {
	ID       int64
	Affected int64
}

// LastInsertId returns the last inserted ID
func (r *Result) LastInsertId() (int64, error) {
	return r.ID, nil
}

// RowsAffected returns the number of rows affected
func (r *Result) RowsAffected() (int64, error) {
	return r.Affected, nil
}

// Ensure Result implements sql.Result interface
var _ sql.Result = (*Result)(nil)
