package define

// Result represents a database result
type Result struct {
	ID int64
}

// LastInsertId returns the last inserted ID
func (r *Result) LastInsertId() (int64, error) {
	return r.ID, nil
}

// RowsAffected returns the number of rows affected
func (r *Result) RowsAffected() (int64, error) {
	return 0, nil
}
