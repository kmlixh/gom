package define

import "database/sql"

// Result implements sql.Result interface
type Result struct {
	ID       int64
	Affected int64
	Error    error // 存储操作过程中的错误信息
}

// LastInsertId returns the last inserted ID
func (r *Result) LastInsertId() (int64, error) {
	if r.Error != nil {
		return 0, r.Error
	}
	return r.ID, nil
}

// RowsAffected returns the number of rows affected
func (r *Result) RowsAffected() (int64, error) {
	if r.Error != nil {
		return 0, r.Error
	}
	return r.Affected, nil
}

// Ensure Result implements sql.Result interface
var _ sql.Result = (*Result)(nil)
