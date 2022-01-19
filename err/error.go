package err

func New(msg string) error {
	return Err{msg: msg}
}

type Err struct {
	msg string
}

func (er Err) Error() string {
	return er.msg
}
