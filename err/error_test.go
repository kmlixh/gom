package err

import "testing"

func TestErr_Error(t *testing.T) {
	type fields struct {
		msg string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"测试写入的Error", fields{"test error"}, "test error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			er := Err{
				msg: tt.fields.msg,
			}
			if got := er.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestError(t *testing.T) {
	type args struct {
		msg string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Err创建测试", args{"test error"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Error(tt.args.msg); (err != nil) != tt.wantErr {
				t.Errorf("Error() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
