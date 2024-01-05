package gom

import "testing"

func TestCamelToSnakeString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"测试驼峰转换", args{"NameTest"}, "name_test"},
		{"测试驼峰双大写转换", args{"NameTTest"}, "name_t_test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CamelToSnakeString(tt.args.s); got != tt.want {
				t.Errorf("CamelToSnakeString() = %v, want %v", got, tt.want)
			}
		})
	}
}
