package query

import (
	"testing"
)

func TestParseLabelSelector_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  map[string]string
	}{
		{`{app="nginx"}`, map[string]string{"app": "nginx"}},
		{`{app="nginx", env="prod"}`, map[string]string{"app": "nginx", "env": "prod"}},
		{`{ app = "nginx" , env = "prod" }`, map[string]string{"app": "nginx", "env": "prod"}},
		{`app="nginx"`, map[string]string{"app": "nginx"}},
		{`{cluster="us-west-2"}`, map[string]string{"cluster": "us-west-2"}},
		{`{}`, nil},
		{``, nil},
	}

	for _, tt := range tests {
		got, err := ParseLabelSelector(tt.input)
		if err != nil {
			t.Errorf("ParseLabelSelector(%q) error: %v", tt.input, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("ParseLabelSelector(%q) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for k, v := range tt.want {
			if got[k] != v {
				t.Errorf("ParseLabelSelector(%q)[%q] = %q, want %q", tt.input, k, got[k], v)
			}
		}
	}
}

func TestParseLabelSelector_Invalid(t *testing.T) {
	tests := []string{
		`{app}`,
		`{="nginx"}`,
	}

	for _, input := range tests {
		_, err := ParseLabelSelector(input)
		if err == nil {
			t.Errorf("ParseLabelSelector(%q) expected error", input)
		}
	}
}
