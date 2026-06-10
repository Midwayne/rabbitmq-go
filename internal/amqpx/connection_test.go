package amqpx

import "testing"

func TestMaskURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "credentials are masked",
			in:   "amqp://user:secret@localhost:5672/",
			want: "amqp://user:***@localhost:5672/",
		},
		{
			name: "no credentials passes through",
			in:   "amqp://localhost:5672/",
			want: "amqp://localhost:5672/",
		},
		{
			name: "user only without password passes through",
			in:   "amqp://user@localhost:5672/",
			want: "amqp://user@localhost:5672/",
		},
		{
			name: "amqps scheme",
			in:   "amqps://user:secret@rabbit.example.com:5671/vhost",
			want: "amqps://user:***@rabbit.example.com:5671/vhost",
		},
		{
			name: "password containing at sign",
			in:   "amqp://user:p@ss@localhost:5672/",
			want: "amqp://user:***@localhost:5672/",
		},
		{
			name: "no scheme passes through",
			in:   "localhost:5672",
			want: "localhost:5672",
		},
		{
			name: "empty string",
			in:   "",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MaskURL(tc.in); got != tc.want {
				t.Errorf("MaskURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
