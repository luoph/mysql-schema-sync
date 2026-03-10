package internal

import (
	"testing"

	"github.com/xanygo/anygo/xt"
)

func TestDetectDialect(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "mysql dsn",
			dsn:  "user:password@tcp(localhost:3306)/mydb",
			want: "mysql",
		},
		{
			name: "postgres url scheme",
			dsn:  "postgres://user:pass@localhost:5432/mydb",
			want: "pgx",
		},
		{
			name: "postgresql url scheme",
			dsn:  "postgresql://user:pass@localhost:5432/mydb",
			want: "pgx",
		},
		{
			name: "postgres keyword format",
			dsn:  "host=localhost port=5432 dbname=mydb user=user password=pass",
			want: "pgx",
		},
		{
			name: "mysql with params",
			dsn:  "root:@tcp(127.0.0.1:3306)/test?charset=utf8mb4",
			want: "mysql",
		},
		{
			name: "postgres with options",
			dsn:  "postgres://user:pass@localhost:5432/mydb?sslmode=disable",
			want: "pgx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := DetectDialect(tt.dsn)
			xt.Equal(t, tt.want, d.DriverName())
		})
	}
}
