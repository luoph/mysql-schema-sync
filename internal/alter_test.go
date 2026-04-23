// Copyright(C) 2022 github.com/hidu  All Rights Reserved.
// Author: hidu <duv123@gmail.com>
// Date: 2022/3/11

package internal

import (
	"strings"
	"testing"
)

func TestTableAlterData_StringWrapsTransaction(t *testing.T) {
	t.Run("alter with SQL wraps BEGIN/COMMIT", func(t *testing.T) {
		ta := &TableAlterData{
			Table: "user",
			Type:  alterTypeAlter,
			SQL: []string{
				`ALTER TABLE "user" ADD COLUMN "age" integer;`,
				`CREATE INDEX idx_user_age ON public.user USING btree (age);`,
			},
		}
		got := ta.String()
		lines := strings.Split(got, "\n")
		if len(lines) < 5 {
			t.Fatalf("expected >=5 lines, got %d: %q", len(lines), got)
		}
		if lines[2] != "BEGIN;" {
			t.Errorf("expected line[2]=BEGIN;, got %q", lines[2])
		}
		if lines[len(lines)-1] != "COMMIT;" {
			t.Errorf("expected last line=COMMIT;, got %q", lines[len(lines)-1])
		}
	})

	t.Run("no change keeps header only", func(t *testing.T) {
		ta := &TableAlterData{Table: "user", Type: alterTypeNo}
		got := ta.String()
		if strings.Contains(got, "BEGIN;") || strings.Contains(got, "COMMIT;") {
			t.Errorf("no-change table should not contain BEGIN/COMMIT, got %q", got)
		}
	})
}

func Test_fmtTableCreateSQL(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "del auto_incr",
			args: args{
				sql: `CREATE TABLE user (
				id bigint unsigned NOT NULL AUTO_INCREMENT,
				email varchar(1000) NOT NULL DEFAULT '',
				PRIMARY KEY (id)
			) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb3`,
			},
			want: `CREATE TABLE user (
				id bigint unsigned NOT NULL AUTO_INCREMENT,
				email varchar(1000) NOT NULL DEFAULT '',
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3`,
		},
		{
			name: "del auto_incr 2",
			args: args{
				sql: `CREATE TABLE user (
				id bigint unsigned NOT NULL AUTO_INCREMENT,
				email varchar(1000) NOT NULL DEFAULT '',
				PRIMARY KEY (id)
			) ENGINE=InnoDB AUTO_INCREMENT=4049116 DEFAULT CHARSET=utf8mb4`,
			},
			want: `CREATE TABLE user (
				id bigint unsigned NOT NULL AUTO_INCREMENT,
				email varchar(1000) NOT NULL DEFAULT '',
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fmtTableCreateSQL(tt.args.sql); got != tt.want {
				t.Errorf("fmtTableCreateSQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
