// Copyright(C) 2022 github.com/fsgo  All Rights Reserved.
// Author: hidu <duv123@gmail.com>
// Date: 2022/9/25

package internal

import (
	"testing"

	"github.com/xanygo/anygo/xt"
)

func TestSchemaSync_getAlterDataBySchema(t *testing.T) {
	type args struct {
		table   string
		sSchema string
		dSchema string
		cfg     *Config
	}
	tests := []struct {
		name string
		sc   *SchemaSync
		args args
		want string
	}{
		{
			name: "user 0-1",
			args: args{
				table:   "user",
				sSchema: testLoadFile("testdata/user/user_0.sql"),
				dSchema: testLoadFile("testdata/user/user_1.sql"),
				cfg:     &Config{},
			},
			sc: &SchemaSync{
				Config: &Config{},
			},
			want: testLoadFile("testdata/user/result_1.sql"),
		},
		{
			name: "user 0-1 ssc",
			args: args{
				table:   "user",
				sSchema: testLoadFile("testdata/user/user_0.sql"),
				dSchema: testLoadFile("testdata/user/user_1.sql"),
				cfg: &Config{
					SingleSchemaChange: true,
				},
			},
			sc: &SchemaSync{
				Config: &Config{},
			},
			want: testLoadFile("testdata/user/result_2.sql"),
		},
		{
			name: "user 0-1 ssc",
			args: args{
				table:   "user",
				sSchema: testLoadFile("testdata/user/user_0.sql"),
				dSchema: testLoadFile("testdata/user/user_1.sql"),
				cfg: &Config{
					SingleSchemaChange: true,
				},
			},
			sc: &SchemaSync{
				Config: &Config{},
			},
			want: testLoadFile("testdata/user/result_2.sql"),
		},
		{
			name: "user 1-0 ssc",
			args: args{
				table:   "user",
				sSchema: testLoadFile("testdata/user/user_1.sql"),
				dSchema: testLoadFile("testdata/user/user_0.sql"),
				cfg: &Config{
					SingleSchemaChange: true,
				},
			},
			sc: &SchemaSync{
				Config: &Config{},
			},
			want: testLoadFile("testdata/user/result_3.sql"),
		},
		{
			name: "user 2-0 ssc",
			args: args{
				table:   "user",
				sSchema: testLoadFile("testdata/user/user_2.sql"),
				dSchema: testLoadFile("testdata/user/user_0.sql"),
				cfg:     &Config{},
			},
			sc: &SchemaSync{
				Config: &Config{},
			},
			want: testLoadFile("testdata/user/result_4.sql"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.sc.getAlterDataBySchema(tt.args.table, tt.args.sSchema, tt.args.dSchema, tt.args.cfg)
			t.Log("got alter:\n", got.String())
			xt.Equal(t, tt.want, got.String())
		})
	}
}

func TestClassifySQL(t *testing.T) {
	t.Run("guard statements go standalone", func(t *testing.T) {
		var alterClauses, standalone []string
		classifySQL([]string{
			"SET @__mss_sql = (SELECT 1)",
			"PREPARE __mss_stmt FROM @__mss_sql",
			"EXECUTE __mss_stmt",
			"DEALLOCATE PREPARE __mss_stmt",
		}, &alterClauses, &standalone)
		xt.Equal(t, 0, len(alterClauses))
		xt.Equal(t, 4, len(standalone))
	})

	t.Run("alter subclauses stay in alterClauses", func(t *testing.T) {
		var alterClauses, standalone []string
		classifySQL([]string{
			`ADD COLUMN "name" text`,
			`DROP COLUMN "old"`,
		}, &alterClauses, &standalone)
		xt.Equal(t, 2, len(alterClauses))
		xt.Equal(t, 0, len(standalone))
	})

	t.Run("create and drop index go standalone", func(t *testing.T) {
		var alterClauses, standalone []string
		classifySQL([]string{
			`CREATE INDEX IF NOT EXISTS "x" ON "t" USING btree (a);`,
			`DROP INDEX IF EXISTS "x";`,
		}, &alterClauses, &standalone)
		xt.Equal(t, 0, len(alterClauses))
		xt.Equal(t, 2, len(standalone))
	})

	t.Run("DO block goes standalone", func(t *testing.T) {
		var alterClauses, standalone []string
		doBlock := `DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid JOIN pg_namespace n ON t.relnamespace = n.oid WHERE n.nspname = 'public' AND t.relname = 'test' AND c.conname = 'pk_test') THEN ALTER TABLE "test" ADD CONSTRAINT "pk_test" PRIMARY KEY ("id"); END IF; END $$;`
		classifySQL([]string{doBlock}, &alterClauses, &standalone)
		xt.Equal(t, 0, len(alterClauses))
		xt.Equal(t, 1, len(standalone))
		xt.Equal(t, doBlock, standalone[0])
	})
}
