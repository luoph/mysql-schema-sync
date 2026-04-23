package internal

import (
	"strings"
	"testing"

	"github.com/xanygo/anygo/xt"
)

func TestPostgresDialect_ParseSchema(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("basic table with primary key", func(t *testing.T) {
		schema := testLoadFile("testdata/pg/user_0.sql")
		mys := d.ParseSchema(schema)

		xt.Equal(t, 5, mys.Fields.Len())
		keys := mys.Fields.Keys()
		xt.Equal(t, "id", keys[0])
		xt.Equal(t, "email", keys[1])
		xt.Equal(t, "register_time", keys[2])
		xt.Equal(t, "password", keys[3])
		xt.Equal(t, "status", keys[4])

		xt.Equal(t, 1, len(mys.IndexAll))
		pk, has := mys.IndexAll["user_pkey"]
		xt.Equal(t, true, has)
		xt.Equal(t, indexTypePrimary, pk.IndexType)
	})

	t.Run("table with fewer columns", func(t *testing.T) {
		schema := testLoadFile("testdata/pg/user_1.sql")
		mys := d.ParseSchema(schema)

		xt.Equal(t, 2, mys.Fields.Len())
		keys := mys.Fields.Keys()
		xt.Equal(t, "id", keys[0])
		xt.Equal(t, "email", keys[1])
	})

	t.Run("table with foreign key", func(t *testing.T) {
		schema := `CREATE TABLE "orders" (
  "id" bigint NOT NULL,
  "user_id" bigint NOT NULL,
  CONSTRAINT "orders_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "orders_user_fk" FOREIGN KEY ("user_id") REFERENCES "user" ("id")
)`
		mys := d.ParseSchema(schema)

		xt.Equal(t, 2, mys.Fields.Len())
		xt.Equal(t, 1, len(mys.IndexAll))
		xt.Equal(t, 1, len(mys.ForeignAll))

		fk := mys.ForeignAll["orders_user_fk"]
		xt.Equal(t, indexTypeForeignKey, fk.IndexType)
		xt.Equal(t, 1, len(fk.RelationTables))
		xt.Equal(t, "user", fk.RelationTables[0])
	})

	t.Run("table with unique and check constraints", func(t *testing.T) {
		schema := `CREATE TABLE "products" (
  "id" bigint NOT NULL,
  "name" varchar(255) NOT NULL,
  "price" numeric(10,2) NOT NULL,
  CONSTRAINT "products_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "products_name_key" UNIQUE ("name"),
  CONSTRAINT "products_price_check" CHECK (("price" > 0))
)`
		mys := d.ParseSchema(schema)

		xt.Equal(t, 3, mys.Fields.Len())
		// PRIMARY + UNIQUE + CHECK = 3 in IndexAll
		xt.Equal(t, 3, len(mys.IndexAll))
		_, hasPK := mys.IndexAll["products_pkey"]
		xt.Equal(t, true, hasPK)
		_, hasUK := mys.IndexAll["products_name_key"]
		xt.Equal(t, true, hasUK)
		chk, hasChk := mys.IndexAll["products_price_check"]
		xt.Equal(t, true, hasChk)
		xt.Equal(t, checkConstraint, chk.IndexType)
	})
}

func TestPostgresDialect_FieldsEqual(t *testing.T) {
	d := &PostgresDialect{}

	strPtr := func(s string) *string { return &s }

	t.Run("identical fields", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "name", ColumnType: "varchar(255)", IsNullAble: "NO"}
		b := &FieldInfo{ColumnName: "name", ColumnType: "varchar(255)", IsNullAble: "NO"}
		xt.Equal(t, true, d.FieldsEqual(a, b))
	})

	t.Run("type alias normalization", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "age", ColumnType: "int4", IsNullAble: "YES"}
		b := &FieldInfo{ColumnName: "age", ColumnType: "integer", IsNullAble: "YES"}
		xt.Equal(t, true, d.FieldsEqual(a, b))
	})

	t.Run("different types", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "age", ColumnType: "integer", IsNullAble: "YES"}
		b := &FieldInfo{ColumnName: "age", ColumnType: "bigint", IsNullAble: "YES"}
		xt.Equal(t, false, d.FieldsEqual(a, b))
	})

	t.Run("different nullability", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "NO"}
		b := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES"}
		xt.Equal(t, false, d.FieldsEqual(a, b))
	})

	t.Run("different defaults", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES", ColumnDefault: strPtr("'hello'")}
		b := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES", ColumnDefault: strPtr("'world'")}
		xt.Equal(t, false, d.FieldsEqual(a, b))
	})

	t.Run("default with type cast cleaned", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES", ColumnDefault: strPtr("'hello'::text")}
		b := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES", ColumnDefault: strPtr("'hello'")}
		xt.Equal(t, true, d.FieldsEqual(a, b))
	})

	t.Run("different comments", func(t *testing.T) {
		a := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES", ColumnComment: "用户名"}
		b := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES", ColumnComment: "姓名"}
		xt.Equal(t, false, d.FieldsEqual(a, b))
	})

	t.Run("nil fields", func(t *testing.T) {
		xt.Equal(t, true, d.FieldsEqual(nil, nil))
		xt.Equal(t, false, d.FieldsEqual(&FieldInfo{}, nil))
		xt.Equal(t, false, d.FieldsEqual(nil, &FieldInfo{}))
	})
}

func TestPostgresDialect_FieldDef(t *testing.T) {
	d := &PostgresDialect{}

	strPtr := func(s string) *string { return &s }

	t.Run("basic column", func(t *testing.T) {
		f := &FieldInfo{ColumnName: "name", ColumnType: "varchar(255)", IsNullAble: "NO"}
		xt.Equal(t, `"name" varchar(255) NOT NULL`, d.FieldDef(f))
	})

	t.Run("nullable column", func(t *testing.T) {
		f := &FieldInfo{ColumnName: "bio", ColumnType: "text", IsNullAble: "YES"}
		xt.Equal(t, `"bio" text`, d.FieldDef(f))
	})

	t.Run("column with default", func(t *testing.T) {
		f := &FieldInfo{ColumnName: "status", ColumnType: "integer", IsNullAble: "NO", ColumnDefault: strPtr("0")}
		xt.Equal(t, `"status" integer NOT NULL DEFAULT 0`, d.FieldDef(f))
	})

	t.Run("serial column skips default", func(t *testing.T) {
		f := &FieldInfo{ColumnName: "id", ColumnType: "bigint", IsNullAble: "NO", Extra: "auto_increment",
			ColumnDefault: strPtr("nextval('id_seq'::regclass)")}
		xt.Equal(t, `"id" bigint NOT NULL`, d.FieldDef(f))
	})
}

func TestPostgresDialect_GenChangeColumn(t *testing.T) {
	d := &PostgresDialect{}

	strPtr := func(s string) *string { return &s }

	t.Run("type change", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "name", ColumnType: "varchar(500)", IsNullAble: "NO"}
		dst := &FieldInfo{ColumnName: "name", ColumnType: "varchar(255)", IsNullAble: "NO"}
		clauses := d.GenChangeColumn("name", src, dst)
		xt.Equal(t, 1, len(clauses))
		xt.Equal(t, `ALTER COLUMN "name" TYPE varchar(500)`, clauses[0])
	})

	t.Run("nullability change", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "email", ColumnType: "varchar(255)", IsNullAble: "NO"}
		dst := &FieldInfo{ColumnName: "email", ColumnType: "varchar(255)", IsNullAble: "YES"}
		clauses := d.GenChangeColumn("email", src, dst)
		xt.Equal(t, 1, len(clauses))
		xt.Equal(t, `ALTER COLUMN "email" SET NOT NULL`, clauses[0])
	})

	t.Run("drop not null", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "email", ColumnType: "varchar(255)", IsNullAble: "YES"}
		dst := &FieldInfo{ColumnName: "email", ColumnType: "varchar(255)", IsNullAble: "NO"}
		clauses := d.GenChangeColumn("email", src, dst)
		xt.Equal(t, 1, len(clauses))
		xt.Equal(t, `ALTER COLUMN "email" DROP NOT NULL`, clauses[0])
	})

	t.Run("default change", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "status", ColumnType: "integer", IsNullAble: "NO", ColumnDefault: strPtr("1")}
		dst := &FieldInfo{ColumnName: "status", ColumnType: "integer", IsNullAble: "NO", ColumnDefault: strPtr("0")}
		clauses := d.GenChangeColumn("status", src, dst)
		xt.Equal(t, 1, len(clauses))
		xt.Equal(t, `ALTER COLUMN "status" SET DEFAULT 1`, clauses[0])
	})

	t.Run("drop default", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "status", ColumnType: "integer", IsNullAble: "NO"}
		dst := &FieldInfo{ColumnName: "status", ColumnType: "integer", IsNullAble: "NO", ColumnDefault: strPtr("0")}
		clauses := d.GenChangeColumn("status", src, dst)
		xt.Equal(t, 1, len(clauses))
		xt.Equal(t, `ALTER COLUMN "status" DROP DEFAULT`, clauses[0])
	})

	t.Run("multiple changes", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "price", ColumnType: "numeric(12,2)", IsNullAble: "NO", ColumnDefault: strPtr("0")}
		dst := &FieldInfo{ColumnName: "price", ColumnType: "numeric(10,2)", IsNullAble: "YES", ColumnDefault: strPtr("100")}
		clauses := d.GenChangeColumn("price", src, dst)
		xt.Equal(t, 3, len(clauses))
		xt.Equal(t, `ALTER COLUMN "price" TYPE numeric(12,2)`, clauses[0])
		xt.Equal(t, `ALTER COLUMN "price" SET NOT NULL`, clauses[1])
		xt.Equal(t, `ALTER COLUMN "price" SET DEFAULT 0`, clauses[2])
	})

	t.Run("no change", func(t *testing.T) {
		src := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES"}
		dst := &FieldInfo{ColumnName: "name", ColumnType: "text", IsNullAble: "YES"}
		clauses := d.GenChangeColumn("name", src, dst)
		xt.Equal(t, 0, len(clauses))
	})
}

func TestPostgresDialect_GenIndex(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("add primary key", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "pk_test", SQL: `PRIMARY KEY ("id")`}
		sqls := d.GenAddIndex("test", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `ADD CONSTRAINT "pk_test" PRIMARY KEY ("id")`, sqls[0])
	})

	t.Run("drop primary key", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "pk_test"}
		sql := d.GenDropIndex("test", idx)
		xt.Equal(t, `DROP CONSTRAINT "pk_test"`, sql)
	})

	t.Run("add unique constraint", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeUnique, Name: "uq_email", SQL: `UNIQUE ("email")`}
		sqls := d.GenAddIndex("test", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `ADD CONSTRAINT "uq_email" UNIQUE ("email")`, sqls[0])
	})

	t.Run("add with drop", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "pk_test", SQL: `PRIMARY KEY ("id")`}
		sqls := d.GenAddIndex("test", idx, true)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP CONSTRAINT "pk_test"`, sqls[0])
		xt.Equal(t, `ADD CONSTRAINT "pk_test" PRIMARY KEY ("id")`, sqls[1])
	})
}

func TestPostgresDialect_GenForeignKey(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("add foreign key", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user", SQL: `FOREIGN KEY ("user_id") REFERENCES "user" ("id")`}
		sqls := d.GenAddForeignKey("orders", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `ADD CONSTRAINT "fk_user" FOREIGN KEY ("user_id") REFERENCES "user" ("id")`, sqls[0])
	})

	t.Run("drop foreign key", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user"}
		sql := d.GenDropForeignKey("orders", idx)
		xt.Equal(t, `DROP CONSTRAINT "fk_user"`, sql)
	})

	t.Run("add with drop", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user", SQL: `FOREIGN KEY ("user_id") REFERENCES "user" ("id")`}
		sqls := d.GenAddForeignKey("orders", idx, true)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP CONSTRAINT "fk_user"`, sqls[0])
	})
}

func TestPostgresDialect_WrapAlterSQL(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("single clause", func(t *testing.T) {
		result := d.WrapAlterSQL("user", []string{`ADD COLUMN "name" text`}, false)
		xt.Equal(t, 1, len(result))
		xt.Equal(t, `ALTER TABLE "user" ADD COLUMN "name" text;`, result[0])
	})

	t.Run("multiple clauses", func(t *testing.T) {
		clauses := []string{`ADD COLUMN "a" text`, `ADD COLUMN "b" integer`}
		result := d.WrapAlterSQL("test", clauses, false)
		// PostgreSQL: each clause is always a separate statement
		xt.Equal(t, 2, len(result))
		xt.Equal(t, `ALTER TABLE "test" ADD COLUMN "a" text;`, result[0])
		xt.Equal(t, `ALTER TABLE "test" ADD COLUMN "b" integer;`, result[1])
	})

	t.Run("empty clauses", func(t *testing.T) {
		result := d.WrapAlterSQL("test", nil, false)
		xt.Equal(t, 0, len(result))
	})
}

func TestPostgresDialect_GenCommentColumnSQL(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("set comment", func(t *testing.T) {
		sql := d.GenCommentColumnSQL("user", "name", "用户名")
		xt.Equal(t, `COMMENT ON COLUMN "user"."name" IS '用户名';`, sql)
	})

	t.Run("remove comment", func(t *testing.T) {
		sql := d.GenCommentColumnSQL("user", "name", "")
		xt.Equal(t, `COMMENT ON COLUMN "user"."name" IS NULL;`, sql)
	})

	t.Run("comment with single quotes", func(t *testing.T) {
		sql := d.GenCommentColumnSQL("user", "name", "it's a name")
		xt.Equal(t, `COMMENT ON COLUMN "user"."name" IS 'it''s a name';`, sql)
	})
}

func TestPostgresDialect_Misc(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("quote", func(t *testing.T) {
		xt.Equal(t, `"user"`, d.Quote("user"))
	})

	t.Run("driver name", func(t *testing.T) {
		xt.Equal(t, "pgx", d.DriverName())
	})

	t.Run("supports column order", func(t *testing.T) {
		xt.Equal(t, false, d.SupportsColumnOrder())
	})

	t.Run("gen drop table", func(t *testing.T) {
		xt.Equal(t, `DROP TABLE "user";`, d.GenDropTable("user"))
	})

	t.Run("gen create table", func(t *testing.T) {
		schema := `CREATE TABLE "test" ("id" integer)`
		xt.Equal(t, schema+";", d.GenCreateTable(schema))
	})

	t.Run("gen drop column", func(t *testing.T) {
		xt.Equal(t, `DROP COLUMN "name"`, d.GenDropColumn("name"))
	})

	t.Run("gen add column", func(t *testing.T) {
		xt.Equal(t, `ADD COLUMN "name" text NOT NULL`, d.GenAddColumn(`"name" text NOT NULL`, "", false, 0))
	})

	t.Run("clean table schema is no-op", func(t *testing.T) {
		schema := `CREATE TABLE "test" ("id" integer)`
		xt.Equal(t, schema, d.CleanTableSchema(schema))
	})
}

func TestPgNormalizeColumnType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"int4", "integer"},
		{"int8", "bigint"},
		{"int2", "smallint"},
		{"float4", "real"},
		{"float8", "double precision"},
		{"bool", "boolean"},
		{"varchar", "character varying"},
		{"varchar(100)", "character varying(100)"},
		{"integer", "integer"},
		{"bigint", "bigint"},
		{"text", "text"},
		{"numeric(10,2)", "numeric(10,2)"},
		{"integer[]", "integer[]"},
		{"int4[]", "integer[]"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			xt.Equal(t, tt.want, pgNormalizeColumnType(tt.input))
		})
	}
}

func TestPgSerialTypeFor(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantOK  bool
	}{
		{"bigint", "bigserial", true},
		{"int8", "bigserial", true},
		{"BIGINT", "bigserial", true},
		{"  bigint  ", "bigserial", true},
		{"integer", "serial", true},
		{"int", "serial", true},
		{"int4", "serial", true},
		{"smallint", "smallserial", true},
		{"int2", "smallserial", true},
		{"text", "", false},
		{"numeric(10,2)", "", false},
		{"", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := pgSerialTypeFor(tt.input)
			xt.Equal(t, tt.wantOK, ok)
			xt.Equal(t, tt.want, got)
		})
	}
}

func TestPgCleanDefault(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"'hello'::character varying", "'hello'"},
		{"0::numeric", "0"},
		{"'hello'", "'hello'"},
		{"42", "42"},
		{"nextval('id_seq'::regclass)", "nextval('id_seq'::regclass)"}, // regclass cast is special
		{"NULL", "NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			xt.Equal(t, tt.want, pgCleanDefault(tt.input))
		})
	}
}

// TestPostgresDialect_getAlterDataBySchema tests PG schema sync via the SchemaSync integration
func TestPostgresDialect_getAlterDataBySchema(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		sSchema string
		dSchema string
		cfg     *Config
		want    string
	}{
		{
			name:    "pg user 0->1: add missing columns",
			table:   "user",
			sSchema: testLoadFile("testdata/pg/user_0.sql"),
			dSchema: testLoadFile("testdata/pg/user_1.sql"),
			cfg:     &Config{},
			want:    testLoadFile("testdata/pg/result_1.sql"),
		},
		{
			name:    "pg user 1->0: reverse (no change, dest has more columns)",
			table:   "user",
			sSchema: testLoadFile("testdata/pg/user_1.sql"),
			dSchema: testLoadFile("testdata/pg/user_0.sql"),
			cfg:     &Config{},
			want:    testLoadFile("testdata/pg/result_2.sql"),
		},
		{
			name:    "pg constraint rename: same definition, different name",
			table:   "user_audio",
			sSchema: testLoadFile("testdata/pg/user_audio_src.sql"),
			dSchema: testLoadFile("testdata/pg/user_audio_dst.sql"),
			cfg:     &Config{Drop: true},
			want:    testLoadFile("testdata/pg/result_constraint_rename.sql"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &SchemaSync{
				Config: tt.cfg,
				SourceDb: &MyDb{
					dialect: &PostgresDialect{},
				},
			}
			got := sc.getAlterDataBySchema(tt.table, tt.sSchema, tt.dSchema, tt.cfg)
			t.Log("got alter:\n", got.String())
			xt.Equal(t, strings.TrimSpace(tt.want), strings.TrimSpace(got.String()))
		})
	}
}
