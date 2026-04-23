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

	t.Run("add btree index with full CREATE INDEX def", func(t *testing.T) {
		idx := &DbIndex{
			IndexType: indexTypeIndex,
			Name:      "idx_user_id",
			SQL:       `CREATE INDEX idx_user_id ON public.user_agent_file USING btree (user_id)`,
		}
		sqls := d.GenAddIndex("user_agent_file", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `CREATE INDEX idx_user_id ON public.user_agent_file USING btree (user_id);`, sqls[0])
	})

	t.Run("add hnsw index with full def", func(t *testing.T) {
		idx := &DbIndex{
			IndexType: indexTypeIndex,
			Name:      "idx_hnsw",
			SQL:       `CREATE INDEX idx_hnsw ON public.note_embedding USING hnsw (embedding vector_cosine_ops)`,
		}
		sqls := d.GenAddIndex("note_embedding", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `CREATE INDEX idx_hnsw ON public.note_embedding USING hnsw (embedding vector_cosine_ops);`, sqls[0])
	})

	t.Run("add partial index with WHERE clause", func(t *testing.T) {
		idx := &DbIndex{
			IndexType: indexTypeIndex,
			Name:      "idx_active",
			SQL:       `CREATE INDEX idx_active ON public.t USING btree (user_id) WHERE (is_deleted = false)`,
		}
		sqls := d.GenAddIndex("t", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `CREATE INDEX idx_active ON public.t USING btree (user_id) WHERE (is_deleted = false);`, sqls[0])
	})

	t.Run("full def index with drop first", func(t *testing.T) {
		idx := &DbIndex{
			IndexType: indexTypeIndex,
			Name:      "idx_user_id",
			SQL:       `CREATE INDEX idx_user_id ON public.t USING btree (user_id)`,
		}
		sqls := d.GenAddIndex("t", idx, true)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP INDEX "idx_user_id";`, sqls[0])
		xt.Equal(t, `CREATE INDEX idx_user_id ON public.t USING btree (user_id);`, sqls[1])
	})

	t.Run("legacy expression-only index def falls back to btree wrap", func(t *testing.T) {
		// 旧路径兼容：SQL 字段不是完整 CREATE INDEX 语句时仍用 btree 包装
		idx := &DbIndex{IndexType: indexTypeIndex, Name: "idx_a", SQL: `"a", "b"`}
		sqls := d.GenAddIndex("t", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `CREATE INDEX "idx_a" ON "t" USING btree ("a", "b");`, sqls[0])
	})
}

func TestPostgresDialect_GenTrigger(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("add trigger uses pg_get_triggerdef output", func(t *testing.T) {
		trg := &DbTrigger{
			Name:       "trg_guard",
			Table:      "user_document",
			Definition: "CREATE TRIGGER trg_guard BEFORE UPDATE ON user_document FOR EACH ROW EXECUTE FUNCTION fn_guard()",
		}
		xt.Equal(t,
			"CREATE TRIGGER trg_guard BEFORE UPDATE ON user_document FOR EACH ROW EXECUTE FUNCTION fn_guard();",
			d.GenAddTrigger(trg))
	})

	t.Run("add trigger preserves existing semicolon", func(t *testing.T) {
		trg := &DbTrigger{Name: "trg_a", Table: "t", Definition: "CREATE TRIGGER trg_a BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();"}
		xt.Equal(t, trg.Definition, d.GenAddTrigger(trg))
	})

	t.Run("drop trigger quotes identifiers", func(t *testing.T) {
		trg := &DbTrigger{Name: "trg_a", Table: "user_document"}
		xt.Equal(t, `DROP TRIGGER IF EXISTS "trg_a" ON "user_document";`, d.GenDropTrigger(trg))
	})
}

func TestSchemaSync_diffTriggers(t *testing.T) {
	cfgDrop := &Config{Drop: true}
	cfgNoDrop := &Config{Drop: false}

	mkSC := func(cfg *Config) *SchemaSync {
		return &SchemaSync{Config: cfg, SourceDb: &MyDb{dialect: &PostgresDialect{}}}
	}
	trg := func(name, def string) *DbTrigger {
		return &DbTrigger{Name: name, Table: "t", Definition: def}
	}

	t.Run("source add", func(t *testing.T) {
		alter := &TableAlterData{
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a ...")}},
				Dest:   &MySchema{},
			},
		}
		sqls := mkSC(cfgNoDrop).diffTriggers(alter)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, "CREATE TRIGGER a ...;", sqls[0])
	})

	t.Run("definition changed: drop then create", func(t *testing.T) {
		alter := &TableAlterData{
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a v2")}},
				Dest:   &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a v1")}},
			},
		}
		sqls := mkSC(cfgNoDrop).diffTriggers(alter)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP TRIGGER IF EXISTS "a" ON "t";`, sqls[0])
		xt.Equal(t, "CREATE TRIGGER a v2;", sqls[1])
	})

	t.Run("unchanged: no sql", func(t *testing.T) {
		alter := &TableAlterData{
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a")}},
				Dest:   &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a")}},
			},
		}
		sqls := mkSC(cfgNoDrop).diffTriggers(alter)
		xt.Equal(t, 0, len(sqls))
	})

	t.Run("dest only + drop enabled: drop", func(t *testing.T) {
		alter := &TableAlterData{
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{},
				Dest:   &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a")}},
			},
		}
		sqls := mkSC(cfgDrop).diffTriggers(alter)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `DROP TRIGGER IF EXISTS "a" ON "t";`, sqls[0])
	})

	t.Run("dest only + drop disabled: keep", func(t *testing.T) {
		alter := &TableAlterData{
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{},
				Dest:   &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a")}},
			},
		}
		sqls := mkSC(cfgNoDrop).diffTriggers(alter)
		xt.Equal(t, 0, len(sqls))
	})
}

func TestPostgresDialect_GenFunction(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("add function reuses definition and ensures semicolon", func(t *testing.T) {
		fn := &DbFunction{
			Name:       "fn_guard",
			Signature:  "",
			Definition: "CREATE OR REPLACE FUNCTION public.fn_guard() RETURNS trigger LANGUAGE plpgsql AS $function$ BEGIN RETURN NEW; END; $function$",
		}
		got := d.GenAddFunction(fn)
		xt.Equal(t, true, strings.HasSuffix(got, ";"))
		xt.Equal(t, true, strings.Contains(got, "CREATE OR REPLACE FUNCTION"))
	})

	t.Run("drop function with empty signature", func(t *testing.T) {
		fn := &DbFunction{Name: "fn_guard", Signature: ""}
		xt.Equal(t, `DROP FUNCTION IF EXISTS "fn_guard"();`, d.GenDropFunction(fn))
	})

	t.Run("drop overloaded function preserves args", func(t *testing.T) {
		fn := &DbFunction{Name: "fn_add", Signature: "integer, integer"}
		xt.Equal(t, `DROP FUNCTION IF EXISTS "fn_add"(integer, integer);`, d.GenDropFunction(fn))
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

func TestPostgresDialect_GenExtension(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("add uses IF NOT EXISTS", func(t *testing.T) {
		xt.Equal(t, `CREATE EXTENSION IF NOT EXISTS "vector";`,
			d.GenAddExtension(&DbExtension{Name: "vector"}))
	})
	t.Run("drop uses IF EXISTS", func(t *testing.T) {
		xt.Equal(t, `DROP EXTENSION IF EXISTS "pgcrypto";`,
			d.GenDropExtension(&DbExtension{Name: "pgcrypto"}))
	})
}

func TestPostgresDialect_GenCommentIndexSQL(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("set", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON INDEX "idx_a" IS 'hot path';`,
			d.GenCommentIndexSQL("idx_a", "hot path"))
	})
	t.Run("clear", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON INDEX "idx_a" IS NULL;`,
			d.GenCommentIndexSQL("idx_a", ""))
	})
	t.Run("escape quote", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON INDEX "idx_a" IS 'it''s';`,
			d.GenCommentIndexSQL("idx_a", "it's"))
	})
}

func TestPostgresDialect_GenCommentTableSQL(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("set comment", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON TABLE "user_note" IS '用户笔记';`,
			d.GenCommentTableSQL("user_note", "用户笔记"))
	})
	t.Run("clear comment", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON TABLE "user_note" IS NULL;`,
			d.GenCommentTableSQL("user_note", ""))
	})
	t.Run("escape single quote", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON TABLE "t" IS 'it''s a table';`,
			d.GenCommentTableSQL("t", "it's a table"))
	})
}

func TestSchemaSync_diffTableComment(t *testing.T) {
	mkSC := func() *SchemaSync {
		return &SchemaSync{Config: &Config{}, SourceDb: &MyDb{dialect: &PostgresDialect{}}}
	}
	mkAlter := func(src, dst string) *TableAlterData {
		return &TableAlterData{
			Table: "t",
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{TableComment: src},
				Dest:   &MySchema{TableComment: dst},
			},
		}
	}
	t.Run("unchanged: empty sql", func(t *testing.T) {
		xt.Equal(t, "", mkSC().diffTableComment(mkAlter("a", "a")))
	})
	t.Run("source set, dest empty: COMMENT set", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON TABLE "t" IS 'a';`, mkSC().diffTableComment(mkAlter("a", "")))
	})
	t.Run("source empty, dest set: COMMENT NULL", func(t *testing.T) {
		xt.Equal(t, `COMMENT ON TABLE "t" IS NULL;`, mkSC().diffTableComment(mkAlter("", "a")))
	})
	t.Run("both empty: no sql", func(t *testing.T) {
		xt.Equal(t, "", mkSC().diffTableComment(mkAlter("", "")))
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

func TestPostgresDialect_DefinitionsEqual(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("byte-identical", func(t *testing.T) {
		s := "CREATE INDEX idx ON t USING btree (id)"
		xt.Equal(t, true, d.DefinitionsEqual(s, s))
	})

	t.Run("whitespace-only diff collapses to equal", func(t *testing.T) {
		// 正好复现用户报告的函数空白噪声：两行缩进 7 vs 8 空格
		a := `CREATE OR REPLACE FUNCTION public.fn_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
       IF x = 1 THEN
           RETURN NEW;
       END IF;
       RETURN NEW;
END;
$function$`
		b := `CREATE OR REPLACE FUNCTION public.fn_guard()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
        IF x = 1 THEN
            RETURN NEW;
        END IF;
        RETURN NEW;
END;
$function$`
		xt.Equal(t, true, d.DefinitionsEqual(a, b))
	})

	t.Run("newline vs single space collapses", func(t *testing.T) {
		a := "CREATE INDEX idx ON t\nUSING btree (id)"
		b := "CREATE INDEX idx ON t USING btree (id)"
		xt.Equal(t, true, d.DefinitionsEqual(a, b))
	})

	t.Run("array collective cast normalized to element cast", func(t *testing.T) {
		// 用户实际 case 的 minimal 版本：源用集合级 cast，目标 PG 18 re-parse 后变元素级 cast
		a := "CHECK (((c)::text = ANY ((ARRAY['a'::character varying])::text[])))"
		b := "CHECK (((c)::text = ANY (ARRAY[('a'::character varying)::text])))"
		xt.Equal(t, true, d.DefinitionsEqual(a, b))
	})

	t.Run("array cast plus varchar alias alignment", func(t *testing.T) {
		a := "CHECK ((s)::varchar = ANY ((ARRAY['x'::varchar, 'y'::varchar])::text[]))"
		b := "CHECK ((s)::character varying = ANY (ARRAY[('x'::character varying)::text, ('y'::character varying)::text]))"
		xt.Equal(t, true, d.DefinitionsEqual(a, b))
	})

	t.Run("content difference remains", func(t *testing.T) {
		a := "CREATE INDEX idx ON t USING btree (id)"
		b := "CREATE INDEX idx ON t USING btree (user_id)"
		xt.Equal(t, false, d.DefinitionsEqual(a, b))
	})
}

func TestPgNormalizeExpr_UserCases(t *testing.T) {
	d := &PostgresDialect{}

	cases := []struct {
		name string
		src  string
		dst  string
	}{
		{
			name: "user_agent_file.category_check",
			src:  "CHECK (((category)::text = ANY ((ARRAY['input'::character varying, 'output'::character varying])::text[])))",
			dst:  "CHECK (((category)::text = ANY (ARRAY[('input'::character varying)::text, ('output'::character varying)::text])))",
		},
		{
			name: "user_agent_file.origin_check",
			src:  "CHECK (((origin)::text = ANY ((ARRAY['upload'::character varying, 'generated'::character varying, 'restored'::character varying])::text[])))",
			dst:  "CHECK (((origin)::text = ANY (ARRAY[('upload'::character varying)::text, ('generated'::character varying)::text, ('restored'::character varying)::text])))",
		},
		{
			name: "user_agent_schedule.status_check",
			src:  "CHECK (((status)::text = ANY ((ARRAY['active'::character varying, 'paused'::character varying, 'disabled'::character varying, 'error'::character varying])::text[])))",
			dst:  "CHECK (((status)::text = ANY (ARRAY[('active'::character varying)::text, ('paused'::character varying)::text, ('disabled'::character varying)::text, ('error'::character varying)::text])))",
		},
		{
			name: "user_agent_schedule_run.status_check (5 elements)",
			src:  "CHECK (((status)::text = ANY ((ARRAY['queued'::character varying, 'running'::character varying, 'success'::character varying, 'failed'::character varying, 'skipped'::character varying])::text[])))",
			dst:  "CHECK (((status)::text = ANY (ARRAY[('queued'::character varying)::text, ('running'::character varying)::text, ('success'::character varying)::text, ('failed'::character varying)::text, ('skipped'::character varying)::text])))",
		},
		{
			name: "partial index WHERE clause with same pattern",
			src:  "CREATE INDEX idx ON t USING btree (a, b) WHERE ((c IS NULL) AND ((s)::text = ANY ((ARRAY['x'::character varying, 'y'::character varying])::text[])))",
			dst:  "CREATE INDEX idx ON t USING btree (a, b) WHERE ((c IS NULL) AND ((s)::text = ANY (ARRAY[('x'::character varying)::text, ('y'::character varying)::text])))",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			xt.Equal(t, true, d.DefinitionsEqual(tt.src, tt.dst))
		})
	}
}

func TestPgSplitTopLevelCommas(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{`'a'::text, 'b'::text`, []string{`'a'::text`, ` 'b'::text`}},
		{`'a,b'::text, 'c'::text`, []string{`'a,b'::text`, ` 'c'::text`}},
		{`f(x, y), g(z)`, []string{`f(x, y)`, ` g(z)`}},
		{`ARRAY[1, 2], 3`, []string{`ARRAY[1, 2]`, ` 3`}},
		{`single`, []string{`single`}},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got := pgSplitTopLevelCommas(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("len mismatch: got %d want %d (%v vs %v)", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				xt.Equal(t, tt.want[i], got[i])
			}
		})
	}
}

func TestPgIndexTail(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"basic btree",
			"CREATE INDEX idx_abc ON public.t USING btree (col)",
			"USING btree (col)",
		},
		{
			"partial index with WHERE",
			"CREATE INDEX idx_x ON public.t USING btree (a, b) WHERE (deleted = false)",
			"USING btree (a, b) WHERE (deleted = false)",
		},
		{
			"gin with opclass",
			"CREATE INDEX idx_tsv ON public.t USING gin (tsv)",
			"USING gin (tsv)",
		},
		{
			"temp table probe head is stripped (tail identical)",
			"CREATE INDEX _probe_idx_1 ON pg_temp._probe_tbl_2 USING btree (channel) WHERE (status = 'ok')",
			"USING btree (channel) WHERE (status = 'ok')",
		},
		{
			"no USING clause returns empty",
			"CREATE INDEX idx_x ON t (a)",
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xt.Equal(t, tt.want, pgIndexTail(tt.in))
		})
	}
}

func TestPgProbeName_Unique(t *testing.T) {
	// 确认多次调用产生唯一名字（同一会话内不冲突）
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		n := pgProbeName("x")
		if seen[n] {
			t.Fatalf("duplicate probe name: %s", n)
		}
		seen[n] = true
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
