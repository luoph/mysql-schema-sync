# 幂等可重跑 DDL 生成 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 PG / MySQL 生成的所有 DDL 幂等可重跑，中途被 kill 后直接重跑即可收敛。

**Architecture:** 不改执行 / 事务模型，只改各 `Gen*` 生成点。PG 优先用原生 `IF [NOT] EXISTS`，`ADD CONSTRAINT` 用「先 `DROP CONSTRAINT IF EXISTS` 再 `ADD`」兜底；MySQL `CREATE/DROP TABLE` 用 `IF EXISTS`，`ADD/DROP COLUMN`、`ADD/DROP INDEX`、`ADD/DROP FK` 用查 `information_schema` 的运行时动态 SQL 守卫（`SET`+`PREPARE`+`EXECUTE`+`DEALLOCATE`）。

**Tech Stack:** Go；测试框架 `github.com/xanygo/anygo/xt`（`xt.Equal(t, want, got)`）；表驱动 + golden 文件（`testLoadFile`）。

## Global Constraints

- 始终开启，不引入任何配置开关 / CLI 标志。
- 不在目标库安装存储过程等持久对象。
- 不改执行 / 事务模型，不新增 `BEGIN/COMMIT` 包裹（`TableAlterData.String()` 现有的 BEGIN/COMMIT 保持原样）。
- 提交信息遵循 Conventional Commits，中文，**不含 emoji**；正文结尾加 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。
- 当前分支 `develop`（非 master），无需新建分支。
- 测试命令：`go test ./internal/...`；构建：`go build ./...`。
- MySQL 守卫内嵌 DDL 的字符串转义：`\` → `\\`、`'` → `''`（顺序：先反斜杠后单引号）。
- 守卫使用固定标识符 `@__mss_sql`（会话变量）与 `__mss_stmt`（prepared statement 名）。守卫每条语句结尾带 `;`。
- MySQL `CHANGE COLUMN`、`ALTER ... COMMENT`、表级属性变更**不加**守卫，保持现状。

---

## 文件结构

- `internal/dialect.go` — `Dialect` 接口：`GenAddColumn` / `GenDropColumn` 签名变更。
- `internal/dialect_pg.go` — PG 各 `Gen*` 幂等化 + `pgIndexIfNotExists` helper。
- `internal/dialect_mysql.go` — MySQL `CREATE/DROP TABLE` + 列 / 索引 / FK 守卫 + `mysqlGuard` / `mysqlEscapeSQLLiteral` helper。
- `internal/index.go` — MySQL 索引 / FK 的 add/drop DDL 文本（供守卫包裹）。
- `internal/sync.go` — 列变更路由改走 `classifySQL`；`classifySQL` 前缀扩充；建表分支 CREATE INDEX 幂等；触发器 drop-before-create；列方法新签名调用点。
- 测试与 golden：`internal/dialect_pg_test.go`、`internal/dialect_mysql_test.go`、`internal/sync_test.go`、`internal/testdata/pg/result_*.sql`、`internal/testdata/user/result_1.sql`、`internal/testdata/user/result_2.sql`、新增 `internal/idempotent_test.go`。

---

## Task 1: 列方法接口签名重构（纯重构，无行为变化）

把 `GenAddColumn` / `GenDropColumn` 改为接收 `table` 参数并返回 `[]string`。本任务**不改任何输出**，仅为后续 MySQL 守卫腾出表名与多语句返回能力。所有现有 golden 测试必须保持绿色。

**Files:**
- Modify: `internal/dialect.go:43-53`
- Modify: `internal/dialect_pg.go:584-586`（GenAddColumn）、`629-631`（GenDropColumn）
- Modify: `internal/dialect_mysql.go:238-246`（GenAddColumn）、`256-258`（GenDropColumn）
- Modify: `internal/sync.go:289`、`internal/sync.go:307-333`（legacy 循环）、`internal/sync.go:343`
- Test: `internal/dialect_pg_test.go:647-653`

**Interfaces:**
- Produces:
  - `GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string`
  - `GenDropColumn(table, colName string) []string`

- [ ] **Step 1: 更新 PG 单测以匹配新签名（先红）**

把 `internal/dialect_pg_test.go` 的 `TestPostgresDialect_Misc` 两个子测试改为：

```go
	t.Run("gen drop column", func(t *testing.T) {
		xt.Equal(t, []string{`DROP COLUMN "name"`}, d.GenDropColumn("user", "name"))
	})

	t.Run("gen add column", func(t *testing.T) {
		xt.Equal(t, []string{`ADD COLUMN "name" text NOT NULL`}, d.GenAddColumn("user", `"name" text NOT NULL`, "", false, 0))
	})
```

- [ ] **Step 2: 运行测试确认编译失败**

Run: `go test ./internal/ -run TestPostgresDialect_Misc 2>&1 | head`
Expected: 编译错误（参数 / 返回类型不匹配）。

- [ ] **Step 3: 改接口定义**

`internal/dialect.go`：

```go
	// GenAddColumn generates ADD COLUMN clause(s)
	GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string

	// GenDropColumn generates DROP COLUMN clause(s)
	GenDropColumn(table, colName string) []string
```

- [ ] **Step 4: 改 PG 实现（行为不变，单元素切片）**

`internal/dialect_pg.go`：

```go
func (p *PostgresDialect) GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string {
	return []string{"ADD COLUMN " + colDef}
}
```

```go
func (p *PostgresDialect) GenDropColumn(table, colName string) []string {
	return []string{fmt.Sprintf(`DROP COLUMN "%s"`, colName)}
}
```

- [ ] **Step 5: 改 MySQL 实现（行为不变，单元素切片，暂不加守卫）**

`internal/dialect_mysql.go`：

```go
func (m *MySQLDialect) GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string {
	if afterCol == "" {
		if isFirst {
			return []string{"ADD " + colDef + " FIRST"}
		}
		return []string{"ADD " + colDef}
	}
	return []string{fmt.Sprintf("ADD %s AFTER `%s`", colDef, afterCol)}
}
```

```go
func (m *MySQLDialect) GenDropColumn(table, colName string) []string {
	return []string{fmt.Sprintf("drop `%s`", colName)}
}
```

- [ ] **Step 6: 改 sync.go 三处调用点**

`internal/sync.go:289`（结构化循环 add）：

```go
				newClauses = append(newClauses, d.GenAddColumn(table, colDef, beforeFieldName, fieldCount == 0, fieldCount)...)
```

`internal/sync.go:343`（结构化循环 drop）：

```go
				alterClauses = append(alterClauses, d.GenDropColumn(table, name)...)
```

legacy 文本循环 `internal/sync.go:307-333` 改为收集切片（原来用单个 `alterSQL string`）：

```go
	} else {
		log.Printf("[Debug] Using legacy text-based field comparison for table %s", table)
		for fieldName, value := range sourceMyS.Fields.Iter() {
			if sc.Config.IsIgnoreField(table, fieldName) {
				log.Printf("ignore column %s.%s", table, fieldName)
				continue
			}
			var newClauses []string
			if destDt, has := destMyS.Fields.Get(fieldName); has {
				if value != destDt {
					if s := d.GenChangeColumnText(fieldName, value); s != "" {
						newClauses = append(newClauses, s)
					}
				}
				beforeFieldName = fieldName
			} else {
				newClauses = append(newClauses, d.GenAddColumn(table, value, beforeFieldName, fieldCount == 0, fieldCount)...)
				beforeFieldName = fieldName
			}

			if len(newClauses) != 0 {
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "alterSQL=", newClauses)
				alterClauses = append(alterClauses, newClauses...)
			} else {
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "not change")
			}
			fieldCount++
		}
	}
```

- [ ] **Step 7: 运行受影响测试，确认全绿**

Run: `go build ./... && go test ./internal/ -run 'TestPostgresDialect|TestSchemaSync_getAlterDataBySchema' 2>&1 | tail -20`
Expected: PASS（输出与重构前完全一致）。

- [ ] **Step 8: 全量回归**

Run: `go test ./internal/... 2>&1 | tail -5`
Expected: PASS（`TestWithDB` 因无 env 而 SKIP，属正常）。

- [ ] **Step 9: 提交**

```bash
git add internal/dialect.go internal/dialect_pg.go internal/dialect_mysql.go internal/sync.go internal/dialect_pg_test.go
git commit -m "refactor: 列生成接口改为接收表名并返回 []string

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: classifySQL 路由扩展 + 列变更走 classifySQL（输出中性）

让守卫语句（`SET`/`PREPARE`/`EXECUTE`/`DEALLOCATE`）能被识别为 standalone，并把列变更结果也经 `classifySQL` 分流。本任务对 PG / MySQL 当前输出**无影响**（PG 列子句仍归 alterClauses，MySQL 列子句仍归 alterClauses），golden 保持绿色。

**Files:**
- Modify: `internal/sync.go:717-728`（classifySQL）
- Modify: `internal/sync.go:299-301`（结构化循环 newClauses 收尾）
- Test: `internal/sync_test.go`（新增 `TestClassifySQL`）

**Interfaces:**
- Consumes: `classifySQL(sqls []string, alterClauses, standaloneSQL *[]string)`（已存在）
- Produces: `classifySQL` 额外把以 `SET `/`PREPARE `/`EXECUTE `/`DEALLOCATE ` 开头的语句归入 standalone。

- [ ] **Step 1: 写失败测试**

`internal/sync_test.go` 追加：

```go
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
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run TestClassifySQL -v 2>&1 | tail -20`
Expected: FAIL（`SET`/`PREPARE`/`EXECUTE`/`DEALLOCATE` 当前被归入 alterClauses）。

- [ ] **Step 3: 扩展 classifySQL**

`internal/sync.go` 的 `classifySQL`：

```go
// classifySQL separates standalone SQL from ALTER TABLE clauses
func classifySQL(sqls []string, alterClauses, standaloneSQL *[]string) {
	for _, s := range sqls {
		upper := strings.ToUpper(strings.TrimSpace(s))
		if strings.HasPrefix(upper, "CREATE ") ||
			strings.HasPrefix(upper, "DROP INDEX") ||
			strings.HasPrefix(upper, "DROP TABLE") ||
			strings.HasPrefix(upper, "COMMENT ON") ||
			strings.HasPrefix(upper, "SET ") ||
			strings.HasPrefix(upper, "PREPARE ") ||
			strings.HasPrefix(upper, "EXECUTE ") ||
			strings.HasPrefix(upper, "DEALLOCATE ") {
			*standaloneSQL = append(*standaloneSQL, s)
		} else {
			*alterClauses = append(*alterClauses, s)
		}
	}
}
```

- [ ] **Step 4: 结构化循环的列结果改走 classifySQL**

`internal/sync.go` 结构化循环收尾（原 299-304）：

```go
			if len(newClauses) > 0 {
				classifySQL(newClauses, &alterClauses, &standaloneSQL)
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "alterSQL=", newClauses)
			} else {
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "not change")
			}
```

> 注：`getSchemaDiff` 返回 `(alterClauses, standaloneSQL)`，`standaloneSQL` 变量已在函数内存在（索引 / FK 路径在用）。结构化循环此前直接 append 到 `alterClauses`，改为 `classifySQL` 后 PG / MySQL 的列子句（`ADD`/`DROP`/`CHANGE`/`MODIFY` 开头）仍归 alterClauses，输出不变。

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./internal/ -run TestClassifySQL -v 2>&1 | tail -10`
Expected: PASS。

- [ ] **Step 6: golden 回归**

Run: `go test ./internal/ -run TestSchemaSync_getAlterDataBySchema 2>&1 | tail -10`
Expected: PASS（输出未变）。

- [ ] **Step 7: 提交**

```bash
git add internal/sync.go internal/sync_test.go
git commit -m "refactor: classifySQL 识别守卫语句并让列变更走统一分流

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: PG DROP 侧 IF EXISTS + CREATE/ADD COLUMN 幂等

PG 的简单关键字插入：建表、删表、删列、加列、删索引、删约束、删外键。

**Files:**
- Modify: `internal/dialect_pg.go`：`GenCreateTable`(1164)、`GenDropTable`(1168)、`GenAddColumn`(584)、`GenDropColumn`(629)、`GenDropIndex`(1139)、`GenDropForeignKey`(1160)
- Test: `internal/dialect_pg_test.go`

**Interfaces:**
- Produces（供 Task 6 端到端断言依赖的输出形态）：
  - `GenCreateTable` → `CREATE TABLE IF NOT EXISTS ...;`
  - `GenDropTable` → `DROP TABLE IF EXISTS "t";`
  - `GenAddColumn` → `[]string{"ADD COLUMN IF NOT EXISTS ..."}`
  - `GenDropColumn` → `[]string{`DROP COLUMN IF EXISTS "c"`}`
  - `GenDropIndex`（index）→ `DROP INDEX IF EXISTS "x";`；（约束）→ `DROP CONSTRAINT IF EXISTS "x"`
  - `GenDropForeignKey` → `DROP CONSTRAINT IF EXISTS "x"`

- [ ] **Step 1: 写失败测试**

`internal/dialect_pg_test.go` 追加：

```go
func TestPostgresDialect_Idempotent_DropSide(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("create table if not exists", func(t *testing.T) {
		schema := `CREATE TABLE "test" ("id" integer)`
		xt.Equal(t, `CREATE TABLE IF NOT EXISTS "test" ("id" integer);`, d.GenCreateTable(schema))
	})
	t.Run("drop table if exists", func(t *testing.T) {
		xt.Equal(t, `DROP TABLE IF EXISTS "user";`, d.GenDropTable("user"))
	})
	t.Run("add column if not exists", func(t *testing.T) {
		xt.Equal(t, []string{`ADD COLUMN IF NOT EXISTS "name" text NOT NULL`},
			d.GenAddColumn("user", `"name" text NOT NULL`, "", false, 0))
	})
	t.Run("drop column if exists", func(t *testing.T) {
		xt.Equal(t, []string{`DROP COLUMN IF EXISTS "name"`}, d.GenDropColumn("user", "name"))
	})
	t.Run("drop index if exists", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeIndex, Name: "idx_a"}
		xt.Equal(t, `DROP INDEX IF EXISTS "idx_a";`, d.GenDropIndex("t", idx))
	})
	t.Run("drop constraint if exists", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "pk_test"}
		xt.Equal(t, `DROP CONSTRAINT IF EXISTS "pk_test"`, d.GenDropIndex("t", idx))
	})
	t.Run("drop foreign key if exists", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user"}
		xt.Equal(t, `DROP CONSTRAINT IF EXISTS "fk_user"`, d.GenDropForeignKey("orders", idx))
	})
}
```

并更新 Task 1 中 `TestPostgresDialect_Misc` 的两个子测试与既有 `TestPostgresDialect_GenIndex`/`GenForeignKey`/`Misc` 中受影响断言（见 Step 4）。

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run TestPostgresDialect_Idempotent_DropSide -v 2>&1 | tail -20`
Expected: FAIL。

- [ ] **Step 3: 实现 PG 关键字插入**

`internal/dialect_pg.go`：

```go
func (p *PostgresDialect) GenCreateTable(schema string) string {
	s := strings.TrimSpace(schema)
	if up := strings.ToUpper(s); strings.HasPrefix(up, "CREATE TABLE ") && !strings.HasPrefix(up, "CREATE TABLE IF NOT EXISTS") {
		s = "CREATE TABLE IF NOT EXISTS " + s[len("CREATE TABLE "):]
	}
	return s + ";"
}
```

```go
func (p *PostgresDialect) GenDropTable(tableName string) string {
	return fmt.Sprintf(`DROP TABLE IF EXISTS "%s";`, tableName)
}
```

```go
func (p *PostgresDialect) GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string {
	return []string{"ADD COLUMN IF NOT EXISTS " + colDef}
}
```

```go
func (p *PostgresDialect) GenDropColumn(table, colName string) []string {
	return []string{fmt.Sprintf(`DROP COLUMN IF EXISTS "%s"`, colName)}
}
```

```go
func (p *PostgresDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	switch idx.IndexType {
	case indexTypePrimary, checkConstraint, indexTypeUnique:
		return fmt.Sprintf(`DROP CONSTRAINT IF EXISTS "%s"`, idx.Name)
	case indexTypeIndex:
		return fmt.Sprintf(`DROP INDEX IF EXISTS "%s";`, idx.Name)
	}
	return ""
}
```

```go
func (p *PostgresDialect) GenDropForeignKey(tableName string, idx *DbIndex) string {
	return fmt.Sprintf(`DROP CONSTRAINT IF EXISTS "%s"`, idx.Name)
}
```

- [ ] **Step 4: 更新受牵连的既有 PG 断言**

`internal/dialect_pg_test.go`：

- `TestPostgresDialect_Misc` 的 `gen drop table` → `DROP TABLE IF EXISTS "user";`；`gen create table` → `CREATE TABLE IF NOT EXISTS "test" ("id" integer);`；`gen drop column` → `[]string{`DROP COLUMN IF EXISTS "name"`}`；`gen add column` → `[]string{`ADD COLUMN IF NOT EXISTS "name" text NOT NULL`}`。
- `TestPostgresDialect_GenIndex` 的 `drop primary key` → `DROP CONSTRAINT IF EXISTS "pk_test"`；`full def index with drop first` 的 `sqls[0]` → `DROP INDEX IF EXISTS "idx_user_id";`（`add with drop` 的 `sqls[0]` 留到 Task 4 一并改）。
- `TestPostgresDialect_GenForeignKey` 的 `drop foreign key` → `DROP CONSTRAINT IF EXISTS "fk_user"`。

> `add ...` 系列（GenAddIndex / GenAddForeignKey）的 DROP 前缀与 CREATE INDEX IF NOT EXISTS 在 Task 4 处理，本步只改 drop / create-table / column。

- [ ] **Step 5: 运行 PG 单测**

Run: `go test ./internal/ -run 'TestPostgresDialect_Idempotent_DropSide|TestPostgresDialect_Misc|TestPostgresDialect_GenForeignKey' -v 2>&1 | tail -25`
Expected: PASS。（`TestPostgresDialect_GenIndex` 仍可能因 add 系列未改而部分失败，Task 4 修复。）

- [ ] **Step 6: 提交**

```bash
git add internal/dialect_pg.go internal/dialect_pg_test.go
git commit -m "fix: PG 建表/删表/列/索引/约束 DROP 侧加 IF [NOT] EXISTS

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: PG ADD 侧幂等（CREATE INDEX IF NOT EXISTS + 约束/FK 先 DROP 再 ADD）

PG 的 `ADD CONSTRAINT` 无 `IF NOT EXISTS`，用「总是前置 `DROP CONSTRAINT IF EXISTS`」兜底；普通索引用 `CREATE INDEX IF NOT EXISTS`。

**Files:**
- Modify: `internal/dialect_pg.go`：新增 `pgIndexIfNotExists`，改 `GenAddIndex`(633)、`GenAddForeignKey`(1149)
- Modify: `internal/sync.go:143-148`（建表分支直接拼 `idx.SQL` 的 CREATE INDEX）
- Test: `internal/dialect_pg_test.go`

**Interfaces:**
- Consumes: `GenDropIndex` / `GenDropForeignKey`（Task 3 已为 `IF EXISTS`）
- Produces:
  - `pgIndexIfNotExists(def string) string` — 在 `CREATE [UNIQUE] INDEX` 后插入 `IF NOT EXISTS`（已有则原样返回）
  - `GenAddIndex`（约束类型）→ `[]string{`DROP CONSTRAINT IF EXISTS "x"`, `ADD CONSTRAINT "x" ...`}`
  - `GenAddIndex`（index 类型）→ `CREATE [UNIQUE] INDEX IF NOT EXISTS ... ;`（needDrop 时前置 `DROP INDEX IF EXISTS`）
  - `GenAddForeignKey` → `[]string{`DROP CONSTRAINT IF EXISTS "x"`, `ADD CONSTRAINT "x" FOREIGN KEY ...`}`

- [ ] **Step 1: 写失败测试**

`internal/dialect_pg_test.go` 追加：

```go
func TestPgIndexIfNotExists(t *testing.T) {
	tests := []struct{ in, want string }{
		{"CREATE INDEX idx ON t USING btree (a)", "CREATE INDEX IF NOT EXISTS idx ON t USING btree (a)"},
		{"CREATE UNIQUE INDEX idx ON t USING btree (a)", "CREATE UNIQUE INDEX IF NOT EXISTS idx ON t USING btree (a)"},
		{"CREATE INDEX IF NOT EXISTS idx ON t USING btree (a)", "CREATE INDEX IF NOT EXISTS idx ON t USING btree (a)"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			xt.Equal(t, tt.want, pgIndexIfNotExists(tt.in))
		})
	}
}

func TestPostgresDialect_Idempotent_AddSide(t *testing.T) {
	d := &PostgresDialect{}

	t.Run("add primary key always drops first", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "pk_test", SQL: `PRIMARY KEY ("id")`}
		sqls := d.GenAddIndex("test", idx, false)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP CONSTRAINT IF EXISTS "pk_test"`, sqls[0])
		xt.Equal(t, `ADD CONSTRAINT "pk_test" PRIMARY KEY ("id")`, sqls[1])
	})
	t.Run("add btree index if not exists", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeIndex, Name: "idx_user_id",
			SQL: `CREATE INDEX idx_user_id ON public.t USING btree (user_id)`}
		sqls := d.GenAddIndex("t", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `CREATE INDEX IF NOT EXISTS idx_user_id ON "t" USING btree (user_id);`, sqls[0])
	})
	t.Run("legacy expression index if not exists", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeIndex, Name: "idx_a", SQL: `"a", "b"`}
		sqls := d.GenAddIndex("t", idx, false)
		xt.Equal(t, 1, len(sqls))
		xt.Equal(t, `CREATE INDEX IF NOT EXISTS "idx_a" ON "t" USING btree ("a", "b");`, sqls[0])
	})
	t.Run("add index with drop", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeIndex, Name: "idx_user_id",
			SQL: `CREATE INDEX idx_user_id ON public.t USING btree (user_id)`}
		sqls := d.GenAddIndex("t", idx, true)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP INDEX IF EXISTS "idx_user_id";`, sqls[0])
		xt.Equal(t, `CREATE INDEX IF NOT EXISTS idx_user_id ON "t" USING btree (user_id);`, sqls[1])
	})
	t.Run("add foreign key always drops first", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user",
			SQL: `FOREIGN KEY ("user_id") REFERENCES "user" ("id")`}
		sqls := d.GenAddForeignKey("orders", idx, false)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP CONSTRAINT IF EXISTS "fk_user"`, sqls[0])
		xt.Equal(t, `ADD CONSTRAINT "fk_user" FOREIGN KEY ("user_id") REFERENCES "user" ("id")`, sqls[1])
	})
}
```

并把既有 `TestPostgresDialect_GenIndex` / `TestPostgresDialect_GenForeignKey` 中 `add ...` 子测试改为新预期：

- `add primary key` → 2 条（`DROP CONSTRAINT IF EXISTS "pk_test"`, `ADD CONSTRAINT "pk_test" PRIMARY KEY ("id")`）
- `add unique constraint` → 2 条（`DROP CONSTRAINT IF EXISTS "uq_email"`, `ADD CONSTRAINT "uq_email" UNIQUE ("email")`）
- `add with drop`（PK，needDrop=true）→ 2 条，`sqls[0]` = `DROP CONSTRAINT IF EXISTS "pk_test"`
- `add btree index with full CREATE INDEX def` / `add hnsw index with full def` / `add partial index with WHERE clause` → 在 `CREATE INDEX` 后加 `IF NOT EXISTS`
- `full def index with drop first` → `sqls[0]` = `DROP INDEX IF EXISTS "idx_user_id";`，`sqls[1]` 加 `IF NOT EXISTS`
- `legacy expression-only index def...` → `CREATE INDEX IF NOT EXISTS "idx_a" ON "t" USING btree ("a", "b");`
- `TestPostgresDialect_GenForeignKey` 的 `add foreign key` → 2 条；`add with drop` → 2 条，`sqls[0]` = `DROP CONSTRAINT IF EXISTS "fk_user"`

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run 'TestPgIndexIfNotExists|TestPostgresDialect_Idempotent_AddSide' -v 2>&1 | tail -20`
Expected: FAIL。

- [ ] **Step 3: 新增 helper**

`internal/dialect_pg.go`（放在 `pgRewriteIndexTable` 附近）：

```go
// pgIndexIfNotExists 在 "CREATE INDEX" / "CREATE UNIQUE INDEX" 之后插入 "IF NOT EXISTS"，
// 已含则原样返回。仅作用于语句开头，不影响 USING method / 列表达式 / WHERE。
var pgCreateIndexHeadReg = regexp.MustCompile(`(?i)^(CREATE\s+(?:UNIQUE\s+)?INDEX)\s+`)

func pgIndexIfNotExists(def string) string {
	s := strings.TrimSpace(def)
	if regexp.MustCompile(`(?i)^CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS`).MatchString(s) {
		return s
	}
	return pgCreateIndexHeadReg.ReplaceAllString(s, "$1 IF NOT EXISTS ")
}
```

- [ ] **Step 4: 改 GenAddIndex**

`internal/dialect_pg.go` 的 `GenAddIndex`：

```go
func (p *PostgresDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string

	// 约束类（PK/UNIQUE/CHECK）：PG 无 ADD CONSTRAINT IF NOT EXISTS，总是前置
	// DROP CONSTRAINT IF EXISTS 兜底幂等；普通索引用 CREATE INDEX IF NOT EXISTS。
	switch idx.IndexType {
	case indexTypePrimary, indexTypeUnique, checkConstraint:
		if dropSQL := p.GenDropIndex(tableName, idx); dropSQL != "" {
			sqls = append(sqls, dropSQL)
		}
		defSQL := pgConstraintPrefixRe.ReplaceAllString(idx.SQL, "")
		sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, defSQL))
	case indexTypeIndex:
		if needDrop {
			if dropSQL := p.GenDropIndex(tableName, idx); dropSQL != "" {
				sqls = append(sqls, dropSQL)
			}
		}
		defSQL := pgConstraintPrefixRe.ReplaceAllString(idx.SQL, "")
		upperDef := strings.ToUpper(strings.TrimSpace(defSQL))
		if strings.HasPrefix(upperDef, "CREATE INDEX") || strings.HasPrefix(upperDef, "CREATE UNIQUE INDEX") {
			sqls = append(sqls, ensureSemicolon(pgIndexIfNotExists(pgRewriteIndexTable(defSQL, tableName))))
		} else {
			sqls = append(sqls, fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %q ON "%s" USING btree (%s);`, idx.Name, tableName, defSQL))
		}
	}

	return sqls
}
```

> 注意：原实现里 `needDrop` 时对所有类型前置 drop。新实现中约束类**无条件**前置 drop（满足幂等），index 类仅 needDrop 时前置；行为对约束类是「总是 drop+add」，对 index 类与原先一致（仅多了 IF NOT EXISTS）。

- [ ] **Step 5: 改 GenAddForeignKey**

```go
func (p *PostgresDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	// FK 同约束：总是前置 DROP CONSTRAINT IF EXISTS 兜底幂等（needDrop 参数不再影响是否 drop）。
	sqls = append(sqls, p.GenDropForeignKey(tableName, idx))
	defSQL := pgConstraintPrefixRe.ReplaceAllString(idx.SQL, "")
	sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, defSQL))
	return sqls
}
```

- [ ] **Step 6: 改建表分支的 CREATE INDEX（sync.go）**

`internal/sync.go` 的 `getAlterDataBySchema` 建表分支（原 143-148），把直接拼 `idx.SQL` 改为经 `pgIndexIfNotExists`：

```go
				if strings.HasPrefix(upperDef, "CREATE INDEX") || strings.HasPrefix(upperDef, "CREATE UNIQUE INDEX") {
					alter.SQL = append(alter.SQL, ensureSemicolon(pgIndexIfNotExists(strings.TrimRight(idx.SQL, ";"))))
					if idxCommenter != nil && idx.Comment != "" {
						alter.SQL = append(alter.SQL, idxCommenter.GenCommentIndexSQL(idx.Name, idx.Comment))
					}
				}
```

> `pgIndexIfNotExists` 与 `ensureSemicolon` 均在 `internal` 包内，sync.go 可直接调用。MySQL 走不到此分支（仅 PG 的 `IndexAll` 含完整 `CREATE INDEX` 文本）。

- [ ] **Step 7: 运行 PG 单测全绿**

Run: `go test ./internal/ -run 'TestPgIndexIfNotExists|TestPostgresDialect_Idempotent_AddSide|TestPostgresDialect_GenIndex|TestPostgresDialect_GenForeignKey' -v 2>&1 | tail -30`
Expected: PASS。

- [ ] **Step 8: 提交**

```bash
git add internal/dialect_pg.go internal/dialect_pg_test.go internal/sync.go
git commit -m "fix: PG 普通索引 CREATE IF NOT EXISTS、约束/FK 先 DROP IF EXISTS 再 ADD

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: PG 触发器 drop-before-create

新增触发器也要可重跑：`CREATE TRIGGER` 前置 `DROP TRIGGER IF EXISTS`。

**Files:**
- Modify: `internal/sync.go`：`diffTriggers`(537-566)；建表分支触发器 emit（151-155）
- Test: `internal/dialect_pg_test.go`（`TestSchemaSync_diffTriggers`）

**Interfaces:**
- Consumes: `TriggerEnumerator.GenDropTrigger`（已是 `DROP TRIGGER IF EXISTS ... ;`）、`GenAddTrigger`
- Produces: `diffTriggers` 对「源新增」也输出 `[DROP TRIGGER IF EXISTS, CREATE TRIGGER]`

- [ ] **Step 1: 改测试预期（先红）**

`internal/dialect_pg_test.go` 的 `TestSchemaSync_diffTriggers` 中 `source add` 子测试改为：

```go
	t.Run("source add", func(t *testing.T) {
		alter := &TableAlterData{
			SchemaDiff: &SchemaDiff{
				Source: &MySchema{Triggers: map[string]*DbTrigger{"a": trg("a", "CREATE TRIGGER a ...")}},
				Dest:   &MySchema{},
			},
		}
		sqls := mkSC(cfgNoDrop).diffTriggers(alter)
		xt.Equal(t, 2, len(sqls))
		xt.Equal(t, `DROP TRIGGER IF EXISTS "a" ON "t";`, sqls[0])
		xt.Equal(t, "CREATE TRIGGER a ...;", sqls[1])
	})
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run TestSchemaSync_diffTriggers -v 2>&1 | tail -15`
Expected: FAIL（`source add` 现为 1 条）。

- [ ] **Step 3: 改 diffTriggers**

`internal/sync.go` 的 `diffTriggers` 主循环（原 547-556）：

```go
	for name, src := range source {
		dst, has := dest[name]
		if has && sc.definitionsEqual(src.Definition, dst.Definition) {
			continue
		}
		// 幂等：无论目标是否已有同名触发器，CREATE 前一律 DROP IF EXISTS，
		// 让脚本可重跑（PG 无 CREATE TRIGGER IF NOT EXISTS）。
		if has {
			sqls = append(sqls, te.GenDropTrigger(dst))
		} else {
			sqls = append(sqls, te.GenDropTrigger(src))
		}
		sqls = append(sqls, te.GenAddTrigger(src))
	}
```

> `GenDropTrigger` 用 `trg.Name` + `trg.Table` 拼 `DROP TRIGGER IF EXISTS`；`src` 与 `dst` 同名同表，用哪个都可，无 dst 时用 src。

- [ ] **Step 4: 建表分支触发器也前置 DROP**

`internal/sync.go` 建表分支（原 151-155）：

```go
			if te, ok := d.(TriggerEnumerator); ok {
				for _, trg := range alter.SchemaDiff.Source.Triggers {
					alter.SQL = append(alter.SQL, te.GenDropTrigger(trg))
					alter.SQL = append(alter.SQL, te.GenAddTrigger(trg))
				}
			}
```

- [ ] **Step 5: 运行测试全绿**

Run: `go test ./internal/ -run TestSchemaSync_diffTriggers -v 2>&1 | tail -15`
Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add internal/sync.go internal/dialect_pg_test.go
git commit -m "fix: PG 触发器新增前置 DROP TRIGGER IF EXISTS 以可重跑

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: 重新生成 PG golden 集成文件

PG 端到端 golden（`result_1` / `result_2` / `result_constraint_rename`）因幂等化而变化，重新核对生成。

**Files:**
- Modify: `internal/testdata/pg/result_1.sql`、`internal/testdata/pg/result_2.sql`、`internal/testdata/pg/result_constraint_rename.sql`

- [ ] **Step 1: 跑集成测试看实际输出**

Run: `go test ./internal/ -run TestPostgresDialect_getAlterDataBySchema -v 2>&1 | sed -n '1,80p'`
Expected: FAIL，日志里 `got alter:` 段落是新的幂等输出。

- [ ] **Step 2: 逐个核对并写入 golden**

对每个失败用例，确认 `got` 内容满足：
- `result_1`（user_0→user_1，加列）：每个 `ADD COLUMN` 均为 `ALTER TABLE "user" ADD COLUMN IF NOT EXISTS ...;`。
- `result_constraint_rename`（user_audio，Drop=true，约束改名）：旧约束 `ALTER TABLE "user_audio" DROP CONSTRAINT IF EXISTS "<old>";`，新约束 `ALTER TABLE "user_audio" DROP CONSTRAINT IF EXISTS "<new>"; ALTER TABLE "user_audio" ADD CONSTRAINT "<new>" ...;`。

把每个用例的 `got`（`got.String()` 的 `strings.TrimSpace` 结果）原样写入对应 golden 文件。可用如下方式取得精确文本：临时在测试里把 `xt.Equal` 换成 `os.WriteFile("testdata/pg/result_1.sql", []byte(got.String()+"\n"), 0644)` 跑一次后改回——或手工依据日志誊写。誊写后务必人工核对无裸 `CREATE`/裸 `ADD CONSTRAINT`/裸 `DROP`。

- [ ] **Step 3: 运行确认通过**

Run: `go test ./internal/ -run TestPostgresDialect_getAlterDataBySchema -v 2>&1 | tail -15`
Expected: PASS。

- [ ] **Step 4: 提交**

```bash
git add internal/testdata/pg/result_1.sql internal/testdata/pg/result_2.sql internal/testdata/pg/result_constraint_rename.sql
git commit -m "test: 更新 PG golden 至幂等 DDL 输出

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: MySQL CREATE/DROP TABLE IF EXISTS

**Files:**
- Modify: `internal/dialect_mysql.go`：`GenCreateTable`(278)、`GenDropTable`(282)
- Test: `internal/dialect_mysql_test.go`

**Interfaces:**
- Produces: `GenCreateTable` → `CREATE TABLE IF NOT EXISTS ...;`；`GenDropTable` → `` DROP TABLE IF EXISTS `t`; ``

- [ ] **Step 1: 写失败测试**

`internal/dialect_mysql_test.go` 追加：

```go
func TestMySQLDialect_Idempotent_Table(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("create table if not exists keeps auto_increment strip", func(t *testing.T) {
		in := "CREATE TABLE `t` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4"
		out := d.GenCreateTable(in)
		xt.Equal(t, true, strings.HasPrefix(out, "CREATE TABLE IF NOT EXISTS `t`"))
		xt.Equal(t, false, strings.Contains(out, "AUTO_INCREMENT=9"))
		xt.Equal(t, true, strings.HasSuffix(out, ";"))
	})
	t.Run("drop table if exists", func(t *testing.T) {
		xt.Equal(t, "DROP TABLE IF EXISTS `user`;", d.GenDropTable("user"))
	})
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run TestMySQLDialect_Idempotent_Table -v 2>&1 | tail -15`
Expected: FAIL。

- [ ] **Step 3: 实现**

`internal/dialect_mysql.go`：

```go
func (m *MySQLDialect) GenCreateTable(schema string) string {
	s := mysqlAutoIncrReg.ReplaceAllString(schema, " ")
	if up := strings.ToUpper(strings.TrimSpace(s)); strings.HasPrefix(up, "CREATE TABLE ") && !strings.HasPrefix(up, "CREATE TABLE IF NOT EXISTS") {
		trimmed := strings.TrimSpace(s)
		s = "CREATE TABLE IF NOT EXISTS " + trimmed[len("CREATE TABLE "):]
	}
	return s + ";"
}
```

```go
func (m *MySQLDialect) GenDropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", tableName)
}
```

- [ ] **Step 4: 运行确认通过**

Run: `go test ./internal/ -run 'TestMySQLDialect_Idempotent_Table|Test_fmtTableCreateSQL' -v 2>&1 | tail -15`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add internal/dialect_mysql.go internal/dialect_mysql_test.go
git commit -m "fix: MySQL CREATE/DROP TABLE 加 IF [NOT] EXISTS

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: MySQL 守卫 helper + ADD/DROP COLUMN 守卫

实现运行时存在性守卫，并改写 MySQL 列的 add/drop 为守卫块。

**Files:**
- Modify: `internal/dialect_mysql.go`：新增 `mysqlEscapeSQLLiteral`、`mysqlGuard`；改 `GenAddColumn`、`GenDropColumn`
- Test: `internal/dialect_mysql_test.go`
- Modify: `internal/testdata/user/result_1.sql`、`internal/testdata/user/result_2.sql`

**Interfaces:**
- Produces:
  - `mysqlEscapeSQLLiteral(s string) string` — `\` → `\\`，再 `'` → `''`
  - `mysqlGuard(probeFrom, probeWhere, ddl string, runWhenExists bool) []string` — 返回 4 条带 `;` 的语句
  - `GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string` — 守卫块（probe `information_schema.COLUMNS`，runWhenExists=false）
  - `GenDropColumn(table, colName string) []string` — 守卫块（runWhenExists=true）

- [ ] **Step 1: 写失败测试**

`internal/dialect_mysql_test.go` 追加：

```go
func TestMySQLEscapeSQLLiteral(t *testing.T) {
	xt.Equal(t, "a''b", mysqlEscapeSQLLiteral("a'b"))
	xt.Equal(t, `a\\b`, mysqlEscapeSQLLiteral(`a\b`))
	xt.Equal(t, `\\''`, mysqlEscapeSQLLiteral(`\'`))
}

func TestMySQLGuard(t *testing.T) {
	t.Run("add semantics uses COUNT=0", func(t *testing.T) {
		got := mysqlGuard("information_schema.COLUMNS",
			"TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND COLUMN_NAME='c'",
			"ALTER TABLE `t` ADD `c` int", false)
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `t` ADD `c` int', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND COLUMN_NAME='c');",
			got[0])
		xt.Equal(t, "PREPARE __mss_stmt FROM @__mss_sql;", got[1])
		xt.Equal(t, "EXECUTE __mss_stmt;", got[2])
		xt.Equal(t, "DEALLOCATE PREPARE __mss_stmt;", got[3])
	})
	t.Run("drop semantics uses COUNT>0 and escapes ddl", func(t *testing.T) {
		got := mysqlGuard("information_schema.STATISTICS",
			"TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND INDEX_NAME='idx'",
			"ALTER TABLE `t` DROP INDEX `idx`", true)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `t` DROP INDEX `idx`', 'SELECT 1') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND INDEX_NAME='idx');",
			got[0])
	})
}

func TestMySQLDialect_GuardColumns(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("add column guarded", func(t *testing.T) {
		got := d.GenAddColumn("user", "`age` int NOT NULL", "email", false, 1)
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `age` int NOT NULL AFTER `email`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='age');",
			got[0])
	})
	t.Run("add first column guarded", func(t *testing.T) {
		got := d.GenAddColumn("user", "`age` int NOT NULL", "", true, 0)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `age` int NOT NULL FIRST', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='age');",
			got[0])
	})
	t.Run("drop column guarded", func(t *testing.T) {
		got := d.GenDropColumn("user", "age")
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `user` DROP COLUMN `age`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='age');",
			got[0])
	})
}
```

> 列名探测从 `colDef` 提取：`colDef` 形如 `` `age` int NOT NULL ``，取第一对反引号内文本为列名。

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run 'TestMySQLEscapeSQLLiteral|TestMySQLGuard|TestMySQLDialect_GuardColumns' -v 2>&1 | tail -25`
Expected: FAIL（函数未定义）。

- [ ] **Step 3: 实现 helper**

`internal/dialect_mysql.go`（顶部按需补 import `strings`，已存在）：

```go
// mysqlEscapeSQLLiteral 转义将被内嵌进单引号字符串字面量的 DDL：先转义反斜杠，
// 再转义单引号（顺序不可换，避免二次转义）。
func mysqlEscapeSQLLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

// mysqlGuard 生成运行时存在性守卫块：仅当 information_schema 探测计数满足条件时执行
// ddl，否则执行无害 'SELECT 1'。返回 4 条带分号的独立语句。
// probeFrom 形如 "information_schema.COLUMNS"；probeWhere 为不含 WHERE 关键字、已含
// TABLE_SCHEMA=DATABASE() 的条件。runWhenExists=false → COUNT(*)=0 才执行（ADD 语义），
// true → COUNT(*)>0 才执行（DROP 语义）。
func mysqlGuard(probeFrom, probeWhere, ddl string, runWhenExists bool) []string {
	cmp := "=0"
	if runWhenExists {
		cmp = ">0"
	}
	set := fmt.Sprintf("SET @__mss_sql = (SELECT IF(COUNT(*)%s, '%s', 'SELECT 1') FROM %s WHERE %s);",
		cmp, mysqlEscapeSQLLiteral(ddl), probeFrom, probeWhere)
	return []string{
		set,
		"PREPARE __mss_stmt FROM @__mss_sql;",
		"EXECUTE __mss_stmt;",
		"DEALLOCATE PREPARE __mss_stmt;",
	}
}

// mysqlColumnName 从列定义（如 "`age` int NOT NULL"）取第一对反引号内的列名。
func mysqlColumnName(colDef string) string {
	s := strings.TrimSpace(colDef)
	if len(s) > 0 && s[0] == '`' {
		if i := strings.IndexByte(s[1:], '`'); i >= 0 {
			return s[1 : i+1]
		}
	}
	return s
}
```

- [ ] **Step 4: 改 GenAddColumn / GenDropColumn**

```go
func (m *MySQLDialect) GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string {
	var ddl string
	switch {
	case afterCol == "" && isFirst:
		ddl = fmt.Sprintf("ALTER TABLE `%s` ADD %s FIRST", table, colDef)
	case afterCol == "":
		ddl = fmt.Sprintf("ALTER TABLE `%s` ADD %s", table, colDef)
	default:
		ddl = fmt.Sprintf("ALTER TABLE `%s` ADD %s AFTER `%s`", table, colDef, afterCol)
	}
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", table, mysqlColumnName(colDef))
	return mysqlGuard("information_schema.COLUMNS", where, ddl, false)
}

func (m *MySQLDialect) GenDropColumn(table, colName string) []string {
	ddl := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", table, colName)
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", table, colName)
	return mysqlGuard("information_schema.COLUMNS", where, ddl, true)
}
```

> 旧 `GenDropColumn` 输出 `` drop `x` `` 子句（依赖 WrapAlterSQL 套 `ALTER TABLE`）；新实现直接给出完整 `ALTER TABLE ... DROP COLUMN`，并经守卫包裹。

- [ ] **Step 5: 运行 helper / 列守卫单测全绿**

Run: `go test ./internal/ -run 'TestMySQLEscapeSQLLiteral|TestMySQLGuard|TestMySQLDialect_GuardColumns' -v 2>&1 | tail -20`
Expected: PASS。

- [ ] **Step 6: 重新生成 MySQL 列 golden**

`internal/testdata/user/result_1.sql` 与 `result_2.sql`（同一加列场景，守卫化后两者相同）改为：

```sql
-- Table : user
-- Type : alter
BEGIN;
SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `register_time` timestamp NOT NULL AFTER `email`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='register_time');
PREPARE __mss_stmt FROM @__mss_sql;
EXECUTE __mss_stmt;
DEALLOCATE PREPARE __mss_stmt;
SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `password` varchar(1000) NOT NULL DEFAULT '''' AFTER `register_time`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='password');
PREPARE __mss_stmt FROM @__mss_sql;
EXECUTE __mss_stmt;
DEALLOCATE PREPARE __mss_stmt;
SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `status` tinyint unsigned NOT NULL DEFAULT ''0'' AFTER `password`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='status');
PREPARE __mss_stmt FROM @__mss_sql;
EXECUTE __mss_stmt;
DEALLOCATE PREPARE __mss_stmt;
COMMIT;
```

> `password`/`status` 的默认值 `''`/`'0'` 在内嵌字面量里被转义为 `''''`/`''0''`（外层单引号 + 内层双写）。务必先跑测试看 `got` 实际文本再誊写，避免转义层数算错。

- [ ] **Step 7: 运行 MySQL 集成 golden 全绿**

Run: `go test ./internal/ -run 'TestSchemaSync_getAlterDataBySchema' -v 2>&1 | tail -20`
Expected: PASS（`user 0-1`、`user 0-1 ssc` 用新 golden；`user 1-0 ssc`(result_3 不变)、`user 2-0 ssc`(result_4 CHANGE 不变) 仍绿）。

- [ ] **Step 8: 提交**

```bash
git add internal/dialect_mysql.go internal/dialect_mysql_test.go internal/testdata/user/result_1.sql internal/testdata/user/result_2.sql
git commit -m "fix: MySQL 列 ADD/DROP 用动态 SQL 守卫实现幂等可重跑

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: MySQL 索引 / 外键守卫（含 PRIMARY KEY）

把 MySQL 的 `ADD/DROP INDEX`、`ADD/DROP FOREIGN KEY` 改为守卫块。

**Files:**
- Modify: `internal/dialect_mysql.go`：`GenAddIndex`(260)、`GenDropIndex`(264)、`GenAddForeignKey`(268)、`GenDropForeignKey`(272)
- Modify: `internal/index.go`：新增导出索引 / FK 的「裸 DDL 文本」辅助（add 用 `idx.SQL`，drop 用类型分派），供守卫复用
- Test: `internal/dialect_mysql_test.go`

**Interfaces:**
- Consumes: `mysqlGuard`、`DbIndex`（`idx.IndexType` / `idx.Name` / `idx.SQL`）
- Produces:
  - `GenAddIndex` → 守卫块（probe `STATISTICS`，PK 用 `INDEX_NAME='PRIMARY'`，其余 `INDEX_NAME='<name>'`，runWhenExists=false）；needDrop 时整组前置一份 drop 守卫
  - `GenDropIndex` → 守卫块（runWhenExists=true）
  - `GenAddForeignKey` → 守卫块（probe `TABLE_CONSTRAINTS`，`CONSTRAINT_TYPE='FOREIGN KEY'`，runWhenExists=false）
  - `GenDropForeignKey` → 守卫块（runWhenExists=true）

- [ ] **Step 1: 写失败测试**

`internal/dialect_mysql_test.go` 追加：

```go
func TestMySQLDialect_GuardIndexes(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("add unique index guarded", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeUnique, Name: "uk_email", SQL: "UNIQUE KEY `uk_email` (`email`)"}
		got := d.GenAddIndex("user", idx, false)
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD UNIQUE KEY `uk_email` (`email`)', 'SELECT 1') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND INDEX_NAME='uk_email');",
			got[0])
	})
	t.Run("drop index guarded", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeIndex, Name: "idx_code"}
		got := d.GenDropIndex("user", idx)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `user` DROP INDEX `idx_code`', 'SELECT 1') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND INDEX_NAME='idx_code');",
			got[0])
	})
	t.Run("add primary key probes PRIMARY", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "PRIMARY KEY", SQL: "PRIMARY KEY (`id`)"}
		got := d.GenAddIndex("user", idx, false)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD PRIMARY KEY (`id`)', 'SELECT 1') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND INDEX_NAME='PRIMARY');",
			got[0])
	})
	t.Run("drop primary key probes PRIMARY", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypePrimary, Name: "PRIMARY KEY"}
		got := d.GenDropIndex("user", idx)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `user` DROP PRIMARY KEY', 'SELECT 1') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND INDEX_NAME='PRIMARY');",
			got[0])
	})
	t.Run("add index with drop prepends drop guard", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeUnique, Name: "uk_email", SQL: "UNIQUE KEY `uk_email` (`email`)"}
		got := d.GenAddIndex("user", idx, true)
		xt.Equal(t, 8, len(got)) // drop 守卫 4 + add 守卫 4
		xt.Equal(t, true, strings.Contains(got[0], "IF(COUNT(*)>0"))
		xt.Equal(t, true, strings.Contains(got[4], "IF(COUNT(*)=0"))
	})
}

func TestMySQLDialect_GuardForeignKeys(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("add fk guarded", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user",
			SQL: "CONSTRAINT `fk_user` FOREIGN KEY (`uid`) REFERENCES `user` (`id`)"}
		got := d.GenAddForeignKey("orders", idx, false)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `orders` ADD CONSTRAINT `fk_user` FOREIGN KEY (`uid`) REFERENCES `user` (`id`)', 'SELECT 1') FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='orders' AND CONSTRAINT_NAME='fk_user' AND CONSTRAINT_TYPE='FOREIGN KEY');",
			got[0])
	})
	t.Run("drop fk guarded", func(t *testing.T) {
		idx := &DbIndex{IndexType: indexTypeForeignKey, Name: "fk_user"}
		got := d.GenDropForeignKey("orders", idx)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `orders` DROP FOREIGN KEY `fk_user`', 'SELECT 1') FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='orders' AND CONSTRAINT_NAME='fk_user' AND CONSTRAINT_TYPE='FOREIGN KEY');",
			got[0])
	})
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./internal/ -run 'TestMySQLDialect_GuardIndexes|TestMySQLDialect_GuardForeignKeys' -v 2>&1 | tail -25`
Expected: FAIL。

- [ ] **Step 3: 新增 index.go 裸 DDL 文本辅助**

`internal/index.go` 追加（供 MySQL 守卫复用，纯文本拼接，不含 `ALTER TABLE` 前缀）：

```go
// mysqlAddBody 返回 ADD 子句体（不含 "ALTER TABLE `t` " 前缀）。
func (idx *DbIndex) mysqlAddBody() string {
	return "ADD " + idx.SQL
}

// mysqlDropBody 返回 DROP 子句体（不含 "ALTER TABLE `t` " 前缀）。
func (idx *DbIndex) mysqlDropBody() string {
	switch idx.IndexType {
	case indexTypePrimary:
		return "DROP PRIMARY KEY"
	case indexTypeIndex, indexTypeUnique:
		return fmt.Sprintf("DROP INDEX `%s`", idx.Name)
	case indexTypeForeignKey:
		return fmt.Sprintf("DROP FOREIGN KEY `%s`", idx.Name)
	case checkConstraint:
		return fmt.Sprintf("DROP CHECK `%s`", idx.Name)
	}
	return ""
}

// mysqlIndexProbeName 返回 information_schema.STATISTICS.INDEX_NAME 的探测值：
// 主键固定为 'PRIMARY'，其余用索引名。
func (idx *DbIndex) mysqlIndexProbeName() string {
	if idx.IndexType == indexTypePrimary {
		return "PRIMARY"
	}
	return idx.Name
}
```

- [ ] **Step 4: 改 MySQL 索引 / FK 生成**

`internal/dialect_mysql.go`：

```go
func (m *MySQLDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	if needDrop {
		sqls = append(sqls, m.GenDropIndexGuard(tableName, idx)...)
	}
	ddl := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, idx.mysqlAddBody())
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND INDEX_NAME='%s'", tableName, idx.mysqlIndexProbeName())
	return append(sqls, mysqlGuard("information_schema.STATISTICS", where, ddl, false)...)
}

// GenDropIndexGuard 是 GenDropIndex 的内部复用版本（GenDropIndex 接口返回 string，
// 守卫需返回多条，故单列一个 []string 版本）。
func (m *MySQLDialect) GenDropIndexGuard(tableName string, idx *DbIndex) []string {
	ddl := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, idx.mysqlDropBody())
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND INDEX_NAME='%s'", tableName, idx.mysqlIndexProbeName())
	return mysqlGuard("information_schema.STATISTICS", where, ddl, true)
}

func (m *MySQLDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	if needDrop {
		sqls = append(sqls, m.GenDropForeignKeyGuard(tableName, idx)...)
	}
	ddl := fmt.Sprintf("ALTER TABLE `%s` %s", tableName, idx.mysqlAddBody())
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND CONSTRAINT_NAME='%s' AND CONSTRAINT_TYPE='FOREIGN KEY'", tableName, idx.Name)
	return append(sqls, mysqlGuard("information_schema.TABLE_CONSTRAINTS", where, ddl, false)...)
}

func (m *MySQLDialect) GenDropForeignKeyGuard(tableName string, idx *DbIndex) []string {
	ddl := fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`", tableName, idx.Name)
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND CONSTRAINT_NAME='%s' AND CONSTRAINT_TYPE='FOREIGN KEY'", tableName, idx.Name)
	return mysqlGuard("information_schema.TABLE_CONSTRAINTS", where, ddl, true)
}
```

接口方法 `GenDropIndex` / `GenDropForeignKey` 仍需返回 `string`（接口约定），但 MySQL 走守卫需多条。处理：让接口方法返回**整段守卫拼接的多语句单串**（用 `\n` 连接），由 `classifySQL` 按首词 `SET` 归 standalone，`String()` 渲染时本就 `\n` 连接，执行入口 `SyncSQL4DestInTx` 按元素执行——但**单串多语句会被 `tx.Query` 当一条执行而失败**。

因此改 `sync.go` 中 drop 索引 / drop 外键的调用点，让其使用守卫的 `[]string` 版本而非接口 string 版本：

- `internal/sync.go` drop index 段（原 399-413）：

```go
	if sc.Config.Drop {
		for indexName, dIdx := range destMyS.IndexAll {
			if sc.Config.IsIgnoreIndex(table, indexName) {
				log.Printf("ignore index %s.%s", table, indexName)
				continue
			}
			if _, has := sourceMyS.IndexAll[indexName]; !has {
				dropSQLs := d.GenDropIndexMulti(table, dIdx)
				if len(dropSQLs) > 0 {
					classifySQL(dropSQLs, &alterClauses, &standaloneSQL)
					log.Println("[Debug] check index.drop ", fmt.Sprintf("%s.%s", table, indexName), "sql=", dropSQLs)
				}
			}
		}
	}
```

- drop foreign key 段（原 439-454）同理改用 `d.GenDropForeignKeyMulti(table, dIdx)`。

- [ ] **Step 5: 引入 `GenDropIndexMulti` / `GenDropForeignKeyMulti` 到接口**

为统一两 dialect，给 `Dialect` 接口补两个返回 `[]string` 的删除方法，并保留原 string 版（仍被 PG 的 `GenAddIndex`/`GenAddForeignKey` 内部复用）：

`internal/dialect.go` 接口追加：

```go
	// GenDropIndexMulti 返回幂等删除索引/约束的语句组（PG 单元素，MySQL 为守卫块）
	GenDropIndexMulti(tableName string, idx *DbIndex) []string

	// GenDropForeignKeyMulti 返回幂等删除外键的语句组
	GenDropForeignKeyMulti(tableName string, idx *DbIndex) []string
```

PG 实现（`internal/dialect_pg.go`）——单元素包装，复用既有 string 版：

```go
func (p *PostgresDialect) GenDropIndexMulti(tableName string, idx *DbIndex) []string {
	if s := p.GenDropIndex(tableName, idx); s != "" {
		return []string{s}
	}
	return nil
}

func (p *PostgresDialect) GenDropForeignKeyMulti(tableName string, idx *DbIndex) []string {
	return []string{p.GenDropForeignKey(tableName, idx)}
}
```

MySQL 实现（`internal/dialect_mysql.go`）——即上面的 guard 版：

```go
func (m *MySQLDialect) GenDropIndexMulti(tableName string, idx *DbIndex) []string {
	return m.GenDropIndexGuard(tableName, idx)
}

func (m *MySQLDialect) GenDropForeignKeyMulti(tableName string, idx *DbIndex) []string {
	return m.GenDropForeignKeyGuard(tableName, idx)
}
```

> 保留原 `GenDropIndex` / `GenDropForeignKey`（string 版）不动：PG 的 `GenAddIndex` 内部仍调用 string 版；MySQL 的 string 版此后不再被 sync.go 调用（drop 路径已切到 `*Multi`），但仍满足接口、可被测试直接断言（其返回守卫块首条亦可——为简单起见 MySQL 的 string 版返回 `strings.Join(guard, "\n")`，仅作兼容，不进执行路径）。

  实现 MySQL string 版兼容：
```go
func (m *MySQLDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	return strings.Join(m.GenDropIndexGuard(tableName, idx), "\n")
}
func (m *MySQLDialect) GenDropForeignKey(tableName string, idx *DbIndex) string {
	return strings.Join(m.GenDropForeignKeyGuard(tableName, idx), "\n")
}
```

- [ ] **Step 6: 运行索引 / FK 守卫单测全绿**

Run: `go test ./internal/ -run 'TestMySQLDialect_GuardIndexes|TestMySQLDialect_GuardForeignKeys' -v 2>&1 | tail -25`
Expected: PASS。

- [ ] **Step 7: 全量回归**

Run: `go build ./... && go test ./internal/... 2>&1 | tail -10`
Expected: PASS（`TestWithDB` SKIP）。

- [ ] **Step 8: 提交**

```bash
git add internal/dialect.go internal/dialect_pg.go internal/dialect_mysql.go internal/index.go internal/sync.go internal/dialect_mysql_test.go
git commit -m "fix: MySQL 索引/外键 ADD/DROP 用动态 SQL 守卫实现幂等可重跑

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: 端到端「无裸 DDL」断言

加一个跨 dialect 的回归测试，确保整表生成的 SQL 序列不含裸 `CREATE TABLE` / 裸 `CREATE INDEX` / 裸 `ADD CONSTRAINT` / 无 `IF EXISTS` 的 `DROP`。

**Files:**
- Create: `internal/idempotent_test.go`

- [ ] **Step 1: 写测试**

```go
package internal

import (
	"strings"
	"testing"

	"github.com/xanygo/anygo/xt"
)

// 断言一段生成的 SQL 文本不含裸 DDL（即均已幂等化）。
func assertNoBareDDL(t *testing.T, sql string) {
	t.Helper()
	for _, line := range strings.Split(sql, "\n") {
		l := strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ";")))
		switch {
		case strings.HasPrefix(l, "CREATE TABLE ") && !strings.HasPrefix(l, "CREATE TABLE IF NOT EXISTS"):
			t.Errorf("bare CREATE TABLE: %q", line)
		case (strings.HasPrefix(l, "CREATE INDEX ") || strings.HasPrefix(l, "CREATE UNIQUE INDEX ")) && !strings.Contains(l, "IF NOT EXISTS"):
			t.Errorf("bare CREATE INDEX: %q", line)
		}
	}
}

func TestPG_NoBareDDL_AddColumns(t *testing.T) {
	sc := &SchemaSync{Config: &Config{}, SourceDb: &MyDb{dialect: &PostgresDialect{}}}
	got := sc.getAlterDataBySchema("user",
		testLoadFile("testdata/pg/user_0.sql"),
		testLoadFile("testdata/pg/user_1.sql"),
		&Config{})
	out := got.String()
	assertNoBareDDL(t, out)
	xt.Equal(t, true, strings.Contains(out, "ADD COLUMN IF NOT EXISTS"))
}

func TestMySQL_NoBareDDL_AddColumns(t *testing.T) {
	sc := &SchemaSync{Config: &Config{}, SourceDb: &MyDb{dialect: &MySQLDialect{}}}
	got := sc.getAlterDataBySchema("user",
		testLoadFile("testdata/user/user_0.sql"),
		testLoadFile("testdata/user/user_1.sql"),
		&Config{})
	out := got.String()
	assertNoBareDDL(t, out)
	// 守卫块标志
	xt.Equal(t, true, strings.Contains(out, "PREPARE __mss_stmt FROM @__mss_sql"))
	xt.Equal(t, false, strings.Contains(out, "\nADD ")) // 不应再有裸 ADD 子句独占一行
}
```

> `MyDb` 仅用 `dialect` 字段驱动生成，与既有 `TestSchemaSync_diffTriggers` 构造方式一致，无需真实连接。

- [ ] **Step 2: 运行确认通过**

Run: `go test ./internal/ -run 'NoBareDDL' -v 2>&1 | tail -15`
Expected: PASS。

- [ ] **Step 3: 全量回归 + 构建**

Run: `go build ./... && go test ./internal/... 2>&1 | tail -10`
Expected: PASS（`TestWithDB` SKIP）。

- [ ] **Step 4: 提交**

```bash
git add internal/idempotent_test.go
git commit -m "test: 跨 dialect 断言生成 SQL 无裸 DDL

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review 结果

- **Spec 覆盖**：PG 全部生成点（建表/列/索引/约束/FK/触发器）→ Task 3/4/5/6；MySQL 建表/删表 → Task 7；MySQL 列守卫 → Task 8；MySQL 索引/FK 守卫 → Task 9；接口签名变更 → Task 1；classifySQL 路由 → Task 2；端到端无裸 DDL → Task 10。无遗漏。
- **类型一致性**：`GenAddColumn`/`GenDropColumn` 全程 `(table, ...) []string`；新增 `GenDropIndexMulti`/`GenDropForeignKeyMulti` 在 dialect.go 接口、两 dialect 实现、sync.go 调用点一致；`mysqlGuard` 签名 `(probeFrom, probeWhere, ddl string, runWhenExists bool) []string` 在 Task 8 定义、Task 9 复用一致；`pgIndexIfNotExists` 在 Task 4 定义、sync.go 复用一致。
- **占位符扫描**：无 TBD / TODO；golden 文件均要求「先跑测试取实际 `got` 再誊写」，避免手算转义出错。
- **风险点**：MySQL `EXECUTE` DDL 会隐式提交，事务非原子——这正是改幂等守卫的原因，与 spec 非目标一致；`SELECT 1` 作为 no-op 分支可被 `PREPARE`，兼容性优于 `DO 0`。
