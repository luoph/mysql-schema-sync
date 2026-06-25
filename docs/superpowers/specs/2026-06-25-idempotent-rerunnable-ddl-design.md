# 幂等可重跑 DDL 生成 — 设计文档

日期：2026-06-25
状态：已通过设计评审，待写实现计划

## 背景与问题

当前生成器（PostgreSQL / MySQL 两个 dialect）输出的全是裸 DDL：

- `CREATE TABLE "t" (...)` — 表已存在即报错
- `CREATE INDEX "x" ON ...` — 索引已存在即报错
- `ADD COLUMN ...` — 列已存在即报错
- `DROP INDEX "x"` — 索引不存在即报错（无 `IF EXISTS`）

后果：一批 DDL 中途失败（超时 / 进程被 kill）后，目标库停在半截状态；把同一份脚本**重跑**时，已经生效的语句会因「已存在 / 不存在」报错而整批中断，无法收敛。

## 目标

生成器**始终**输出幂等 DDL（无配置开关）。幂等定义：同一份生成的脚本重复执行任意多次结果一致，中途被 kill 后直接重跑即可收敛。

- 不引入配置项。
- 不在目标库安装存储过程等持久对象。
- 不改执行 / 事务模型（保留现有 per-table 单事务）。

## 非目标

- 不加 `BEGIN/COMMIT` 整批包裹（幂等已足够支撑重跑，PG 现有 per-table 事务保持不变；MySQL DDL 隐式提交，事务包裹无效）。
- MySQL `CHANGE COLUMN`（改类型 / 默认值 / 注释）、`ALTER ... COMMENT`、表级属性变更不加运行时守卫。
- 不做配置开关、不做 MariaDB 专属 `IF [NOT] EXISTS` 扩展探测。

## 方案

### PostgreSQL（`dialect_pg.go` + `sync.go`）

PG 全程优先用原生 `IF [NOT] EXISTS`；唯一例外 `ADD CONSTRAINT`（PG 无此语法），用「先 `DROP CONSTRAINT IF EXISTS` 再 `ADD`」兜底幂等。

| 生成点 | 现状 | 改为 |
|---|---|---|
| `GenCreateTable` | `CREATE TABLE "t" (...)` | `CREATE TABLE IF NOT EXISTS "t" (...)` |
| `GenAddColumn` | `ADD COLUMN ...` | `ADD COLUMN IF NOT EXISTS ...` |
| `GenDropColumn` | `DROP COLUMN "c"` | `DROP COLUMN IF EXISTS "c"` |
| `GenDropIndex`（索引） | `DROP INDEX "x";` | `DROP INDEX IF EXISTS "x";` |
| `GenDropIndex`（PK/UNIQUE/CHECK） | `DROP CONSTRAINT "x"` | `DROP CONSTRAINT IF EXISTS "x"` |
| `GenDropForeignKey` | `DROP CONSTRAINT "x"` | `DROP CONSTRAINT IF EXISTS "x"` |
| `GenDropTable` | `DROP TABLE "t";` | `DROP TABLE IF EXISTS "t";` |
| `GenAddIndex`（CREATE INDEX） | `CREATE [UNIQUE] INDEX "x" ON ...` | `CREATE [UNIQUE] INDEX IF NOT EXISTS "x" ON ...` |
| `GenAddIndex`（PK/UNIQUE/CHECK 约束） | 仅 `needDrop` 时前置 DROP | **总是**前置 `DROP CONSTRAINT IF EXISTS "x"`，再 `ADD CONSTRAINT` |
| `GenAddForeignKey` | 同上 | **总是**前置 `DROP CONSTRAINT IF EXISTS "x"`，再 `ADD CONSTRAINT ... FOREIGN KEY` |
| 触发器新增（`diffTriggers` / `GenAddTrigger`） | 裸 `CREATE TRIGGER` | 前置 `DROP TRIGGER IF EXISTS`（已有该 helper）再 `CREATE TRIGGER` |
| `sync.go` 建表分支里的 CREATE INDEX（当前直接拼 `idx.SQL`） | 裸 `CREATE INDEX` | 复用同一个 `CREATE INDEX IF NOT EXISTS` 改写 helper |

实现要点：
- 抽一个 helper（如 `pgIndexIfNotExists(def string) string`），在 `CREATE INDEX` / `CREATE UNIQUE INDEX` 后插入 `IF NOT EXISTS`，供 `GenAddIndex` 与 `sync.go:建表分支` 复用。
- 约束 / FK 的「总是前置 DROP IF EXISTS」对全新约束是无害 no-op（`DROP CONSTRAINT IF EXISTS` 不存在时静默跳过）。
- 函数 / 扩展已是 `CREATE OR REPLACE` / `IF [NOT] EXISTS`，`COMMENT ON` 天然幂等，不动。

### MySQL（`dialect_mysql.go` + `index.go` + `sync.go`）

免费拿到：
- `GenCreateTable` → `CREATE TABLE IF NOT EXISTS ...`（保留现有 `AUTO_INCREMENT=` 剥除）
- `GenDropTable` → `DROP TABLE IF EXISTS ...`

需要运行时动态 SQL 守卫的：`ADD/DROP COLUMN`、`ADD/DROP INDEX`、`ADD/DROP FOREIGN KEY`（含 PRIMARY KEY）。MySQL 原生不支持这些子句的 `IF [NOT] EXISTS`，每条逻辑变更展开为 **4 条语句**，查 `information_schema` 决定是否执行：

```sql
SET @sql = (SELECT IF(COUNT(*)=0,
  'ALTER TABLE `t` ADD COLUMN `c` int NOT NULL',  -- 内嵌真实 DDL，单引号转义为 ''
  'DO 0')
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND COLUMN_NAME='c');
PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
```

- ADD 系列：`COUNT(*)=0` 才执行真实 DDL，否则 `DO 0`（空操作）。
- DROP 系列：`COUNT(*)>0` 才执行（`IF(COUNT(*)>0, '<ddl>', 'DO 0')`）。
- 探测来源：
  - 列：`information_schema.COLUMNS`，条件 `TABLE_NAME` + `COLUMN_NAME`
  - 索引（含 PK）：`information_schema.STATISTICS`，条件 `TABLE_NAME` + `INDEX_NAME`（PK 为 `'PRIMARY'`）
  - 外键：`information_schema.TABLE_CONSTRAINTS`，条件 `TABLE_NAME` + `CONSTRAINT_NAME` + `CONSTRAINT_TYPE='FOREIGN KEY'`
  - 统一加 `TABLE_SCHEMA=DATABASE()`
- 内嵌 DDL 中的单引号按 SQL 字面量规则双写转义（`'` → `''`），覆盖列默认值 / `COMMENT '...'` 等含引号场景。
- 会话作用域：现有 per-table 单事务 = 单连接，`@sql` 与 prepared statement 的 session 作用域成立；手动整段在一个 session 跑同样成立。

实现要点 — 抽 helper：
```go
// runWhenZero=true 表示 COUNT(*)=0 才执行（ADD 语义）；false 表示 >0 才执行（DROP 语义）
func mysqlGuard(probeTable, whereClause, ddl string, runWhenZero bool) []string
```
返回 `[]string{ "SET @sql = (...)", "PREPARE s FROM @sql", "EXECUTE s", "DEALLOCATE PREPARE s" }`。

### 连带的内部改造（MySQL 路径必须付出的结构代价）

1. 守卫块是**完整语句**，不能再进 `WrapAlterSQL` 拼成 `ALTER TABLE t ADD c1, ADD c2`。MySQL 的 `ADD/DROP COLUMN`、`ADD/DROP INDEX`、`ADD/DROP FK` 改为返回**整组独立语句**，走 standalone 通道。
2. `Dialect.GenAddColumn` / `GenDropColumn` 接口签名由返回 `string` 改为返回 `[]string`（PG 侧返回单元素切片，行为不变）。调用点 `sync.go` 两处（结构化对比循环、legacy 文本对比循环）随之调整。
3. `classifySQL` 的 standalone 前缀集扩充 `SET` / `PREPARE` / `EXECUTE` / `DEALLOCATE`；并让**列变更结果也走 `classifySQL` 路由**（当前列变更直接 append 到 `alterClauses`）。路由后：PG 子句（`ADD COLUMN IF NOT EXISTS` / `DROP COLUMN IF EXISTS`）仍归 `alterClauses` → `WrapAlterSQL`；MySQL 守卫块归 `standaloneSQL`。
4. MySQL `CHANGE COLUMN` 保持裸子句不加守卫（只在列已存在时生成，部分失败后列仍在，重跑到同定义安全；且不在用户列举的痛点内，避免无谓膨胀）。
5. 顺序与正确性：`standaloneSQL` 在 `sync.go` 中接在 `WrapAlterSQL` 输出之后追加进 `alter.SQL`，多个守卫块之间顺序保持；执行在单连接顺序进行。此改造顺带修正现有 MySQL `GenDropIndex` 返回 `DROP INDEX \`x\`` 子句被 `classifySQL` 误判为 standalone 但并非合法独立语句的隐患（守卫化后即为合法完整语句）。

## 测试

现有测试文件：`dialect_pg_test.go` / `dialect_mysql_test.go` / `alter_test.go` / `sync_test.go` / `dialect_test.go` 等。

- PG：每个 `Gen*` 的 `IF [NOT] EXISTS` 断言；约束 / FK 的「`DROP CONSTRAINT IF EXISTS` + `ADD CONSTRAINT`」成对断言；`CREATE INDEX` 改写 helper 单测（普通索引、`UNIQUE`、表达式 / partial 索引）；触发器新增前置 `DROP TRIGGER IF EXISTS` 断言。
- MySQL：守卫块结构断言（4 条语句、探测表与 where 条件正确、`COUNT=0` vs `COUNT>0`、单引号转义）；`classifySQL` 路由断言；`GenAddColumn` / `GenDropColumn` 改为 `[]string` 后的回归；`CREATE/DROP TABLE` 的 `IF EXISTS`。
- 端到端（`sync_test.go` 风格）：断言整表生成的 SQL 序列重复出现而不含裸 `CREATE TABLE` / 裸 `CREATE INDEX` / 裸 `ADD` / 裸 `DROP INDEX`（即均带 `IF [NOT] EXISTS` 或被守卫包裹）。

## 影响面小结

改动文件：`dialect_pg.go`、`dialect_mysql.go`、`index.go`、`sync.go`，及对应 `_test.go`。接口层面仅 `GenAddColumn` / `GenDropColumn` 签名 `string → []string`。无配置 / CLI / 执行模型变更。
