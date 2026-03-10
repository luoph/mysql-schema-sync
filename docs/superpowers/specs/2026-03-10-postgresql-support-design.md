# PostgreSQL Support Design

## Overview

为 mysql-schema-sync 项目添加 PostgreSQL → PostgreSQL 的表结构同步支持，与现有 MySQL → MySQL 功能对等。

## Scope

**包含：**
- 字段同步（新增、修改、删除）
- 索引同步（普通索引、唯一索引）
- 主键同步
- 外键同步
- CHECK 约束同步
- 字段注释同步

**不包含：**
- 跨数据库同步（MySQL ↔ PostgreSQL）
- 字段顺序同步（PostgreSQL 不支持列重排序）
- 枚举类型（CREATE TYPE ... AS ENUM）
- 分区表

## Architecture: Dialect Interface

采用 Dialect 接口抽象，将数据库特定操作封装在方言实现中。核心比较逻辑保持共用。

### Interface Definition

```go
type Dialect interface {
    // Connection
    DriverName() string
    GetDatabaseName(db *sql.DB) (string, error)
    GetTableNames(db *sql.DB) ([]string, error)
    GetTableSchema(db *sql.DB, dbName, tableName string) (string, error)
    GetTableFields(db *sql.DB, dbName, tableName string) (map[string]*FieldInfo, error)

    // Schema processing
    ParseSchema(schema string) *MySchema
    CleanTableSchema(schema string) string
    Quote(name string) string
    FieldsEqual(a, b *FieldInfo) bool
    FieldDef(field *FieldInfo) string
    SupportsColumnOrder() bool

    // SQL generation
    GenAddColumn(colDef, afterCol string, isFirst bool, fieldCount int) string
    GenChangeColumn(fieldName string, src, dst *FieldInfo) []string
    GenDropColumn(colName string) string
    GenAddIndex(idx *DbIndex, needDrop bool) []string
    GenDropIndex(idx *DbIndex) string
    GenAddForeignKey(idx *DbIndex, needDrop bool) []string
    GenDropForeignKey(idx *DbIndex) string
    GenCreateTable(schema string) string
    GenDropTable(tableName string) string
    GenCommentSQL(tableName, colName, comment string) string
    WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string
}
```

### DSN Detection

```go
func DetectDialect(dsn string) Dialect {
    // postgres:// | postgresql:// | host= → PostgresDialect
    // otherwise → MySQLDialect
}
```

## PostgreSQL Implementation Details

### Schema Retrieval

PostgreSQL 无 `SHOW CREATE TABLE`，从系统表重建 DDL：

- **表名**：`pg_tables WHERE schemaname = 'public'`
- **列信息**：`information_schema.columns`，拼接 `data_type` + 长度/精度为 `ColumnType`
- **索引**：`pg_indexes`
- **约束**：`information_schema.table_constraints` + `key_column_usage` + `referential_constraints`
- **注释**：`pg_catalog.pg_description` + `pg_catalog.pg_attribute`

### ALTER Syntax Differences

| Operation | MySQL | PostgreSQL |
|-----------|-------|------------|
| Add column | `ADD col_def AFTER other` | `ADD COLUMN col_def` |
| Change type | `CHANGE old new_def` | `ALTER COLUMN "c" TYPE new_type` |
| Change NULL | Included in CHANGE | `ALTER COLUMN "c" SET/DROP NOT NULL` |
| Change default | Included in CHANGE | `ALTER COLUMN "c" SET/DROP DEFAULT val` |
| Drop column | `` DROP `col` `` | `DROP COLUMN "col"` |
| Add index | `ADD INDEX ...` (in ALTER TABLE) | `CREATE INDEX ...` (standalone) |
| Drop index | `` DROP INDEX `idx` `` (in ALTER TABLE) | `DROP INDEX "idx"` (standalone) |
| Primary key | `ADD/DROP PRIMARY KEY` | `ADD/DROP CONSTRAINT "pkey" PRIMARY KEY (...)` |
| Foreign key | `` DROP FOREIGN KEY `fk` `` | `DROP CONSTRAINT "fk"` |
| Comment | Inline `COMMENT 'text'` | `COMMENT ON COLUMN "t"."c" IS 'text'` (standalone) |

### Type Alias Normalization

PostgreSQL FieldsEqual 需归一化类型别名：`int4`→`integer`, `int8`→`bigint`, `varchar`→`character varying` 等。

### SERIAL Handling

`serial`/`bigserial` 在 information_schema 中显示为 `integer` + `nextval(...)` 默认值，比较时需特殊识别。

## File Changes

**New files:**
- `internal/dialect.go` — Interface + DSN detection + shared comparison logic
- `internal/dialect_mysql.go` — MySQL dialect implementation
- `internal/dialect_pg.go` — PostgreSQL dialect implementation
- `internal/dialect_pg_test.go` — PostgreSQL unit tests
- `internal/testdata/pg/` — PostgreSQL test DDL files

**Modified files:**
- `internal/db.go` — MyDb adds dialect field, delegates to dialect
- `internal/sync.go` — getSchemaDiff uses dialect for SQL generation
- `internal/schema.go` — ParseSchema delegates to dialect
- `internal/index.go` — Keep as MySQL default, PG handled in dialect
- `internal/alter.go` — fmtTableCreateSQL moves to MySQL dialect
- `go.mod` — Add `github.com/jackc/pgx/v5`

**Unchanged files:**
- `internal/config.go`, `internal/execute.go`, `internal/email.go`, `internal/statics.go`, `internal/timer.go`, `main.go`

## Constraints

- Source and dest must be same database type (error if mixed)
- PostgreSQL syncs `public` schema only
- Existing MySQL tests must continue to pass
- DSN without PostgreSQL prefix defaults to MySQL (backward compatible)
