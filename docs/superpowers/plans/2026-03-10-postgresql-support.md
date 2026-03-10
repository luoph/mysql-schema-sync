# PostgreSQL Support Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add PostgreSQL → PostgreSQL schema sync support via a Dialect interface abstraction, while keeping all existing MySQL functionality working.

**Architecture:** Introduce a `Dialect` interface that encapsulates all database-specific operations (connection, schema retrieval, DDL parsing, SQL generation). Extract current MySQL logic into `MySQLDialect`, create new `PostgresDialect`. Core comparison logic (field/index diff detection) stays shared in `sync.go`, delegating SQL generation to the dialect.

**Tech Stack:** Go 1.25.1, `github.com/jackc/pgx/v5` (PostgreSQL driver), existing `github.com/go-sql-driver/mysql`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `internal/dialect.go` | Create | Dialect interface definition, DSN detection, shared diff helper |
| `internal/dialect_mysql.go` | Create | MySQL dialect: schema retrieval, DDL parsing, SQL generation |
| `internal/dialect_mysql_test.go` | Create | MySQL dialect unit tests |
| `internal/dialect_pg.go` | Create | PostgreSQL dialect: schema retrieval, DDL reconstruction, SQL generation |
| `internal/dialect_pg_test.go` | Create | PostgreSQL dialect unit tests |
| `internal/db.go` | Modify | Add `dialect` field to `MyDb`, delegate to dialect methods |
| `internal/sync.go` | Modify | Use dialect in `getSchemaDiff` and `getAlterDataBySchema` |
| `internal/testdata/pg/` | Create | PostgreSQL DDL test fixtures |
| `go.mod` | Modify | Add `pgx/v5` dependency |

**Unchanged files:** `internal/config.go`, `internal/execute.go`, `internal/email.go`, `internal/statics.go`, `internal/timer.go`, `internal/schema.go`, `internal/alter.go`, `internal/index.go`, `main.go`

---

## Chunk 1: Dialect Interface & MySQL Extraction

### Task 1: Define Dialect Interface

**Files:**
- Create: `internal/dialect.go`

- [ ] **Step 1: Create dialect.go with interface and DSN detection**

```go
package internal

import (
	"database/sql"
	"strings"
)

// Dialect abstracts database-specific operations for schema sync
type Dialect interface {
	// DriverName returns the database/sql driver name ("mysql" or "postgres")
	DriverName() string

	// GetDatabaseName returns the current database name from a connection
	GetDatabaseName(db *sql.DB) (string, error)

	// GetTableNames returns all user table names
	GetTableNames(db *sql.DB) ([]string, error)

	// GetTableSchema returns the CREATE TABLE DDL (or reconstructed equivalent)
	GetTableSchema(db *sql.DB, dbName, tableName string) (string, error)

	// GetTableFields returns structured field info from information_schema
	GetTableFields(db *sql.DB, dbName, tableName string) (map[string]*FieldInfo, error)

	// ParseSchema parses a DDL string into MySchema
	ParseSchema(schema string) *MySchema

	// CleanTableSchema removes engine/storage config from DDL (e.g., ENGINE=InnoDB)
	CleanTableSchema(schema string) string

	// Quote quotes an identifier (backtick for MySQL, double-quote for PostgreSQL)
	Quote(name string) string

	// FieldsEqual compares two FieldInfo for semantic equality
	FieldsEqual(a, b *FieldInfo) bool

	// FieldDef returns the column definition string for ALTER statements
	FieldDef(field *FieldInfo) string

	// SupportsColumnOrder returns true if AFTER/FIRST is supported
	SupportsColumnOrder() bool

	// GenAddColumn generates ADD COLUMN clause
	GenAddColumn(colDef, afterCol string, isFirst bool, fieldCount int) string

	// GenChangeColumn generates column modification clauses (may return multiple for PG)
	GenChangeColumn(fieldName string, src, dst *FieldInfo) []string

	// GenChangeColumnText generates column modification from raw text definition (legacy)
	GenChangeColumnText(fieldName, colDef string) string

	// GenDropColumn generates DROP COLUMN clause
	GenDropColumn(colName string) string

	// GenAddIndex generates ADD INDEX clauses or standalone CREATE INDEX
	GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string

	// GenDropIndex generates DROP INDEX clause or standalone DROP INDEX
	GenDropIndex(tableName string, idx *DbIndex) string

	// GenAddForeignKey generates ADD FOREIGN KEY clauses
	GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string

	// GenDropForeignKey generates DROP FOREIGN KEY/CONSTRAINT clause
	GenDropForeignKey(tableName string, idx *DbIndex) string

	// GenCreateTable formats CREATE TABLE SQL for execution
	GenCreateTable(schema string) string

	// GenDropTable generates DROP TABLE statement
	GenDropTable(tableName string) string

	// GenCommentColumnSQL generates COMMENT ON COLUMN SQL (PostgreSQL only, empty for MySQL)
	GenCommentColumnSQL(tableName, colName, comment string) string

	// WrapAlterSQL wraps alter clauses into complete ALTER TABLE statement(s)
	WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string
}

// DetectDialect returns the appropriate Dialect based on the DSN format
func DetectDialect(dsn string) Dialect {
	if strings.HasPrefix(dsn, "postgres://") ||
		strings.HasPrefix(dsn, "postgresql://") ||
		strings.Contains(dsn, "host=") {
		return &PostgresDialect{}
	}
	return &MySQLDialect{}
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go build ./...`
Expected: Compilation error (MySQLDialect and PostgresDialect not yet defined) — this is expected, we'll fix in next tasks.

- [ ] **Step 3: Commit**

```bash
git add internal/dialect.go
git commit -m "feat: define Dialect interface for database abstraction"
```

---

### Task 2: Extract MySQL Dialect Implementation

**Files:**
- Create: `internal/dialect_mysql.go`

- [ ] **Step 1: Create dialect_mysql.go with all MySQL-specific logic**

Extract logic from `db.go` (connection, schema retrieval), `schema.go` (ParseSchema), `index.go` (parseDbIndexLine), `alter.go` (fmtTableCreateSQL), `sync.go` (SQL generation), and `db.go` (FieldInfo methods) into `MySQLDialect`.

```go
package internal

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xanygo/anygo/ds/xmap"
)

// MySQLDialect implements Dialect for MySQL databases
type MySQLDialect struct{}

func (m *MySQLDialect) DriverName() string { return "mysql" }

func (m *MySQLDialect) GetDatabaseName(db *sql.DB) (string, error) {
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	return dbName, err
}

func (m *MySQLDialect) GetTableNames(db *sql.DB) ([]string, error) {
	rs, err := db.Query("show table status")
	if err != nil {
		return nil, fmt.Errorf("show tables failed: %w", err)
	}
	defer rs.Close()
	var tables []string
	columns, _ := rs.Columns()
	for rs.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rs.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("show tables scan failed: %w", err)
		}
		valObj := make(map[string]any)
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				valObj[col] = string(b)
			} else {
				valObj[col] = val
			}
		}
		if valObj["Engine"] != nil {
			tables = append(tables, valObj["Name"].(string))
		}
	}
	return tables, nil
}

func (m *MySQLDialect) GetTableSchema(db *sql.DB, dbName, tableName string) (string, error) {
	rs, err := db.Query(fmt.Sprintf("show create table `%s`", tableName))
	if err != nil {
		return "", err
	}
	defer rs.Close()
	var schema string
	for rs.Next() {
		var vname string
		if err := rs.Scan(&vname, &schema); err != nil {
			return "", fmt.Errorf("get table %s schema failed: %w", tableName, err)
		}
	}
	return schema, nil
}

func (m *MySQLDialect) GetTableFields(db *sql.DB, dbName, tableName string) (map[string]*FieldInfo, error) {
	const query = `
		SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
			DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
			CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_COMMENT, EXTRA
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query INFORMATION_SCHEMA.COLUMNS for %q: %w", tableName, err)
	}
	defer rows.Close()
	fields := make(map[string]*FieldInfo)
	for rows.Next() {
		field := &FieldInfo{}
		var charMaxLen, numericPrecision, numericScale sql.NullInt64
		var charset, collation, columnDefault sql.NullString
		err := rows.Scan(
			&field.ColumnName, &field.OrdinalPosition, &columnDefault,
			&field.IsNullAble, &field.DataType, &charMaxLen,
			&numericPrecision, &numericScale, &charset, &collation,
			&field.ColumnType, &field.ColumnComment, &field.Extra,
		)
		if err != nil {
			return nil, fmt.Errorf("scan field for %q: %w", tableName, err)
		}
		if columnDefault.Valid {
			field.ColumnDefault = &columnDefault.String
		}
		if charMaxLen.Valid {
			v := int(charMaxLen.Int64)
			field.CharacterMaximumLength = &v
		}
		if numericPrecision.Valid {
			v := int(numericPrecision.Int64)
			field.NumericPrecision = &v
		}
		if numericScale.Valid {
			v := int(numericScale.Int64)
			field.NumericScale = &v
		}
		if charset.Valid {
			field.CharsetName = &charset.String
		}
		if collation.Valid {
			field.CollationName = &collation.String
		}
		fields[field.ColumnName] = field
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("no fields found for %q in %q", tableName, dbName)
	}
	return fields, nil
}

// MySQL index parsing regex (moved from index.go)
var (
	mysqlIndexReg           = regexp.MustCompile(`^([A-Z]+\s)?KEY\s`)
	mysqlForeignKeyReg      = regexp.MustCompile("^CONSTRAINT `(.+)` FOREIGN KEY.+ REFERENCES `(.+)` ")
	mysqlCheckConstraintReg = regexp.MustCompile("^CONSTRAINT `([^`]+)` CHECK \\(\\((.+)\\)\\)")
)

func (m *MySQLDialect) ParseSchema(schema string) *MySchema {
	schema = strings.TrimSpace(schema)
	lines := strings.Split(schema, "\n")
	mys := &MySchema{
		SchemaRaw:  schema,
		FieldInfos: make(map[string]*FieldInfo),
		IndexAll:   make(map[string]*DbIndex),
		ForeignAll: make(map[string]*DbIndex),
	}
	for i := 1; i < len(lines)-1; i++ {
		line := strings.TrimSpace(lines[i])
		if len(line) == 0 {
			continue
		}
		line = strings.TrimRight(line, ",")
		if line[0] == '`' {
			index := strings.Index(line[1:], "`")
			name := line[1 : index+1]
			mys.Fields.Set(name, line)
		} else {
			idx := m.parseMySQLIndexLine(line)
			if idx == nil {
				continue
			}
			if idx.IndexType == indexTypeForeignKey {
				mys.ForeignAll[idx.Name] = idx
			} else {
				mys.IndexAll[idx.Name] = idx
			}
		}
	}
	return mys
}

func (m *MySQLDialect) parseMySQLIndexLine(line string) *DbIndex {
	line = strings.TrimSpace(line)
	idx := &DbIndex{SQL: line, RelationTables: []string{}}
	if strings.HasPrefix(line, "PRIMARY") {
		idx.IndexType = indexTypePrimary
		idx.Name = "PRIMARY KEY"
		return idx
	}
	if mysqlIndexReg.MatchString(line) {
		arr := strings.Split(line, "`")
		idx.IndexType = indexTypeIndex
		idx.Name = arr[1]
		return idx
	}
	if matches := mysqlForeignKeyReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = indexTypeForeignKey
		idx.Name = matches[1]
		idx.addRelationTable(matches[2])
		return idx
	}
	if matches := mysqlCheckConstraintReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = checkConstraint
		idx.Name = matches[1]
		return idx
	}
	log.Printf("[Warning] MySQL index parse: unsupported line: %s", line)
	return nil
}

func (m *MySQLDialect) CleanTableSchema(schema string) string {
	return strings.Split(schema, "ENGINE")[0]
}

func (m *MySQLDialect) Quote(name string) string {
	return "`" + name + "`"
}

func (m *MySQLDialect) FieldsEqual(a, b *FieldInfo) bool {
	return a.Equals(b) // uses existing FieldInfo.Equals with MySQL normalization
}

func (m *MySQLDialect) FieldDef(field *FieldInfo) string {
	return field.String() // uses existing FieldInfo.String with backtick quoting
}

func (m *MySQLDialect) SupportsColumnOrder() bool { return true }

func (m *MySQLDialect) GenAddColumn(colDef, afterCol string, isFirst bool, fieldCount int) string {
	if afterCol == "" {
		if isFirst {
			return "ADD " + colDef + " FIRST"
		}
		return "ADD " + colDef
	}
	return fmt.Sprintf("ADD %s AFTER `%s`", colDef, afterCol)
}

func (m *MySQLDialect) GenChangeColumn(fieldName string, src, dst *FieldInfo) []string {
	return []string{fmt.Sprintf("CHANGE `%s` %s", fieldName, src.String())}
}

func (m *MySQLDialect) GenChangeColumnText(fieldName, colDef string) string {
	return fmt.Sprintf("CHANGE `%s` %s", fieldName, colDef)
}

func (m *MySQLDialect) GenDropColumn(colName string) string {
	return fmt.Sprintf("drop `%s`", colName)
}

func (m *MySQLDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	return idx.alterAddSQL(needDrop)
}

func (m *MySQLDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	return idx.alterDropSQL()
}

func (m *MySQLDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	return idx.alterAddSQL(needDrop)
}

func (m *MySQLDialect) GenDropForeignKey(tableName string, idx *DbIndex) string {
	return idx.alterDropSQL()
}

var mysqlAutoIncrReg = regexp.MustCompile(`\sAUTO_INCREMENT=[1-9]\d*\s`)

func (m *MySQLDialect) GenCreateTable(schema string) string {
	return mysqlAutoIncrReg.ReplaceAllString(schema, " ") + ";"
}

func (m *MySQLDialect) GenDropTable(tableName string) string {
	return fmt.Sprintf("drop table `%s`;", tableName)
}

func (m *MySQLDialect) GenCommentColumnSQL(tableName, colName, comment string) string {
	return "" // MySQL handles comments inline in column definition
}

func (m *MySQLDialect) WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string {
	if len(clauses) == 0 {
		return nil
	}
	if singleChange {
		var result []string
		for _, clause := range clauses {
			result = append(result, fmt.Sprintf("ALTER TABLE `%s`\n%s;", tableName, clause))
		}
		return result
	}
	return []string{fmt.Sprintf("ALTER TABLE `%s`\n%s;", tableName, strings.Join(clauses, ",\n"))}
}
```

- [ ] **Step 2: Verify it compiles (will fail due to PostgresDialect not defined)**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go vet ./internal/dialect.go ./internal/dialect_mysql.go`
Expected: Should compile these two files in isolation (PostgresDialect reference is in dialect.go only)

- [ ] **Step 3: Commit**

```bash
git add internal/dialect_mysql.go
git commit -m "feat: extract MySQL-specific logic into MySQLDialect"
```

---

### Task 3: Refactor MyDb to Use Dialect

**Files:**
- Modify: `internal/db.go`

- [ ] **Step 1: Refactor MyDb to add dialect field and delegate methods**

Key changes to `db.go`:
1. Add `dialect Dialect` field to `MyDb`
2. Modify `NewMyDb` to detect dialect and use `dialect.DriverName()`
3. Modify `GetTableNames` to delegate to `dialect.GetTableNames()`
4. Modify `GetTableSchema` to delegate to `dialect.GetTableSchema()`
5. Modify `TableFieldsFromInformationSchema` to delegate to `dialect.GetTableFields()`
6. Remove `getDatabaseName` function (moved to dialect)
7. Remove MySQL driver import (moved to dialect_mysql.go)
8. Keep `FieldInfo` struct and its methods (they're still used by MySQLDialect)
9. Keep `Query` method (shared)

The `FieldInfo` type, its methods (`String`, `Equals`, `needsQuotedDefault`, `charsetEquals`, `collationEquals`), and the `dbType` constants stay in `db.go` as they're shared data types. The MySQL-specific _usage_ of them is in `dialect_mysql.go`.

```go
// NewMyDb - updated to use dialect
func NewMyDb(dsn string, dbType dbType) *MyDb {
	dialect := DetectDialect(dsn)
	db, err := sql.Open(dialect.DriverName(), dsn)
	if err != nil {
		panic(fmt.Sprintf("connected to db [%s] failed,err=%s", dsn, err))
	}
	dbName, err := dialect.GetDatabaseName(db)
	if err != nil {
		panic(fmt.Sprintf("get database name failed,err=%s", err))
	}
	return &MyDb{
		sqlDB:   db,
		dbType:  dbType,
		dbName:  dbName,
		dialect: dialect,
	}
}
```

`GetTableNames`, `GetTableSchema`, `TableFieldsFromInformationSchema` delegate to `db.dialect.GetTableNames(db.sqlDB)` etc., wrapping results with logging via `db.Query`.

- [ ] **Step 2: Run existing MySQL tests to verify no regressions**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -run TestParseSchema -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/db.go
git commit -m "refactor: MyDb delegates to Dialect for database operations"
```

---

### Task 4: Refactor sync.go to Use Dialect

**Files:**
- Modify: `internal/sync.go`

- [ ] **Step 1: Update getAlterDataBySchema to use dialect**

Key changes:
1. Replace `RemoveTableSchemaConfig(schema)` with `sc.SourceDb.dialect.CleanTableSchema(schema)`
2. Replace hardcoded `fmt.Sprintf("drop table \`%s\`;", table)` with `sc.SourceDb.dialect.GenDropTable(table)`
3. Replace `fmtTableCreateSQL(sSchema)+";"` with `sc.SourceDb.dialect.GenCreateTable(sSchema)`
4. Replace `WrapAlterSQL` logic with `sc.SourceDb.dialect.WrapAlterSQL(table, diffLines, cfg.SingleSchemaChange)`
5. Use `sc.SourceDb.dialect.ParseSchema()` in `newSchemaDiff` calls

- [ ] **Step 2: Update getSchemaDiff to use dialect for SQL generation**

Replace all hardcoded MySQL SQL strings with dialect method calls:
- `fmt.Sprintf("MODIFY COLUMN %s", ...)` → `sc.SourceDb.dialect.GenChangeColumn(...)`
- `"ADD " + value + " FIRST"` → `sc.SourceDb.dialect.GenAddColumn(value, "", true, fieldCount)`
- `fmt.Sprintf("ADD %s AFTER \`%s\`", ...)` → `sc.SourceDb.dialect.GenAddColumn(value, beforeFieldName, false, fieldCount)`
- `fmt.Sprintf("CHANGE \`%s\` %s", ...)` → `sc.SourceDb.dialect.GenChangeColumn(...)` or `GenChangeColumnText(...)`
- `fmt.Sprintf("drop \`%s\`", name)` → `sc.SourceDb.dialect.GenDropColumn(name)`
- `idx.alterAddSQL(true/false)` → `sc.SourceDb.dialect.GenAddIndex(table, idx, true/false)`
- `dIdx.alterDropSQL()` → `sc.SourceDb.dialect.GenDropIndex(table, dIdx)`
- Foreign key operations → `GenAddForeignKey`/`GenDropForeignKey`
- `sourceFieldInfo.Equals(destFieldInfo)` → `sc.SourceDb.dialect.FieldsEqual(sourceFieldInfo, destFieldInfo)`
- `sourceFieldInfo.String()` → `sc.SourceDb.dialect.FieldDef(sourceFieldInfo)`
- Field order check: wrap with `sc.SourceDb.dialect.SupportsColumnOrder()`

Also add comment SQL generation for PostgreSQL:
- After column comparison, collect comment changes and append `dialect.GenCommentColumnSQL()` results

- [ ] **Step 3: Run all existing tests**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -v`
Expected: All existing tests PASS

- [ ] **Step 4: Commit**

```bash
git add internal/sync.go
git commit -m "refactor: sync.go uses Dialect for all SQL generation"
```

---

### Task 5: Verify MySQL Tests Still Pass

- [ ] **Step 1: Run full test suite**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -v -count=1`
Expected: All tests PASS with no regressions

- [ ] **Step 2: Run go vet**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go vet ./...`
Expected: No issues

- [ ] **Step 3: Commit any fix-ups**

---

## Chunk 2: PostgreSQL Dialect Implementation

### Task 6: Add pgx Dependency

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add pgx/v5 dependency**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go get github.com/jackc/pgx/v5`

- [ ] **Step 2: Verify module is resolved**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go mod tidy`

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add github.com/jackc/pgx/v5 for PostgreSQL support"
```

---

### Task 7: Create PostgreSQL Dialect - Schema Retrieval

**Files:**
- Create: `internal/dialect_pg.go`

- [ ] **Step 1: Create dialect_pg.go with connection and schema retrieval methods**

```go
package internal

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresDialect struct{}

func (p *PostgresDialect) DriverName() string { return "pgx" }

func (p *PostgresDialect) GetDatabaseName(db *sql.DB) (string, error) {
	var dbName string
	err := db.QueryRow("SELECT current_database()").Scan(&dbName)
	return dbName, err
}

func (p *PostgresDialect) GetTableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT tablename FROM pg_tables
		WHERE schemaname = 'public'
		ORDER BY tablename`)
	if err != nil {
		return nil, fmt.Errorf("pg get tables failed: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}
```

`GetTableSchema` reconstructs CREATE TABLE DDL from pg_catalog:
- Query columns from `information_schema.columns`
- Query primary key from `pg_indexes` + `pg_constraint`
- Query indexes from `pg_indexes`
- Query foreign keys from `information_schema.table_constraints` + `referential_constraints` + `key_column_usage`
- Query check constraints from `pg_constraint`
- Format into MySQL-like DDL using double-quoted identifiers

`GetTableFields` queries `information_schema.columns` and builds `FieldInfo`:
- Construct `ColumnType` from `udt_name` + `character_maximum_length` / `numeric_precision` / `numeric_scale`
- Query column comments from `pg_description` + `pg_attribute` and populate `ColumnComment`
- Clean `column_default` values: strip `::type` casting suffixes, detect `nextval(...)` for serial columns

- [ ] **Step 2: Verify compilation**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go build ./...`
Expected: Compiles (PostgresDialect may not yet satisfy interface — complete in next step)

---

### Task 8: PostgreSQL Dialect - DDL Parsing & SQL Generation

**Files:**
- Modify: `internal/dialect_pg.go`

- [ ] **Step 1: Implement ParseSchema for PostgreSQL DDL**

PostgreSQL DDL uses double-quoted identifiers. Parse structure:
```sql
CREATE TABLE "users" (
    "id" serial NOT NULL,
    "name" varchar(100) NOT NULL DEFAULT '',
    PRIMARY KEY ("id"),
    CONSTRAINT "fk_org" FOREIGN KEY ("org_id") REFERENCES "orgs" ("id")
);
```

Regex patterns for PostgreSQL indexes/constraints:
- PRIMARY KEY: `PRIMARY KEY \("..."\)`
- UNIQUE: `UNIQUE \("..."\)` or `CONSTRAINT "name" UNIQUE ("...")`
- INDEX: Not in CREATE TABLE DDL (separate CREATE INDEX statements stored as DbIndex)
- FOREIGN KEY: `CONSTRAINT "name" FOREIGN KEY \("..."\) REFERENCES "table" \("..."\)`
- CHECK: `CONSTRAINT "name" CHECK \(...\)`

- [ ] **Step 2: Implement SQL generation methods**

```go
func (p *PostgresDialect) Quote(name string) string {
	return `"` + name + `"`
}

func (p *PostgresDialect) CleanTableSchema(schema string) string {
	return schema // PostgreSQL DDL has no ENGINE clause to strip
}

func (p *PostgresDialect) SupportsColumnOrder() bool { return false }

func (p *PostgresDialect) FieldDef(field *FieldInfo) string {
	// PostgreSQL column definition: "name" type [NOT NULL] [DEFAULT val]
	// No inline COMMENT (handled by GenCommentColumnSQL)
}

func (p *PostgresDialect) FieldsEqual(a, b *FieldInfo) bool {
	// Compare with PostgreSQL type alias normalization
	// int4=integer, int8=bigint, int2=smallint, float8=double precision
	// bool=boolean, varchar=character varying, timestamptz=timestamp with time zone
	// Handle serial: nextval(...) default is equivalent between serial columns
}

func (p *PostgresDialect) GenAddColumn(colDef, afterCol string, isFirst bool, fieldCount int) string {
	return "ADD COLUMN " + colDef // No AFTER/FIRST in PostgreSQL
}

func (p *PostgresDialect) GenChangeColumn(fieldName string, src, dst *FieldInfo) []string {
	// Compare individual attributes and generate separate ALTER COLUMN clauses:
	var clauses []string
	// 1. Type change: ALTER COLUMN "name" TYPE new_type
	// 2. Nullability: ALTER COLUMN "name" SET/DROP NOT NULL
	// 3. Default: ALTER COLUMN "name" SET DEFAULT val / DROP DEFAULT
	return clauses
}

func (p *PostgresDialect) GenChangeColumnText(fieldName, colDef string) string {
	// Fallback: not used for PostgreSQL (always has structured info)
	return ""
}

func (p *PostgresDialect) GenDropColumn(colName string) string {
	return fmt.Sprintf(`DROP COLUMN "%s"`, colName)
}

func (p *PostgresDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	// PostgreSQL: indexes are standalone CREATE INDEX statements
	// Return standalone SQL (will be appended to alter.SQL directly, not wrapped)
}

func (p *PostgresDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	// DROP INDEX "idx_name" (standalone, not ALTER TABLE clause)
}

func (p *PostgresDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	// ALTER TABLE clause: ADD CONSTRAINT "name" FOREIGN KEY ...
}

func (p *PostgresDialect) GenDropForeignKey(tableName string, idx *DbIndex) string {
	return fmt.Sprintf(`DROP CONSTRAINT "%s"`, idx.Name)
}

func (p *PostgresDialect) GenCreateTable(schema string) string {
	return schema + ";"
}

func (p *PostgresDialect) GenDropTable(tableName string) string {
	return fmt.Sprintf(`DROP TABLE "%s";`, tableName)
}

func (p *PostgresDialect) GenCommentColumnSQL(tableName, colName, comment string) string {
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS '%s';`, tableName, colName, escaped)
}

func (p *PostgresDialect) WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string {
	// PostgreSQL: each clause is a separate ALTER TABLE statement
	var result []string
	for _, clause := range clauses {
		result = append(result, fmt.Sprintf(`ALTER TABLE "%s" %s;`, tableName, clause))
	}
	return result
}
```

- [ ] **Step 3: Verify interface satisfaction**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go build ./...`
Expected: Compiles with no errors — both MySQLDialect and PostgresDialect satisfy Dialect

- [ ] **Step 4: Commit**

```bash
git add internal/dialect_pg.go
git commit -m "feat: implement PostgresDialect with schema retrieval and SQL generation"
```

---

### Task 9: Handle Standalone SQL in sync.go

**Files:**
- Modify: `internal/sync.go`

- [ ] **Step 1: Support standalone SQL in getSchemaDiff**

PostgreSQL index operations return standalone SQL (CREATE INDEX / DROP INDEX) rather than ALTER TABLE clauses. Modify `getSchemaDiff` to collect two categories:
1. `alterClauses` — wrapped by `WrapAlterSQL` into ALTER TABLE statements
2. `standaloneSQL` — appended directly to `alter.SQL`

For index/foreign key operations, check if the returned SQL is a standalone statement (starts with `CREATE` or `DROP INDEX`). If so, add to `standaloneSQL` instead of `alterClauses`.

The simplest approach: since PostgreSQL's `GenAddIndex` returns complete statements (starting with `CREATE INDEX`), and MySQL's returns ALTER TABLE clauses (starting with `ADD`), we can detect this by checking the prefix.

Alternatively, add a helper method to Dialect:

```go
// IsStandaloneSQL returns true if the SQL is a standalone statement (not an ALTER TABLE clause)
// For PostgreSQL: CREATE INDEX, DROP INDEX are standalone
// For MySQL: all index operations are ALTER TABLE clauses
```

Or more simply: have `GenAddIndex` / `GenDropIndex` use a prefix convention. Standalone statements use a `@@STANDALONE@@` prefix that gets stripped and handled separately.

Actually, the cleanest approach: split the return from getSchemaDiff into two slices, or have the dialect's WrapAlterSQL handle standalone SQL by passing it through unchanged.

- [ ] **Step 2: Update getAlterDataBySchema to handle standalone SQL**

After calling `getSchemaDiff`, separate standalone SQL from ALTER clauses, then:
```go
alterClauses, standaloneSQL := sc.getSchemaDiff(alter)
alter.SQL = append(alter.SQL, sc.SourceDb.dialect.WrapAlterSQL(table, alterClauses, cfg.SingleSchemaChange)...)
alter.SQL = append(alter.SQL, standaloneSQL...)
```

Also append comment SQL here for PostgreSQL.

- [ ] **Step 3: Run tests**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/sync.go
git commit -m "feat: support standalone SQL in schema diff for PostgreSQL indexes"
```

---

## Chunk 3: PostgreSQL Tests

### Task 10: Create PostgreSQL Test Data

**Files:**
- Create: `internal/testdata/pg/source_1.sql`
- Create: `internal/testdata/pg/dest_1.sql`
- Create: `internal/testdata/pg/result_add_columns.sql`
- Create: `internal/testdata/pg/source_2.sql`
- Create: `internal/testdata/pg/dest_2.sql`
- Create: `internal/testdata/pg/result_change_columns.sql`

- [ ] **Step 1: Create test DDL for add column scenario**

`source_1.sql`:
```sql
CREATE TABLE "users" (
    "id" serial NOT NULL,
    "email" varchar(255) NOT NULL DEFAULT '',
    "name" varchar(100) NOT NULL DEFAULT '',
    "status" integer NOT NULL DEFAULT 0,
    PRIMARY KEY ("id")
)
```

`dest_1.sql`:
```sql
CREATE TABLE "users" (
    "id" serial NOT NULL,
    "email" varchar(255) NOT NULL DEFAULT '',
    PRIMARY KEY ("id")
)
```

`result_add_columns.sql`:
```
-- Table : users
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE "users" ADD COLUMN "name" varchar(100) NOT NULL DEFAULT '';
ALTER TABLE "users" ADD COLUMN "status" integer NOT NULL DEFAULT 0;
```

- [ ] **Step 2: Create test DDL for change column scenario**

`source_2.sql`: Table with modified column types, nullability, defaults.
`dest_2.sql`: Original column definitions.
`result_change_columns.sql`: Expected ALTER COLUMN statements.

- [ ] **Step 3: Create test DDL for index scenarios**

Files for adding/dropping indexes, with expected CREATE INDEX / DROP INDEX standalone SQL.

- [ ] **Step 4: Create test DDL for foreign key and comment scenarios**

- [ ] **Step 5: Commit**

```bash
git add internal/testdata/pg/
git commit -m "test: add PostgreSQL DDL test fixtures"
```

---

### Task 11: Write PostgreSQL Dialect Unit Tests

**Files:**
- Create: `internal/dialect_pg_test.go`

- [ ] **Step 1: Write ParseSchema tests for PostgreSQL DDL**

```go
func TestPostgresDialect_ParseSchema(t *testing.T) {
	pg := &PostgresDialect{}
	schema := testLoadFile("testdata/pg/source_1.sql")
	mys := pg.ParseSchema(schema)
	// Verify fields parsed correctly
	// Verify indexes parsed correctly
	// Verify foreign keys parsed correctly
}
```

- [ ] **Step 2: Write FieldsEqual tests with type alias normalization**

```go
func TestPostgresDialect_FieldsEqual(t *testing.T) {
	pg := &PostgresDialect{}
	tests := []struct {
		name string
		a, b *FieldInfo
		want bool
	}{
		{"int4 vs integer", fieldInfo("int4", "integer"), fieldInfo("integer", "integer"), true},
		{"varchar vs character varying", ...},
		{"different types", ...},
		{"null vs not null", ...},
		{"different defaults", ...},
		{"serial nextval equivalence", ...},
	}
}
```

- [ ] **Step 3: Write GenChangeColumn tests**

Test that changing type, nullability, and default each produce the correct ALTER COLUMN clause.

- [ ] **Step 4: Write WrapAlterSQL tests**

Verify each clause becomes its own `ALTER TABLE "t" clause;` statement.

- [ ] **Step 5: Write full schema sync tests (like MySQL sync_test.go)**

```go
func TestPostgresDialect_SchemaSync(t *testing.T) {
	pg := &PostgresDialect{}
	sc := &SchemaSync{Config: &Config{}, SourceDb: &MyDb{dialect: pg}, DestDb: &MyDb{dialect: pg}}

	// Test: add columns
	got := sc.getAlterDataBySchema("users",
		testLoadFile("testdata/pg/source_1.sql"),
		testLoadFile("testdata/pg/dest_1.sql"),
		&Config{})
	xt.Equal(t, testLoadFile("testdata/pg/result_add_columns.sql"), got.String())

	// Test: change columns
	// Test: drop columns
	// Test: index operations
	// Test: foreign key operations
}
```

- [ ] **Step 6: Run all tests**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -v -count=1`
Expected: ALL tests pass (both MySQL and PostgreSQL)

- [ ] **Step 7: Commit**

```bash
git add internal/dialect_pg_test.go
git commit -m "test: add PostgreSQL dialect unit tests"
```

---

### Task 12: Write MySQL Dialect Tests (Regression)

**Files:**
- Create: `internal/dialect_mysql_test.go`

- [ ] **Step 1: Write MySQL dialect unit tests**

Ensure the extracted MySQL dialect produces identical output to the original code. Test `ParseSchema`, `GenAddColumn`, `GenChangeColumn`, `WrapAlterSQL` etc.

- [ ] **Step 2: Run and verify**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -v -count=1`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/dialect_mysql_test.go
git commit -m "test: add MySQL dialect regression tests"
```

---

## Chunk 4: DSN Validation & Final Integration

### Task 13: Add Same-Dialect Validation

**Files:**
- Modify: `internal/config.go`

- [ ] **Step 1: Add validation in Config.Check()**

Source and dest must use the same database type. Detect via `DetectDialect`:

```go
func (cfg *Config) Check() {
	if len(cfg.SourceDSN) == 0 {
		log.Fatal("source DSN is empty")
	}
	if len(cfg.DestDSN) == 0 {
		log.Fatal("dest DSN is empty")
	}
	srcDialect := DetectDialect(cfg.SourceDSN)
	dstDialect := DetectDialect(cfg.DestDSN)
	if srcDialect.DriverName() != dstDialect.DriverName() {
		log.Fatalf("source (%s) and dest (%s) must use the same database type",
			srcDialect.DriverName(), dstDialect.DriverName())
	}
}
```

- [ ] **Step 2: Write test for DSN validation**

- [ ] **Step 3: Commit**

```bash
git add internal/config.go
git commit -m "feat: validate source and dest use same database type"
```

---

### Task 14: Update DSN Detection Tests

**Files:**
- Modify: `internal/dialect.go` (add test in new file)
- Create: `internal/dialect_test.go`

- [ ] **Step 1: Write DetectDialect tests**

```go
func TestDetectDialect(t *testing.T) {
	tests := []struct {
		dsn  string
		want string
	}{
		{"user:pass@tcp(localhost:3306)/mydb", "mysql"},
		{"postgres://user:pass@localhost:5432/mydb", "pgx"},
		{"postgresql://user:pass@localhost/mydb", "pgx"},
		{"host=localhost port=5432 dbname=mydb", "pgx"},
		{"user:pass@(localhost:3306)/mydb", "mysql"},
	}
	for _, tt := range tests {
		d := DetectDialect(tt.dsn)
		xt.Equal(t, tt.want, d.DriverName())
	}
}
```

- [ ] **Step 2: Run and verify**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./internal/ -run TestDetectDialect -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/dialect_test.go
git commit -m "test: add DSN dialect detection tests"
```

---

### Task 15: Clean Up Legacy Code

**Files:**
- Modify: `internal/index.go` — keep as-is (still used by MySQLDialect via `alterAddSQL`/`alterDropSQL`)
- Modify: `internal/alter.go` — remove `fmtTableCreateSQL` (moved to MySQLDialect), keep `TableAlterData` and `alterType`
- Modify: `internal/schema.go` — `ParseSchema` function remains as fallback; `newSchemaDiff` updated to accept dialect

- [ ] **Step 1: Clean up alter.go**

Remove `autoIncrReg` and `fmtTableCreateSQL` since they're now in `dialect_mysql.go`. Keep `TableAlterData`, `alterType`, and their methods.

- [ ] **Step 2: Update schema.go**

`newSchemaDiff` and `NewSchemaDiffWithFieldInfos` should use the dialect's `ParseSchema` instead of the global `ParseSchema`. Update their signatures to accept a `Dialect` parameter, or have the caller pass pre-parsed schemas.

Keep the global `ParseSchema` for backward compatibility (it uses MySQL-style parsing by default).

- [ ] **Step 3: Run all tests**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./... -v -count=1`
Expected: ALL tests pass

- [ ] **Step 4: Run go vet and build**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go vet ./... && go build ./...`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add internal/alter.go internal/schema.go internal/sync.go
git commit -m "refactor: clean up legacy code after dialect extraction"
```

---

### Task 16: Final Integration Verification

- [ ] **Step 1: Run complete test suite**

Run: `cd /Volumes/Hagibis/workspace/luoph/mysql-schema-sync && go test ./... -v -count=1`
Expected: ALL tests pass

- [ ] **Step 2: Run go vet**

Run: `go vet ./...`
Expected: Clean

- [ ] **Step 3: Build binary**

Run: `go build -o mysql-schema-sync .`
Expected: Binary builds successfully

- [ ] **Step 4: Manual smoke test with --help**

Run: `./mysql-schema-sync --help`
Expected: Shows help with existing flags

- [ ] **Step 5: Commit all remaining changes**

```bash
git add -A
git commit -m "feat: PostgreSQL schema sync support complete"
```
