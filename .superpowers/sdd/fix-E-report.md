# fix-E: MySQL Backtick Identifier Escaping — Report

## Sites Changed

### Site 1 — `internal/dialect_mysql.go` line 59 (`GetTableSchema`)
**Before:**
```go
rs, err := db.Query(fmt.Sprintf("show create table `%s`", tableName))
```
**After:**
```go
rs, err := db.Query(fmt.Sprintf("show create table `%s`", mysqlQuoteIdent(tableName)))
```
`tableName` originates from the DB's own table enumeration. This is a read-only query; no unit test is added (requires live DB). `mysqlQuoteIdent` is already tested in `TestMySQLQuoteIdent`.

### Site 2 — `internal/db.go` line 54 (`FieldInfo.String()`)
**Before:**
```go
parts = append(parts, fmt.Sprintf("`%s` %s", f.ColumnName, f.ColumnType))
```
**After:**
```go
parts = append(parts, fmt.Sprintf("`%s` %s", mysqlQuoteIdent(f.ColumnName), f.ColumnType))
```
`f.ColumnType` is NOT escaped — it is a type expression, not a quoted identifier.

## PG Path Audit: Does Any PG Code Call `FieldInfo.String()`?

Grep result for `.String()` in `internal/*.go`:
- `internal/dialect_mysql.go:233`: `return field.String()` — `MySQLDialect.FieldDef()` calls it.
- `internal/dialect_mysql.go:303`: `src.String()` — `MySQLDialect.GenChangeColumn()` calls it.
- `internal/schema.go:36`: `info.String()` — inside `MySchema.String()` debug printer (MySQL schema only).
- `internal/dialect_pg.go`: NO reference to `FieldInfo.String()`. `PostgresDialect.FieldDef()` (line 544) builds its own string using double-quote syntax, completely independent of `FieldInfo.String()`.

**Conclusion:** `FieldInfo.String()` is exclusively MySQL output (backtick-quoted form). Escaping `f.ColumnName` here does NOT affect any PostgreSQL path.

## TDD: RED → GREEN

**RED (before fix):**
```
--- FAIL: TestFieldInfo_String_BacktickEscape (0.00s)
    field_test.go:426: Not equal:
         expected: "`we``ird` int NULL"
           actual: "`we`ird` int NULL"
```

**GREEN (after fix in `db.go`):**
```
--- PASS: TestFieldInfo_String_BacktickEscape (0.00s)
```

Test added to `internal/field_test.go` as `TestFieldInfo_String_BacktickEscape`, asserting:
```go
FieldInfo{ColumnName: "we`ird", ColumnType: "int", IsNullAble: "YES"}.String()
// → "`we``ird` int NULL"
```

## Golden Files Unchanged

`git diff --name-only -- internal/testdata/` produced no output. All testdata files are byte-identical. Full `go test ./internal/...` PASS.

## Concerns

None. Both changes are minimal, reuse the existing `mysqlQuoteIdent` helper, and are a no-op for normal names (no backtick in column/table name), so the change is safe for all existing schemas.
