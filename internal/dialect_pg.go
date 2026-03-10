package internal

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// PostgresDialect implements Dialect for PostgreSQL databases
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

func (p *PostgresDialect) GetTableSchema(db *sql.DB, dbName, tableName string) (string, error) {
	// 1. Get columns
	colQuery := `
		SELECT
			a.attname,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			a.attnotnull,
			pg_get_expr(d.adbin, d.adrelid) AS default_value
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE a.attrelid = $1::regclass
			AND a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY a.attnum`

	colRows, err := db.Query(colQuery, tableName)
	if err != nil {
		return "", fmt.Errorf("pg get columns for %q: %w", tableName, err)
	}
	defer colRows.Close()

	var colDefs []string
	for colRows.Next() {
		var colName, dataType string
		var notNull bool
		var defaultValue sql.NullString
		if err := colRows.Scan(&colName, &dataType, &notNull, &defaultValue); err != nil {
			return "", err
		}
		def := fmt.Sprintf("  %q %s", colName, dataType)
		if notNull {
			def += " NOT NULL"
		}
		if defaultValue.Valid {
			def += " DEFAULT " + defaultValue.String
		}
		colDefs = append(colDefs, def)
	}
	if err := colRows.Err(); err != nil {
		return "", err
	}
	if len(colDefs) == 0 {
		return "", fmt.Errorf("table %q not found or has no columns", tableName)
	}

	// 2. Get constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)
	conQuery := `
		SELECT
			conname,
			contype,
			pg_get_constraintdef(c.oid) AS condef
		FROM pg_constraint c
		JOIN pg_class t ON c.conrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE t.relname = $1 AND n.nspname = 'public'
		ORDER BY
			CASE contype
				WHEN 'p' THEN 1
				WHEN 'u' THEN 2
				WHEN 'f' THEN 3
				WHEN 'c' THEN 4
				ELSE 5
			END,
			conname`

	conRows, err := db.Query(conQuery, tableName)
	if err != nil {
		return "", fmt.Errorf("pg get constraints for %q: %w", tableName, err)
	}
	defer conRows.Close()

	var constraintDefs []string
	for conRows.Next() {
		var conName, conType, conDef string
		if err := conRows.Scan(&conName, &conType, &conDef); err != nil {
			return "", err
		}
		constraintDefs = append(constraintDefs, fmt.Sprintf("  CONSTRAINT %q %s", conName, conDef))
	}
	if err := conRows.Err(); err != nil {
		return "", err
	}

	// Build CREATE TABLE
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %q (\n", tableName))
	allDefs := append(colDefs, constraintDefs...)
	sb.WriteString(strings.Join(allDefs, ",\n"))
	sb.WriteString("\n)")

	return sb.String(), nil
}

func (p *PostgresDialect) GetTableFields(db *sql.DB, dbName, tableName string) (map[string]*FieldInfo, error) {
	const query = `
		SELECT
			c.column_name,
			c.ordinal_position,
			c.column_default,
			c.is_nullable,
			c.data_type,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.character_set_name,
			c.collation_name,
			c.udt_name,
			COALESCE(
				pgd.description, ''
			) AS column_comment
		FROM information_schema.columns c
		LEFT JOIN pg_catalog.pg_statio_all_tables st
			ON c.table_schema = st.schemaname AND c.table_name = st.relname
		LEFT JOIN pg_catalog.pg_description pgd
			ON pgd.objoid = st.relid
			AND pgd.objsubid = c.ordinal_position
		WHERE c.table_schema = 'public' AND c.table_name = $1
		ORDER BY c.ordinal_position`

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("pg query columns for %q: %w", tableName, err)
	}
	defer rows.Close()

	fields := make(map[string]*FieldInfo)
	for rows.Next() {
		field := &FieldInfo{}
		var charMaxLen, numericPrecision, numericScale sql.NullInt64
		var charset, collation, columnDefault sql.NullString
		var udtName string
		err := rows.Scan(
			&field.ColumnName, &field.OrdinalPosition, &columnDefault,
			&field.IsNullAble, &field.DataType, &charMaxLen,
			&numericPrecision, &numericScale, &charset, &collation,
			&udtName, &field.ColumnComment,
		)
		if err != nil {
			return nil, fmt.Errorf("pg scan field for %q: %w", tableName, err)
		}

		// Build ColumnType from data_type and udt_name
		field.ColumnType = pgBuildColumnType(field.DataType, udtName, charMaxLen, numericPrecision, numericScale)

		if columnDefault.Valid {
			cleaned := pgCleanDefault(columnDefault.String)
			field.ColumnDefault = &cleaned
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
		// Detect serial columns (nextval default)
		if columnDefault.Valid && strings.Contains(columnDefault.String, "nextval(") {
			field.Extra = "auto_increment"
		}
		fields[field.ColumnName] = field
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("no fields found for %q", tableName)
	}
	return fields, nil
}

// pgBuildColumnType constructs the column type string from PG information_schema data
func pgBuildColumnType(dataType, udtName string, charMaxLen, numericPrecision, numericScale sql.NullInt64) string {
	dt := strings.ToLower(dataType)
	switch dt {
	case "character varying":
		if charMaxLen.Valid {
			return fmt.Sprintf("varchar(%d)", charMaxLen.Int64)
		}
		return "varchar"
	case "character":
		if charMaxLen.Valid {
			return fmt.Sprintf("char(%d)", charMaxLen.Int64)
		}
		return "char"
	case "numeric":
		if numericPrecision.Valid && numericScale.Valid {
			return fmt.Sprintf("numeric(%d,%d)", numericPrecision.Int64, numericScale.Int64)
		}
		if numericPrecision.Valid {
			return fmt.Sprintf("numeric(%d)", numericPrecision.Int64)
		}
		return "numeric"
	case "integer":
		return "integer"
	case "smallint":
		return "smallint"
	case "bigint":
		return "bigint"
	case "boolean":
		return "boolean"
	case "text":
		return "text"
	case "real":
		return "real"
	case "double precision":
		return "double precision"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone":
		return "timestamptz"
	case "date":
		return "date"
	case "time without time zone":
		return "time"
	case "time with time zone":
		return "timetz"
	case "bytea":
		return "bytea"
	case "json":
		return "json"
	case "jsonb":
		return "jsonb"
	case "uuid":
		return "uuid"
	case "array":
		return pgArrayType(udtName)
	case "user-defined":
		return udtName
	default:
		return dt
	}
}

// pgArrayType converts PostgreSQL internal array type names to readable form
func pgArrayType(udtName string) string {
	base := strings.TrimPrefix(udtName, "_")
	normalized := pgNormalizeTypeName(base)
	return normalized + "[]"
}

// pgCleanDefault strips PostgreSQL type casts from default values
var pgTypeCastReg = regexp.MustCompile(`^(.+?)::[\w\s\[\]]+$`)

func pgCleanDefault(defaultVal string) string {
	if matches := pgTypeCastReg.FindStringSubmatch(defaultVal); len(matches) > 1 {
		return matches[1]
	}
	return defaultVal
}

// pgNormalizeTypeName normalizes PostgreSQL type aliases to canonical form
func pgNormalizeTypeName(name string) string {
	switch strings.ToLower(name) {
	case "int2":
		return "smallint"
	case "int4":
		return "integer"
	case "int8":
		return "bigint"
	case "float4":
		return "real"
	case "float8":
		return "double precision"
	case "bool":
		return "boolean"
	case "varchar":
		return "character varying"
	case "char":
		return "character"
	case "timestamptz":
		return "timestamp with time zone"
	case "timetz":
		return "time with time zone"
	default:
		return name
	}
}

// pgNormalizeColumnType normalizes a column type for comparison
func pgNormalizeColumnType(colType string) string {
	lower := strings.ToLower(strings.TrimSpace(colType))

	// Handle types with parameters like varchar(100), numeric(10,2)
	if idx := strings.Index(lower, "("); idx > 0 {
		basePart := lower[:idx]
		paramPart := lower[idx:]
		normalized := pgNormalizeTypeName(basePart)
		return normalized + paramPart
	}

	// Handle array types
	if strings.HasSuffix(lower, "[]") {
		basePart := strings.TrimSuffix(lower, "[]")
		return pgNormalizeTypeName(basePart) + "[]"
	}

	return pgNormalizeTypeName(lower)
}

// PostgreSQL constraint/index line parsing regexes
var (
	pgPrimaryKeyReg = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+PRIMARY\s+KEY\s*\((.+)\)`)
	pgUniqueKeyReg  = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+UNIQUE\s*\((.+)\)`)
	pgForeignKeyReg = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+FOREIGN\s+KEY\s*\(.+\)\s+REFERENCES\s+"([^"]+)"`)
	pgCheckReg      = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+CHECK\s*\((.+)\)`)
)

func (p *PostgresDialect) ParseSchema(schema string) *MySchema {
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
		if line[0] == '"' {
			index := strings.Index(line[1:], "\"")
			if index < 0 {
				continue
			}
			name := line[1 : index+1]
			mys.Fields.Set(name, line)
		} else {
			idx := p.parsePgIndexLine(line)
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

func (p *PostgresDialect) parsePgIndexLine(line string) *DbIndex {
	line = strings.TrimSpace(line)
	idx := &DbIndex{SQL: line, RelationTables: []string{}}

	if matches := pgPrimaryKeyReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = indexTypePrimary
		idx.Name = matches[1]
		return idx
	}

	if matches := pgUniqueKeyReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = indexTypeIndex
		idx.Name = matches[1]
		return idx
	}

	if matches := pgForeignKeyReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = indexTypeForeignKey
		idx.Name = matches[1]
		idx.addRelationTable(matches[2])
		return idx
	}

	if matches := pgCheckReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = checkConstraint
		idx.Name = matches[1]
		return idx
	}

	log.Printf("[Warning] PostgreSQL index parse: unsupported line: %s", line)
	return nil
}

func (p *PostgresDialect) CleanTableSchema(schema string) string {
	return schema
}

func (p *PostgresDialect) Quote(name string) string {
	return `"` + name + `"`
}

func (p *PostgresDialect) FieldsEqual(a, b *FieldInfo) bool {
	if a == nil || b == nil {
		return a == b
	}

	if a.ColumnName != b.ColumnName {
		return false
	}

	if pgNormalizeColumnType(a.ColumnType) != pgNormalizeColumnType(b.ColumnType) {
		return false
	}

	if a.IsNullAble != b.IsNullAble {
		return false
	}

	// Compare defaults (both cleaned)
	if (a.ColumnDefault == nil) != (b.ColumnDefault == nil) {
		return false
	}
	if a.ColumnDefault != nil && b.ColumnDefault != nil {
		cleanA := pgCleanDefault(*a.ColumnDefault)
		cleanB := pgCleanDefault(*b.ColumnDefault)
		if cleanA != cleanB {
			return false
		}
	}

	if a.ColumnComment != b.ColumnComment {
		return false
	}

	return true
}

func (p *PostgresDialect) FieldDef(field *FieldInfo) string {
	var parts []string

	parts = append(parts, fmt.Sprintf(`"%s" %s`, field.ColumnName, field.ColumnType))

	if strings.ToUpper(field.IsNullAble) == "NO" {
		parts = append(parts, "NOT NULL")
	}

	if field.ColumnDefault != nil && field.Extra != "auto_increment" {
		parts = append(parts, "DEFAULT "+*field.ColumnDefault)
	}

	return strings.Join(parts, " ")
}

func (p *PostgresDialect) SupportsColumnOrder() bool { return false }

func (p *PostgresDialect) GenAddColumn(colDef, afterCol string, isFirst bool, fieldCount int) string {
	return "ADD COLUMN " + colDef
}

func (p *PostgresDialect) GenChangeColumn(fieldName string, src, dst *FieldInfo) []string {
	var clauses []string

	srcType := pgNormalizeColumnType(src.ColumnType)
	dstType := pgNormalizeColumnType(dst.ColumnType)
	if srcType != dstType {
		clauses = append(clauses, fmt.Sprintf(`ALTER COLUMN "%s" TYPE %s`, fieldName, src.ColumnType))
	}

	if src.IsNullAble != dst.IsNullAble {
		if strings.ToUpper(src.IsNullAble) == "NO" {
			clauses = append(clauses, fmt.Sprintf(`ALTER COLUMN "%s" SET NOT NULL`, fieldName))
		} else {
			clauses = append(clauses, fmt.Sprintf(`ALTER COLUMN "%s" DROP NOT NULL`, fieldName))
		}
	}

	srcDefault := ""
	dstDefault := ""
	if src.ColumnDefault != nil {
		srcDefault = pgCleanDefault(*src.ColumnDefault)
	}
	if dst.ColumnDefault != nil {
		dstDefault = pgCleanDefault(*dst.ColumnDefault)
	}

	if srcDefault != dstDefault {
		if src.ColumnDefault != nil && src.Extra != "auto_increment" {
			clauses = append(clauses, fmt.Sprintf(`ALTER COLUMN "%s" SET DEFAULT %s`, fieldName, *src.ColumnDefault))
		} else if src.ColumnDefault == nil {
			clauses = append(clauses, fmt.Sprintf(`ALTER COLUMN "%s" DROP DEFAULT`, fieldName))
		}
	}

	return clauses
}

func (p *PostgresDialect) GenChangeColumnText(fieldName, colDef string) string {
	return ""
}

func (p *PostgresDialect) GenDropColumn(colName string) string {
	return fmt.Sprintf(`DROP COLUMN "%s"`, colName)
}

func (p *PostgresDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string

	if needDrop {
		dropSQL := p.GenDropIndex(tableName, idx)
		if dropSQL != "" {
			sqls = append(sqls, dropSQL)
		}
	}

	switch idx.IndexType {
	case indexTypePrimary:
		sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, idx.SQL))
	case indexTypeIndex:
		if strings.Contains(strings.ToUpper(idx.SQL), "UNIQUE") {
			sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, idx.SQL))
		} else {
			sqls = append(sqls, fmt.Sprintf(`CREATE INDEX %q ON "%s" USING btree (%s);`, idx.Name, tableName, idx.SQL))
		}
	case checkConstraint:
		sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, idx.SQL))
	}

	return sqls
}

func (p *PostgresDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	switch idx.IndexType {
	case indexTypePrimary, checkConstraint:
		return fmt.Sprintf(`DROP CONSTRAINT "%s"`, idx.Name)
	case indexTypeIndex:
		if strings.Contains(strings.ToUpper(idx.SQL), "UNIQUE") {
			return fmt.Sprintf(`DROP CONSTRAINT "%s"`, idx.Name)
		}
		return fmt.Sprintf(`DROP INDEX "%s";`, idx.Name)
	}
	return ""
}

func (p *PostgresDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	if needDrop {
		sqls = append(sqls, p.GenDropForeignKey(tableName, idx))
	}
	sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, idx.SQL))
	return sqls
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
	if comment == "" {
		return fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS NULL;`, tableName, colName)
	}
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS '%s';`, tableName, colName, escaped)
}

func (p *PostgresDialect) WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string {
	if len(clauses) == 0 {
		return nil
	}
	var result []string
	for _, clause := range clauses {
		result = append(result, fmt.Sprintf(`ALTER TABLE "%s" %s;`, tableName, clause))
	}
	return result
}
