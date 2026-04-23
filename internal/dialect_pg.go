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
	// 1. Get columns.
	// pg_get_serial_sequence 用于识别 owned sequence：当列默认值为 nextval(<owned_seq>::regclass)
	// 且 sequence 归属当前列时，该列本质上就是 serial/bigserial/smallserial，我们在输出时
	// 折叠为 SERIAL 族类型并丢弃 DEFAULT，让目标库自动建 sequence，避免 relation not exists。
	colQuery := `
		SELECT
			a.attname,
			pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
			a.attnotnull,
			pg_get_expr(d.adbin, d.adrelid) AS default_value,
			pg_get_serial_sequence(quote_ident($1), a.attname) AS owned_seq
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
		var defaultValue, ownedSeq sql.NullString
		if err := colRows.Scan(&colName, &dataType, &notNull, &defaultValue, &ownedSeq); err != nil {
			return "", err
		}

		if ownedSeq.Valid && defaultValue.Valid && strings.Contains(defaultValue.String, "nextval(") {
			if serialType, ok := pgSerialTypeFor(dataType); ok {
				dataType = serialType
				defaultValue = sql.NullString{}
			}
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

	// 2. Get constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK).
	// 注意：PostgreSQL 17+ 把列级 NOT NULL 也作为命名约束（contype='n'）暴露在 pg_constraint 中，
	// 与列定义里的 NOT NULL 语义重复，需排除，避免在 CREATE TABLE 中生成冗余/不兼容旧版本的
	// "CONSTRAINT xxx NOT NULL col" 子句。
	conQuery := `
		SELECT
			conname,
			contype,
			pg_get_constraintdef(c.oid) AS condef
		FROM pg_constraint c
		JOIN pg_class t ON c.conrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		WHERE t.relname = $1 AND n.nspname = 'public' AND c.contype <> 'n'
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

// pgSerialTypeFor 将 owned-sequence 支撑的整型列折叠为 SERIAL 族伪类型。
// 这样 CREATE TABLE 输出时 PostgreSQL 会自动按 "<table>_<column>_seq" 规则建 sequence，
// 避免生成的 DDL 依赖尚不存在的 sequence。
func pgSerialTypeFor(dataType string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "bigint", "int8":
		return "bigserial", true
	case "integer", "int", "int4":
		return "serial", true
	case "smallint", "int2":
		return "smallserial", true
	default:
		return "", false
	}
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
	pgPrimaryKeyReg      = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+PRIMARY\s+KEY\s*\((.+)\)`)
	pgUniqueKeyReg       = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+UNIQUE\s*\((.+)\)`)
	pgForeignKeyReg      = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+FOREIGN\s+KEY\s*\(.+\)\s+REFERENCES\s+"([^"]+)"`)
	pgCheckReg           = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+CHECK\s*\((.+)\)`)
	pgNotNullReg         = regexp.MustCompile(`(?i)^CONSTRAINT\s+"([^"]+)"\s+NOT\s+NULL(?:\s+|$)`)
	pgConstraintPrefixRe = regexp.MustCompile(`(?i)^CONSTRAINT\s+"[^"]+"\s+`)
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

	// Skip NOT NULL constraints — already represented in column definitions
	if pgNotNullReg.MatchString(line) {
		return nil
	}

	// Strip CONSTRAINT "name" prefix to store only the definition part (e.g. PRIMARY KEY (...), UNIQUE (...))
	// This avoids duplication when GenAddIndex prepends ADD CONSTRAINT "name"
	defSQL := pgConstraintPrefixRe.ReplaceAllString(line, "")
	idx := &DbIndex{SQL: defSQL, RelationTables: []string{}}

	if matches := pgPrimaryKeyReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = indexTypePrimary
		idx.Name = matches[1]
		return idx
	}

	if matches := pgUniqueKeyReg.FindStringSubmatch(line); len(matches) > 0 {
		idx.IndexType = indexTypeUnique
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

	// Defensive: strip any residual CONSTRAINT "name" prefix from idx.SQL
	defSQL := pgConstraintPrefixRe.ReplaceAllString(idx.SQL, "")

	switch idx.IndexType {
	case indexTypePrimary, indexTypeUnique, checkConstraint:
		sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, defSQL))
	case indexTypeIndex:
		// 对于 IndexEnumerator 返回的普通索引，idx.SQL 已是完整 CREATE INDEX ... 语句
		// （支持 USING btree/gin/hnsw/…，以及 WHERE 条件和表达式索引），原样使用即可；
		// 旧路径下 defSQL 只是列表达式，则按 btree 拼接兼容。
		upperDef := strings.ToUpper(strings.TrimSpace(defSQL))
		if strings.HasPrefix(upperDef, "CREATE INDEX") || strings.HasPrefix(upperDef, "CREATE UNIQUE INDEX") {
			sqls = append(sqls, ensureSemicolon(defSQL))
		} else {
			sqls = append(sqls, fmt.Sprintf(`CREATE INDEX %q ON "%s" USING btree (%s);`, idx.Name, tableName, defSQL))
		}
	}

	return sqls
}

func ensureSemicolon(sql string) string {
	s := strings.TrimSpace(sql)
	if !strings.HasSuffix(s, ";") {
		s += ";"
	}
	return s
}

// pgCanonicalWhitespace 把任意空白（空格/制表符/换行）连续块折叠为单个空格，
// 并去掉首尾空白。用于幂等比较：消除 PG 不同历史/版本 round-trip 引入的缩进与
// 换行噪声。注意不处理 AST 层差异（如 ANY(ARRAY[...]::text[]) 与 ANY(ARRAY[...::text])
// 这类等价但语法结构不同的表达式），此类仍会判为"不等"。
func pgCanonicalWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// DefinitionsEqual 比较前折叠空白。若两段 DDL 仅在缩进、换行上不同（典型 case：
// 源库是用户手写 DDL，目标库被工具 CREATE OR REPLACE 过后 PG 重新序列化），会被
// 判定为相等，避免反复触发重建。
func (p *PostgresDialect) DefinitionsEqual(a, b string) bool {
	if a == b {
		return true
	}
	return pgCanonicalWhitespace(a) == pgCanonicalWhitespace(b)
}

// GetTableTriggers implements TriggerEnumerator: 枚举指定表上的用户触发器。
// 通过 NOT tgisinternal 过滤 PostgreSQL 为外键等内部维护的触发器；
// Definition 使用 pg_get_triggerdef 的 pretty 输出，可直接在目标库重放。
func (p *PostgresDialect) GetTableTriggers(db *sql.DB, tableName string) ([]*DbTrigger, error) {
	const q = `
		SELECT t.tgname, pg_get_triggerdef(t.oid, true) AS def
		FROM pg_trigger t
		JOIN pg_class c ON c.oid = t.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
		  AND c.relname = $1
		  AND NOT t.tgisinternal
		ORDER BY t.tgname`
	rows, err := db.Query(q, tableName)
	if err != nil {
		return nil, fmt.Errorf("pg get triggers for %q: %w", tableName, err)
	}
	defer rows.Close()

	var result []*DbTrigger
	for rows.Next() {
		var name, def string
		if err := rows.Scan(&name, &def); err != nil {
			return nil, err
		}
		result = append(result, &DbTrigger{
			Name:       name,
			Table:      tableName,
			Definition: def,
		})
	}
	return result, rows.Err()
}

// GetTableComment implements TableCommentEnumerator: 读取 public schema 下表的
// COMMENT ON TABLE 文本，空字符串表示无注释。
func (p *PostgresDialect) GetTableComment(db *sql.DB, tableName string) (string, error) {
	const q = `
		SELECT COALESCE(d.description, '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
		WHERE n.nspname = 'public' AND c.relname = $1 AND c.relkind = 'r'`
	var comment string
	err := db.QueryRow(q, tableName).Scan(&comment)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return comment, err
}

// GenCommentTableSQL 生成 COMMENT ON TABLE 语句；空字符串时清除注释。
func (p *PostgresDialect) GenCommentTableSQL(tableName, comment string) string {
	if comment == "" {
		return fmt.Sprintf(`COMMENT ON TABLE %q IS NULL;`, tableName)
	}
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf(`COMMENT ON TABLE %q IS '%s';`, tableName, escaped)
}

// TableCommentInline 返回 false：PostgreSQL 的 CREATE TABLE 不含表注释子句，
// 必须用独立的 COMMENT ON TABLE 语句设置。
func (p *PostgresDialect) TableCommentInline() bool { return false }

// GenDropTrigger 生成 DROP TRIGGER 语句；trigger 在 PostgreSQL 中与其宿主表绑定。
func (p *PostgresDialect) GenDropTrigger(trg *DbTrigger) string {
	return fmt.Sprintf(`DROP TRIGGER IF EXISTS %q ON %q;`, trg.Name, trg.Table)
}

// GenAddTrigger 直接重放 pg_get_triggerdef 的完整 CREATE TRIGGER DDL。
func (p *PostgresDialect) GenAddTrigger(trg *DbTrigger) string {
	return ensureSemicolon(trg.Definition)
}

// GetFunctions implements FunctionEnumerator: 枚举 public schema 下的用户自定义函数。
// 通过 pg_depend deptype='e' 排除属于 extension 的函数（pgvector、pgcrypto 等），
// Definition 直接使用 pg_get_functiondef 输出的 CREATE OR REPLACE FUNCTION 语句。
func (p *PostgresDialect) GetFunctions(db *sql.DB) ([]*DbFunction, error) {
	const q = `
		SELECT p.proname,
		       pg_get_function_identity_arguments(p.oid) AS args,
		       pg_get_functiondef(p.oid) AS def
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = 'public'
		  AND NOT EXISTS (
		    SELECT 1 FROM pg_depend d
		    WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
		  )
		  AND p.prokind = 'f'
		ORDER BY p.proname, args`
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("pg get functions: %w", err)
	}
	defer rows.Close()

	var result []*DbFunction
	for rows.Next() {
		var name, args, def string
		if err := rows.Scan(&name, &args, &def); err != nil {
			return nil, err
		}
		result = append(result, &DbFunction{
			Name:       name,
			Signature:  args,
			Definition: def,
		})
	}
	return result, rows.Err()
}

// GenDropFunction 生成带签名的 DROP FUNCTION；签名保证对重载函数的精准定位。
func (p *PostgresDialect) GenDropFunction(fn *DbFunction) string {
	return fmt.Sprintf(`DROP FUNCTION IF EXISTS %q(%s);`, fn.Name, fn.Signature)
}

// GenAddFunction 直接重放 pg_get_functiondef 的完整 CREATE OR REPLACE FUNCTION DDL。
func (p *PostgresDialect) GenAddFunction(fn *DbFunction) string {
	return ensureSemicolon(fn.Definition)
}

// GetExtensions implements ExtensionEnumerator: 枚举已安装的扩展。
// plpgsql 是 PostgreSQL 默认语言扩展，标准模板库会自带，跳过比对避免噪声。
func (p *PostgresDialect) GetExtensions(db *sql.DB) ([]*DbExtension, error) {
	const q = `
		SELECT extname, extversion
		FROM pg_extension
		WHERE extname NOT IN ('plpgsql')
		ORDER BY extname`
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("pg get extensions: %w", err)
	}
	defer rows.Close()

	var result []*DbExtension
	for rows.Next() {
		var name, version string
		if err := rows.Scan(&name, &version); err != nil {
			return nil, err
		}
		result = append(result, &DbExtension{Name: name, Version: version})
	}
	return result, rows.Err()
}

// GenAddExtension 用 IF NOT EXISTS 避免重复创建报错。
func (p *PostgresDialect) GenAddExtension(ext *DbExtension) string {
	return fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %q;`, ext.Name)
}

// GenDropExtension 用 IF EXISTS 保持幂等。注意 DROP EXTENSION 会级联删除
// 该扩展提供的所有对象（类型/函数/操作符/索引方法等），仅在 cfg.Drop=true
// 时才会被调用。
func (p *PostgresDialect) GenDropExtension(ext *DbExtension) string {
	return fmt.Sprintf(`DROP EXTENSION IF EXISTS %q;`, ext.Name)
}

// GetTableIndexes implements IndexEnumerator: 枚举非约束索引（通过 pg_constraint.conindid
// 排除由 PK/UNIQUE/EXCLUDE 约束占用的物理索引），返回带完整 CREATE INDEX DDL 的 DbIndex 列表。
// 同时一并读取索引级 COMMENT（pg_description.objsubid=0），保存到 DbIndex.Comment。
func (p *PostgresDialect) GetTableIndexes(db *sql.DB, tableName string) ([]*DbIndex, error) {
	const q = `
		SELECT ic.relname AS indexname,
		       pg_get_indexdef(ic.oid) AS indexdef,
		       COALESCE(d.description, '') AS indexcomment
		FROM pg_class ic
		JOIN pg_index i ON ic.oid = i.indexrelid
		JOIN pg_class t ON i.indrelid = t.oid
		JOIN pg_namespace n ON t.relnamespace = n.oid
		LEFT JOIN pg_constraint con ON con.conindid = ic.oid
		LEFT JOIN pg_description d ON d.objoid = ic.oid AND d.objsubid = 0
		WHERE n.nspname = 'public'
		  AND t.relname = $1
		  AND ic.relkind = 'i'
		  AND con.oid IS NULL
		ORDER BY ic.relname`
	rows, err := db.Query(q, tableName)
	if err != nil {
		return nil, fmt.Errorf("pg get indexes for %q: %w", tableName, err)
	}
	defer rows.Close()

	var result []*DbIndex
	for rows.Next() {
		var name, def, comment string
		if err := rows.Scan(&name, &def, &comment); err != nil {
			return nil, err
		}
		result = append(result, &DbIndex{
			IndexType:      indexTypeIndex,
			Name:           name,
			SQL:            def,
			Comment:        comment,
			RelationTables: []string{},
		})
	}
	return result, rows.Err()
}

// GenCommentIndexSQL 生成 COMMENT ON INDEX 语句；空字符串时清除注释。
func (p *PostgresDialect) GenCommentIndexSQL(indexName, comment string) string {
	if comment == "" {
		return fmt.Sprintf(`COMMENT ON INDEX %q IS NULL;`, indexName)
	}
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf(`COMMENT ON INDEX %q IS '%s';`, indexName, escaped)
}

func (p *PostgresDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	switch idx.IndexType {
	case indexTypePrimary, checkConstraint, indexTypeUnique:
		return fmt.Sprintf(`DROP CONSTRAINT "%s"`, idx.Name)
	case indexTypeIndex:
		return fmt.Sprintf(`DROP INDEX "%s";`, idx.Name)
	}
	return ""
}

func (p *PostgresDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	if needDrop {
		sqls = append(sqls, p.GenDropForeignKey(tableName, idx))
	}
	// Defensive: strip any residual CONSTRAINT "name" prefix from idx.SQL
	defSQL := pgConstraintPrefixRe.ReplaceAllString(idx.SQL, "")
	sqls = append(sqls, fmt.Sprintf("ADD CONSTRAINT %q %s", idx.Name, defSQL))
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
