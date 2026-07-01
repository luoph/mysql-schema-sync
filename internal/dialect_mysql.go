package internal

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
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
	rs, err := db.Query(fmt.Sprintf("show create table `%s`", mysqlQuoteIdent(tableName)))
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
		SELECT
			COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE,
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

// MySQL index parsing regex
var (
	mysqlIndexReg      = regexp.MustCompile(`^([A-Z]+\s)?KEY\s`)
	mysqlForeignKeyReg = regexp.MustCompile("^CONSTRAINT `(.+)` FOREIGN KEY.+ REFERENCES `(.+)` ")
	// MySQL 8.0+ 规范化输出 CHECK 时会包裹双层括号 "CHECK ((expr))"，但用户
	// 自定义表达式也可能是单层 "CHECK (expr)"；放宽为只要求外层一对括号，
	// 内部表达式可以包含任意括号嵌套。
	mysqlCheckConstraintReg = regexp.MustCompile("^CONSTRAINT `([^`]+)` CHECK \\((.+)\\)$")
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
		// 识别 UNIQUE KEY 以便生成更准确的日志/调试输出；FULLTEXT / SPATIAL KEY
		// 仍归入 indexTypeIndex（执行路径直接重放 idx.SQL，不受类型标签影响）。
		if strings.HasPrefix(strings.ToUpper(line), "UNIQUE") {
			idx.IndexType = indexTypeUnique
		} else {
			idx.IndexType = indexTypeIndex
		}
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
	// 只剥除 AUTO_INCREMENT=数字（受数据行计数驱动，与结构无关），保留 ENGINE /
	// CHARSET / COLLATE / COMMENT / PARTITION BY 等尾部属性参与比对。当前仅实现
	// TABLE COMMENT 的 ALTER 生成，其他属性差异不会被转换为 ALTER DDL，但仍能触发
	// 结构化差异检测；未来补引擎/字符集同步时无需再改 CleanTableSchema。
	return mysqlAutoIncrReg.ReplaceAllString(schema, " ")
}

func (m *MySQLDialect) Quote(name string) string {
	return "`" + mysqlQuoteIdent(name) + "`"
}

func (m *MySQLDialect) FieldsEqual(a, b *FieldInfo) bool {
	return a.Equals(b)
}

func (m *MySQLDialect) FieldDef(field *FieldInfo) string {
	return field.String()
}

func (m *MySQLDialect) SupportsColumnOrder() bool { return true }

// mysqlQuoteIdent 转义反引号包裹的标识符：把名字内部的反引号双写。
// 用于所有由本工具拼接进 `...` 的标识符（表名、列名、索引名、外键名）。
func mysqlQuoteIdent(name string) string {
	return strings.ReplaceAll(name, "`", "``")
}

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
	// 当无反引号时，取首个空白符分隔 token，并去除反引号
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}
	return strings.Trim(fields[0], "`")
}

func (m *MySQLDialect) GenAddColumn(table, colDef, afterCol string, isFirst bool, fieldCount int) []string {
	var ddl string
	switch {
	case afterCol == "" && isFirst:
		ddl = fmt.Sprintf("ALTER TABLE `%s` ADD %s FIRST", mysqlQuoteIdent(table), colDef)
	case afterCol == "":
		ddl = fmt.Sprintf("ALTER TABLE `%s` ADD %s", mysqlQuoteIdent(table), colDef)
	default:
		ddl = fmt.Sprintf("ALTER TABLE `%s` ADD %s AFTER `%s`", mysqlQuoteIdent(table), colDef, mysqlQuoteIdent(afterCol))
	}
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", mysqlEscapeSQLLiteral(table), mysqlEscapeSQLLiteral(mysqlColumnName(colDef)))
	return mysqlGuard("information_schema.COLUMNS", where, ddl, false)
}

func (m *MySQLDialect) GenChangeColumn(fieldName string, src, dst *FieldInfo) []string {
	return []string{fmt.Sprintf("CHANGE `%s` %s", mysqlQuoteIdent(fieldName), src.String())}
}

func (m *MySQLDialect) GenChangeColumnText(fieldName, colDef string) string {
	return fmt.Sprintf("CHANGE `%s` %s", mysqlQuoteIdent(fieldName), colDef)
}

// GenDropColumn 生成裸 DROP COLUMN 子句（不带 information_schema 存在性守卫）。
// 与 ADD/索引/外键 不同，DROP COLUMN 不追求可重跑幂等：diff 已确认该列在目标库
// 存在才会走到这里，直接 DROP 即成功；且返回裸子句可由 WrapAlterSQL 把同表多个
// DROP COLUMN 合并进单条 ALTER TABLE，避免守卫式 prepared statement 的冗长输出。
// 代价：脚本重复执行时列已删会报错，此处不做防护。
func (m *MySQLDialect) GenDropColumn(table, colName string) []string {
	return []string{fmt.Sprintf("DROP COLUMN `%s`", mysqlQuoteIdent(colName))}
}

func (m *MySQLDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	if needDrop {
		sqls = append(sqls, m.GenDropIndexGuard(tableName, idx)...)
	}
	ddl := fmt.Sprintf("ALTER TABLE `%s` %s", mysqlQuoteIdent(tableName), idx.mysqlAddBody())
	from, nameCond := idx.mysqlProbe(mysqlEscapeSQLLiteral)
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND %s", mysqlEscapeSQLLiteral(tableName), nameCond)
	return append(sqls, mysqlGuard(from, where, ddl, false)...)
}

// GenDropIndexGuard 返回带存在性守卫的 DROP INDEX/PRIMARY KEY 语句组（4条）。
func (m *MySQLDialect) GenDropIndexGuard(tableName string, idx *DbIndex) []string {
	ddl := fmt.Sprintf("ALTER TABLE `%s` %s", mysqlQuoteIdent(tableName), idx.mysqlDropBody())
	from, nameCond := idx.mysqlProbe(mysqlEscapeSQLLiteral)
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND %s", mysqlEscapeSQLLiteral(tableName), nameCond)
	return mysqlGuard(from, where, ddl, true)
}

func (m *MySQLDialect) GenDropIndex(tableName string, idx *DbIndex) []string {
	return m.GenDropIndexGuard(tableName, idx)
}

func (m *MySQLDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	var sqls []string
	if needDrop {
		sqls = append(sqls, m.GenDropForeignKeyGuard(tableName, idx)...)
	}
	ddl := fmt.Sprintf("ALTER TABLE `%s` %s", mysqlQuoteIdent(tableName), idx.mysqlAddBody())
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND CONSTRAINT_NAME='%s' AND CONSTRAINT_TYPE='FOREIGN KEY'", mysqlEscapeSQLLiteral(tableName), mysqlEscapeSQLLiteral(idx.Name))
	return append(sqls, mysqlGuard("information_schema.TABLE_CONSTRAINTS", where, ddl, false)...)
}

// GenDropForeignKeyGuard 返回带存在性守卫的 DROP FOREIGN KEY 语句组（4条）。
func (m *MySQLDialect) GenDropForeignKeyGuard(tableName string, idx *DbIndex) []string {
	ddl := fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`", mysqlQuoteIdent(tableName), mysqlQuoteIdent(idx.Name))
	where := fmt.Sprintf("TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND CONSTRAINT_NAME='%s' AND CONSTRAINT_TYPE='FOREIGN KEY'", mysqlEscapeSQLLiteral(tableName), mysqlEscapeSQLLiteral(idx.Name))
	return mysqlGuard("information_schema.TABLE_CONSTRAINTS", where, ddl, true)
}

func (m *MySQLDialect) GenDropForeignKey(tableName string, idx *DbIndex) []string {
	return m.GenDropForeignKeyGuard(tableName, idx)
}

var mysqlAutoIncrReg = regexp.MustCompile(`\sAUTO_INCREMENT=[1-9]\d*\s`)

func (m *MySQLDialect) GenCreateTable(schema string) string {
	s := mysqlAutoIncrReg.ReplaceAllString(schema, " ")
	if up := strings.ToUpper(strings.TrimSpace(s)); strings.HasPrefix(up, "CREATE TABLE ") && !strings.HasPrefix(up, "CREATE TABLE IF NOT EXISTS") {
		trimmed := strings.TrimSpace(s)
		s = "CREATE TABLE IF NOT EXISTS " + trimmed[len("CREATE TABLE "):]
	}
	return s + ";"
}

func (m *MySQLDialect) GenDropTable(tableName string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", mysqlQuoteIdent(tableName))
}

func (m *MySQLDialect) GenCommentColumnSQL(tableName, colName, comment string) string {
	return "" // MySQL handles comments inline in column definition
}

// GetTableComment implements TableCommentEnumerator: 从 information_schema
// 读取表级 COMMENT 文本，空字符串表示无注释。
func (m *MySQLDialect) GetTableComment(db *sql.DB, tableName string) (string, error) {
	const q = `
		SELECT COALESCE(TABLE_COMMENT, '')
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`
	var comment string
	err := db.QueryRow(q, tableName).Scan(&comment)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return comment, err
}

// GenCommentTableSQL 生成 ALTER TABLE ... COMMENT = '...' 语句；空字符串清除注释。
func (m *MySQLDialect) GenCommentTableSQL(tableName, comment string) string {
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf("ALTER TABLE `%s` COMMENT = '%s';", mysqlQuoteIdent(tableName), escaped)
}

// TableCommentInline 返回 true：MySQL 的 CREATE TABLE 已内嵌 COMMENT='...' 子句，
// 整表新建时无需再单独 emit COMMENT 语句。
func (m *MySQLDialect) TableCommentInline() bool { return true }

func (m *MySQLDialect) WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string {
	if len(clauses) == 0 {
		return nil
	}
	escaped := mysqlQuoteIdent(tableName)
	if singleChange {
		var result []string
		for _, clause := range clauses {
			result = append(result, fmt.Sprintf("ALTER TABLE `%s`\n%s;", escaped, clause))
		}
		return result
	}
	return []string{fmt.Sprintf("ALTER TABLE `%s`\n%s;", escaped, strings.Join(clauses, ",\n"))}
}
