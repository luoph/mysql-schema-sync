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
	return a.Equals(b)
}

func (m *MySQLDialect) FieldDef(field *FieldInfo) string {
	return field.String()
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
