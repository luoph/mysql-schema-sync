package internal

import (
	"database/sql"
	"fmt"
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
	// TODO: reconstruct CREATE TABLE DDL from pg_catalog
	return "", fmt.Errorf("PostgresDialect.GetTableSchema not yet implemented")
}

func (p *PostgresDialect) GetTableFields(db *sql.DB, dbName, tableName string) (map[string]*FieldInfo, error) {
	// TODO: query information_schema.columns for PostgreSQL
	return nil, fmt.Errorf("PostgresDialect.GetTableFields not yet implemented")
}

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
	// TODO: implement PostgreSQL index/constraint line parsing
	return nil
}

func (p *PostgresDialect) CleanTableSchema(schema string) string {
	return schema // PostgreSQL DDL has no ENGINE clause
}

func (p *PostgresDialect) Quote(name string) string {
	return `"` + name + `"`
}

func (p *PostgresDialect) FieldsEqual(a, b *FieldInfo) bool {
	// TODO: implement PostgreSQL-specific field comparison with type alias normalization
	if a == nil || b == nil {
		return a == b
	}
	return a.ColumnName == b.ColumnName &&
		a.IsNullAble == b.IsNullAble &&
		a.ColumnType == b.ColumnType
}

func (p *PostgresDialect) FieldDef(field *FieldInfo) string {
	// TODO: implement PostgreSQL column definition formatting
	return fmt.Sprintf(`"%s" %s`, field.ColumnName, field.ColumnType)
}

func (p *PostgresDialect) SupportsColumnOrder() bool { return false }

func (p *PostgresDialect) GenAddColumn(colDef, afterCol string, isFirst bool, fieldCount int) string {
	return "ADD COLUMN " + colDef
}

func (p *PostgresDialect) GenChangeColumn(fieldName string, src, dst *FieldInfo) []string {
	// TODO: implement PostgreSQL column change (ALTER COLUMN TYPE/SET NOT NULL/SET DEFAULT)
	return nil
}

func (p *PostgresDialect) GenChangeColumnText(fieldName, colDef string) string {
	return "" // PostgreSQL always uses structured comparison
}

func (p *PostgresDialect) GenDropColumn(colName string) string {
	return fmt.Sprintf(`DROP COLUMN "%s"`, colName)
}

func (p *PostgresDialect) GenAddIndex(tableName string, idx *DbIndex, needDrop bool) []string {
	// TODO: implement PostgreSQL index creation (standalone CREATE INDEX)
	return nil
}

func (p *PostgresDialect) GenDropIndex(tableName string, idx *DbIndex) string {
	// TODO: implement PostgreSQL index drop (standalone DROP INDEX)
	return ""
}

func (p *PostgresDialect) GenAddForeignKey(tableName string, idx *DbIndex, needDrop bool) []string {
	// TODO: implement
	return nil
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
		return ""
	}
	escaped := strings.ReplaceAll(comment, "'", "''")
	return fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS '%s';`, tableName, colName, escaped)
}

func (p *PostgresDialect) WrapAlterSQL(tableName string, clauses []string, singleChange bool) []string {
	var result []string
	for _, clause := range clauses {
		result = append(result, fmt.Sprintf(`ALTER TABLE "%s" %s;`, tableName, clause))
	}
	return result
}
