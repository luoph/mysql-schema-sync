package internal

import (
	"database/sql"
	"strings"
)

// Dialect abstracts database-specific operations for schema sync
type Dialect interface {
	// DriverName returns the database/sql driver name ("mysql" or "pgx")
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
