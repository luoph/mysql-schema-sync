package internal

import (
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/xanygo/anygo/cli/xcolor"
)

// FieldInfo represents detailed field information from INFORMATION_SCHEMA.COLUMNS
type FieldInfo struct {
	ColumnName             string  `json:"column_name"`
	OrdinalPosition        int     `json:"ordinal_position"`
	ColumnDefault          *string `json:"column_default"`
	IsNullAble             string  `json:"is_nullable"`
	DataType               string  `json:"data_type"`
	CharacterMaximumLength *int    `json:"character_maximum_length"`
	NumericPrecision       *int    `json:"numeric_precision"`
	NumericScale           *int    `json:"numeric_scale"`
	CharsetName            *string `json:"character_set_name"`
	CollationName          *string `json:"collation_name"`
	ColumnType             string  `json:"column_type"`
	ColumnComment          string  `json:"column_comment"`
	Extra                  string  `json:"extra"`
}

// needsQuotedDefault returns true if the field type requires quoted default values
func (f *FieldInfo) needsQuotedDefault() bool {
	// String types that need quoted default values
	stringTypes := []string{
		"char", "varchar", "binary", "varbinary",
		"tinyblob", "blob", "mediumblob", "longblob",
		"tinytext", "text", "mediumtext", "longtext",
		"enum", "set", "json",
	}

	dataType := strings.ToLower(f.DataType)
	return slices.Contains(stringTypes, dataType)
}

// String returns the full column definition as used in MySQL CREATE TABLE
func (f *FieldInfo) String() string {
	var parts []string

	// Column name and type
	parts = append(parts, fmt.Sprintf("`%s` %s", f.ColumnName, f.ColumnType))

	// CHARACTER SET and COLLATION (only for string types where these are non-nil)
	if f.CharsetName != nil {
		parts = append(parts, fmt.Sprintf("CHARACTER SET %s", *f.CharsetName))
	}
	if f.CollationName != nil {
		parts = append(parts, fmt.Sprintf("COLLATE %s", *f.CollationName))
	}

	// NULL/NOT NULL
	if strings.ToUpper(f.IsNullAble) == "NO" {
		parts = append(parts, "NOT NULL")
	} else {
		parts = append(parts, "NULL")
	}

	// Default value
	if f.ColumnDefault != nil {
		defaultValue := *f.ColumnDefault
		upperDefault := strings.ToUpper(defaultValue)

		// Special keywords that don't need quotes
		if upperDefault == "CURRENT_TIMESTAMP" || upperDefault == "NULL" {
			parts = append(parts, fmt.Sprintf("DEFAULT %s", upperDefault))
		} else if f.needsQuotedDefault() {
			// String types need quotes
			parts = append(parts, fmt.Sprintf("DEFAULT '%s'", defaultValue))
		} else {
			// Numeric types don't need quotes
			parts = append(parts, fmt.Sprintf("DEFAULT %s", defaultValue))
		}
	}

	// Extra
	if f.Extra != "" {
		parts = append(parts, strings.ToUpper(f.Extra))
	}

	// Comment
	if f.ColumnComment != "" {
		// Escape single quotes in comment by doubling them
		escapedComment := strings.ReplaceAll(f.ColumnComment, "'", "''")
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", escapedComment))
	}

	return strings.Join(parts, " ")
}

// Equals compares two FieldInfo instances for semantic equality (MySQL-specific)
func (f *FieldInfo) Equals(other *FieldInfo) bool {
	if f == nil || other == nil {
		return f == other
	}

	// Compare basic properties
	if f.ColumnName != other.ColumnName ||
		f.IsNullAble != other.IsNullAble ||
		f.DataType != other.DataType ||
		f.ColumnComment != other.ColumnComment ||
		f.Extra != other.Extra {
		return false
	}

	// Compare ColumnType with normalization for integer display width
	// MySQL 8.0.19+ removed display width for integer types (int(11) -> int)
	normalizedSourceType := normalizeIntegerType(f.ColumnType)
	normalizedDestType := normalizeIntegerType(other.ColumnType)
	if normalizedSourceType != normalizedDestType {
		return false
	}

	// Compare default values
	if (f.ColumnDefault == nil && other.ColumnDefault != nil) ||
		(f.ColumnDefault != nil && other.ColumnDefault == nil) {
		return false
	}
	if f.ColumnDefault != nil && other.ColumnDefault != nil {
		if *f.ColumnDefault != *other.ColumnDefault {
			return false
		}
	}

	// Compare character set and collation (handle NULL values gracefully)
	if !f.charsetEquals(other) || !f.collationEquals(other) {
		return false
	}

	return true
}

// charsetEquals checks if character sets are semantically equal.
// Most string columns report a non-nil charset in INFORMATION_SCHEMA, but
// enum/set may only expose collation on some MySQL-compatible backends.
func (f *FieldInfo) charsetEquals(other *FieldInfo) bool {
	if f.CharsetName == nil && other.CharsetName == nil {
		return true
	}
	if (f.CharsetName == nil) != (other.CharsetName == nil) {
		if !f.hasImplicitCharsetFromCollation() {
			return false
		}
		inferredCharset, ok := inferCharsetFromCollation(f.CollationName, other.CollationName)
		if !ok {
			return false
		}
		if f.CharsetName != nil {
			return strings.EqualFold(*f.CharsetName, inferredCharset)
		}
		return strings.EqualFold(*other.CharsetName, inferredCharset)
	}
	return *f.CharsetName == *other.CharsetName
}

// collationEquals checks if collations are semantically equal.
// In MySQL INFORMATION_SCHEMA, string columns always have non-nil collation,
// non-string columns always have nil collation. nil vs non-nil means different types.
func (f *FieldInfo) collationEquals(other *FieldInfo) bool {
	if f.CollationName == nil && other.CollationName == nil {
		return true
	}
	if (f.CollationName == nil) != (other.CollationName == nil) {
		return false
	}
	return *f.CollationName == *other.CollationName
}

func (f *FieldInfo) hasImplicitCharsetFromCollation() bool {
	switch strings.ToLower(f.DataType) {
	case "enum", "set":
		return true
	default:
		return false
	}
}

func inferCharsetFromCollation(collations ...*string) (string, bool) {
	for _, collation := range collations {
		if collation == nil || *collation == "" {
			continue
		}
		name := strings.ToLower(*collation)
		index := strings.Index(name, "_")
		if index <= 0 {
			continue
		}
		return name[:index], true
	}
	return "", false
}

type dbType string

const (
	dbTypeSource dbType = "source"
	dbTypeDest   dbType = "dest"
)

// MyDb db struct
type MyDb struct {
	sqlDB   *sql.DB
	dbType  dbType
	dbName  string // 数据库名称
	dialect Dialect
}

// NewMyDb parse dsn and create database connection
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

// GetTableNames table names
func (db *MyDb) GetTableNames() []string {
	tables, err := db.dialect.GetTableNames(db.sqlDB)
	if err != nil {
		panic("get table names failed:" + err.Error())
	}
	return tables
}

// GetTableSchema table schema
func (db *MyDb) GetTableSchema(name string) (schema string) {
	s, err := db.dialect.GetTableSchema(db.sqlDB, db.dbName, name)
	if err != nil {
		log.Printf("get table %s schema failed: %s", name, errString(err))
		return ""
	}
	return s
}

// TableFieldsFromInformationSchema retrieves detailed field information
func (db *MyDb) TableFieldsFromInformationSchema(tableName string) (map[string]*FieldInfo, error) {
	return db.dialect.GetTableFields(db.sqlDB, db.dbName, tableName)
}

// TableIndexesExtra 通过 dialect 可选能力 IndexEnumerator 枚举"非 constraint 支撑"的索引。
// 若 dialect 未实现该能力或 sqlDB 不可用（例如纯文本对比测试），返回 (nil, nil)。
func (db *MyDb) TableIndexesExtra(tableName string) ([]*DbIndex, error) {
	if db == nil || db.sqlDB == nil || db.dialect == nil {
		return nil, nil
	}
	ie, ok := db.dialect.(IndexEnumerator)
	if !ok {
		return nil, nil
	}
	return ie.GetTableIndexes(db.sqlDB, tableName)
}

// TableTriggers 通过 dialect 可选能力 TriggerEnumerator 枚举表上的用户触发器。
// 若 dialect 未实现该能力或 sqlDB 不可用，返回 (nil, nil)。
func (db *MyDb) TableTriggers(tableName string) ([]*DbTrigger, error) {
	if db == nil || db.sqlDB == nil || db.dialect == nil {
		return nil, nil
	}
	te, ok := db.dialect.(TriggerEnumerator)
	if !ok {
		return nil, nil
	}
	return te.GetTableTriggers(db.sqlDB, tableName)
}

// TableComment 通过 dialect 可选能力 TableCommentEnumerator 读取表注释。
func (db *MyDb) TableComment(tableName string) (string, error) {
	if db == nil || db.sqlDB == nil || db.dialect == nil {
		return "", nil
	}
	tce, ok := db.dialect.(TableCommentEnumerator)
	if !ok {
		return "", nil
	}
	return tce.GetTableComment(db.sqlDB, tableName)
}

// Functions 通过 dialect 可选能力 FunctionEnumerator 枚举库内用户自定义函数。
func (db *MyDb) Functions() ([]*DbFunction, error) {
	if db == nil || db.sqlDB == nil || db.dialect == nil {
		return nil, nil
	}
	fe, ok := db.dialect.(FunctionEnumerator)
	if !ok {
		return nil, nil
	}
	return fe.GetFunctions(db.sqlDB)
}

// Query execute sql query
func (db *MyDb) Query(query string, args ...any) (rows *sql.Rows, err error) {
	txt := fmt.Sprintf("[%-6s: %s] [Query] Start SQL=%s Args=%s\n",
		db.dbType,
		db.dbName,
		xcolor.GreenString("%s", strings.TrimSpace(query)),
		xcolor.GreenString("%v", args),
	)
	log.Output(2, txt)
	start := time.Now()
	defer func() {
		cost := time.Since(start)
		txt = fmt.Sprintf("[%-6s: %s] [Query] End   Cost=%s Err=%s\n", db.dbType, db.dbName, cost.String(), errString(err))
		log.Output(3, txt)
	}()
	return db.sqlDB.Query(query, args...)
}
