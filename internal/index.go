package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
)

// DbIndex db index
type DbIndex struct {
	IndexType indexType
	Name      string
	SQL       string

	// Comment 用于 PostgreSQL 的 COMMENT ON INDEX；仅在通过 IndexEnumerator
	// 枚举得到的普通索引中有意义（MySQL 的索引注释嵌在 SHOW CREATE TABLE 里，
	// 会随 SQL 文本一同比较，不需要独立字段）。
	Comment string

	// 相关联的表
	RelationTables []string
}

type indexType string

const (
	indexTypePrimary    indexType = "PRIMARY"
	indexTypeIndex      indexType = "INDEX"
	indexTypeUnique     indexType = "UNIQUE"
	indexTypeForeignKey indexType = "FOREIGN KEY"
	checkConstraint     indexType = "CHECK"
)

func (idx *DbIndex) String() string {
	bs, _ := json.MarshalIndent(idx, "  ", " ")
	return string(bs)
}

// mysqlAddBody 返回 ADD 子句体（不含 "ALTER TABLE `t` " 前缀）。
func (idx *DbIndex) mysqlAddBody() string {
	return "ADD " + idx.SQL
}

// mysqlDropBody 返回 DROP 子句体（不含 "ALTER TABLE `t` " 前缀）。
func (idx *DbIndex) mysqlDropBody() string {
	switch idx.IndexType {
	case indexTypePrimary:
		return "DROP PRIMARY KEY"
	case indexTypeIndex, indexTypeUnique:
		return fmt.Sprintf("DROP INDEX `%s`", mysqlQuoteIdent(idx.Name))
	case indexTypeForeignKey:
		return fmt.Sprintf("DROP FOREIGN KEY `%s`", mysqlQuoteIdent(idx.Name))
	case checkConstraint:
		return fmt.Sprintf("DROP CHECK `%s`", mysqlQuoteIdent(idx.Name))
	}
	return ""
}

// mysqlIndexProbeName 返回 information_schema.STATISTICS.INDEX_NAME 的探测值：
// 主键固定为 'PRIMARY'，其余用索引名。
func (idx *DbIndex) mysqlIndexProbeName() string {
	if idx.IndexType == indexTypePrimary {
		return "PRIMARY"
	}
	return idx.Name
}

// mysqlProbe 返回该索引类型对应的 information_schema 探测来源表（from）与名称条件
// 片段（nameCondition，不含 AND 前缀，已含名称列与 CONSTRAINT_TYPE 等限定）。
// 调用方负责将 TABLE_SCHEMA=DATABASE() AND TABLE_NAME='...' 与 nameCondition 拼接。
//
// 注意：不得以 indexTypeForeignKey 调用本函数；外键由 GenAddForeignKey /
// GenDropForeignKeyGuard 独立探测 information_schema.TABLE_CONSTRAINTS。
//
//   - checkConstraint → information_schema.TABLE_CONSTRAINTS，条件为
//     CONSTRAINT_NAME='<Name>' AND CONSTRAINT_TYPE='CHECK'
//   - 其他 → information_schema.STATISTICS，条件为
//     INDEX_NAME='<mysqlIndexProbeName()>'
func (idx *DbIndex) mysqlProbe(escapeFn func(string) string) (from, nameCondition string) {
	if idx.IndexType == checkConstraint {
		return "information_schema.TABLE_CONSTRAINTS",
			fmt.Sprintf("CONSTRAINT_NAME='%s' AND CONSTRAINT_TYPE='CHECK'", escapeFn(idx.Name))
	}
	return "information_schema.STATISTICS",
		fmt.Sprintf("INDEX_NAME='%s'", escapeFn(idx.mysqlIndexProbeName()))
}

func (idx *DbIndex) addRelationTable(table string) {
	table = strings.TrimSpace(table)
	if len(table) != 0 {
		idx.RelationTables = append(idx.RelationTables, table)
	}
}

// 匹配索引字段
var indexReg = regexp.MustCompile(`^([A-Z]+\s)?KEY\s`)

// 匹配外键
var foreignKeyReg = regexp.MustCompile("^CONSTRAINT `(.+)` FOREIGN KEY.+ REFERENCES `(.+)` ")

// Check约束
var checkConstraintReg = regexp.MustCompile("^CONSTRAINT `([^`]+)` CHECK \\(\\((.+)\\)\\)")

func parseDbIndexLine(line string) *DbIndex {
	line = strings.TrimSpace(line)
	idx := &DbIndex{
		SQL:            line,
		RelationTables: []string{},
	}
	if strings.HasPrefix(line, "PRIMARY") {
		idx.IndexType = indexTypePrimary
		idx.Name = "PRIMARY KEY"
		return idx
	}

	// UNIQUE KEY `idx_a` (`a`) USING HASH COMMENT '注释',
	// FULLTEXT KEY `c` (`c`)
	// PRIMARY KEY (`d`)
	// KEY `idx_e` (`e`),
	if indexReg.MatchString(line) {
		arr := strings.Split(line, "`")
		if strings.HasPrefix(strings.ToUpper(line), "UNIQUE") {
			idx.IndexType = indexTypeUnique
		} else {
			idx.IndexType = indexTypeIndex
		}
		idx.Name = arr[1]
		return idx
	}

	// CONSTRAINT `busi_table_ibfk_1` FOREIGN KEY (`repo_id`) REFERENCES `repo_table` (`repo_id`)
	foreignMatches := foreignKeyReg.FindStringSubmatch(line)
	if len(foreignMatches) > 0 {
		idx.IndexType = indexTypeForeignKey
		idx.Name = foreignMatches[1]
		idx.addRelationTable(foreignMatches[2])
		return idx
	}

	// CONSTRAINT `chk_xx_1` CHECK ((`x` >= 0 and `y` <= 100))
	checkMatches := checkConstraintReg.FindStringSubmatch(line)
	if len(checkMatches) > 0 {
		idx.IndexType = checkConstraint
		idx.Name = checkMatches[1]
		return idx
	}

	log.Fatalln("db_index parse failed, unsupported, line:", line)
	return nil
}
