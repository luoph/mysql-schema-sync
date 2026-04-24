package internal

import (
	"testing"

	"github.com/xanygo/anygo/ds/xmap"
	"github.com/xanygo/anygo/xt"
)

// TestGetSchemaDiff_MySQLCharsetDisplayNoise 复现：同一列在源/目标的 SHOW CREATE TABLE
// 输出中是否显式带 COLLATE/CHARACTER SET 取决于该表的默认字符集/排序规则，但
// INFORMATION_SCHEMA.COLUMNS 报告的有效字符集/排序规则两侧相同。此时
// getSchemaDiff 不应生成任何 ALTER，因为列在语义上完全等价。
func TestGetSchemaDiff_MySQLCharsetDisplayNoise(t *testing.T) {
	mkSchema := func(line string, fi *FieldInfo) *MySchema {
		fields := xmap.Ordered[string, string]{}
		fields.Set(fi.ColumnName, line)
		return &MySchema{
			Fields:     fields,
			FieldInfos: map[string]*FieldInfo{fi.ColumnName: fi},
			IndexAll:   map[string]*DbIndex{},
			ForeignAll: map[string]*DbIndex{},
		}
	}
	mkFI := func() *FieldInfo {
		return &FieldInfo{
			ColumnName:             "scene",
			OrdinalPosition:        1,
			IsNullAble:             "YES",
			DataType:               "varchar",
			CharacterMaximumLength: func() *int { v := 32; return &v }(),
			CharsetName:            stringPtr("utf8mb4"),
			CollationName:          stringPtr("utf8mb4_general_ci"),
			ColumnType:             "varchar(32)",
			ColumnComment:          "场景标识: navigation=模板库",
		}
	}

	// 源：SHOW CREATE TABLE 输出带显式 COLLATE（因为源表默认 collation 与列不同）
	// 目标：SHOW CREATE TABLE 输出不带 COLLATE（因为目标表默认 collation 与列一致）
	// 两侧 INFORMATION_SCHEMA.COLUMNS 给出相同的 (utf8mb4, utf8mb4_general_ci)。
	src := mkSchema(
		"`scene` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '场景标识: navigation=模板库'",
		mkFI(),
	)
	dst := mkSchema(
		"`scene` varchar(32) DEFAULT NULL COMMENT '场景标识: navigation=模板库'",
		mkFI(),
	)

	sc := &SchemaSync{Config: &Config{}}
	alter := &TableAlterData{
		Table:      "action_card_tab",
		SchemaDiff: &SchemaDiff{Source: src, Dest: dst, Table: "action_card_tab"},
	}
	alterClauses, standalone := sc.getSchemaDiff(alter)
	xt.Equal(t, 0, len(alterClauses))
	xt.Equal(t, 0, len(standalone))
}

// TestGetSchemaDiff_MySQLCharsetDisplayNoise_ExplicitCharsetVsImplicit 变体：
// 源/目标在 DDL 文本层面差异是 "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
// 与 "COLLATE utf8mb4_general_ci"；两侧 INFORMATION_SCHEMA 报告一致。
func TestGetSchemaDiff_MySQLCharsetDisplayNoise_ExplicitCharsetVsImplicit(t *testing.T) {
	fi := &FieldInfo{
		ColumnName:    "shared_id",
		IsNullAble:    "NO",
		DataType:      "varchar",
		CharsetName:   stringPtr("utf8mb4"),
		CollationName: stringPtr("utf8mb4_general_ci"),
		ColumnType:    "varchar(64)",
		ColumnDefault: stringPtr(""),
		ColumnComment: "分享ID",
	}

	mkSchema := func(line string) *MySchema {
		fields := xmap.Ordered[string, string]{}
		fields.Set(fi.ColumnName, line)
		return &MySchema{
			Fields:     fields,
			FieldInfos: map[string]*FieldInfo{fi.ColumnName: fi},
			IndexAll:   map[string]*DbIndex{},
			ForeignAll: map[string]*DbIndex{},
		}
	}

	src := mkSchema(
		"`shared_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '分享ID'",
	)
	dst := mkSchema(
		"`shared_id` varchar(64) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '分享ID'",
	)

	sc := &SchemaSync{Config: &Config{}}
	alter := &TableAlterData{
		Table:      "user_contact",
		SchemaDiff: &SchemaDiff{Source: src, Dest: dst, Table: "user_contact"},
	}
	alterClauses, standalone := sc.getSchemaDiff(alter)
	xt.Equal(t, 0, len(alterClauses))
	xt.Equal(t, 0, len(standalone))
}
