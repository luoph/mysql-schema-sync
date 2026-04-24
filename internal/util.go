package internal

import (
	"bytes"
	"encoding/json"
	"html"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/xanygo/anygo/cli/xcolor"
)

// Version 版本号，格式：更新日期(8位).更新次数(累加)
const Version = "20260310.1"

// AppURL site
const AppURL = "https://github.com/luoph/mysql-schema-sync/"

const timeFormatStd string = "2006-01-02 15:04:05"

// loadJsonFile load json
func loadJSONFile(jsonPath string, val any) error {
	bs, err := os.ReadFile(jsonPath)
	if err != nil {
		return err
	}
	lines := strings.Split(string(bs), "\n")
	var bf bytes.Buffer
	for _, line := range lines {
		lineNew := strings.TrimSpace(line)
		if (len(lineNew) > 0 && lineNew[0] == '#') || (len(lineNew) > 1 && lineNew[0:2] == "//") {
			continue
		}
		bf.WriteString(lineNew)
	}
	return json.Unmarshal(bf.Bytes(), &val)
}

func inStringSlice(str string, strSli []string) bool {
	for _, v := range strSli {
		if str == v {
			return true
		}
	}
	return false
}

func simpleMatch(patternStr string, str string, msg ...string) bool {
	str = strings.TrimSpace(str)
	patternStr = strings.TrimSpace(patternStr)
	if patternStr == str {
		log.Println("simple_match:suc,equal", msg, "patternStr:", patternStr, "str:", str)
		return true
	}
	pattern := "^" + strings.ReplaceAll(patternStr, "*", `.*`) + "$"
	match, err := regexp.MatchString(pattern, str)
	if err != nil {
		log.Println("simple_match:error", msg, "patternStr:", patternStr, "pattern:", pattern, "str:", str, "err:", err)
	}
	// if match {
	// log.Println("simple_match:suc", msg, "patternStr:", patternStr, "pattern:", pattern, "str:", str)
	// }
	return match
}

func htmlPre(str string) string {
	return "<pre>" + html.EscapeString(str) + "</pre>"
}

func dsnShort(dsn string) string {
	i := strings.Index(dsn, "@")
	if i < 1 {
		return dsn
	}
	return dsn[i+1:]
}

func errString(err error) string {
	if err == nil {
		return xcolor.YellowString("<nil>")
	}
	return xcolor.RedString("%s", err.Error())
}

// normalizeColumnDDL normalizes a column DDL line by removing known false-positive
// differences. Used from the "semantically-equal" branch of column diffing to decide
// whether any text-level difference remains that warrants a CHANGE.
//
// Normalizations applied:
//  1. Integer display width: int(11) → int, bigint(20) → bigint 等（MySQL 8.0.19+ 兼容）。
//  2. MySQL SHOW CREATE TABLE 对 `CHARACTER SET` / `COLLATE` 子句的"按表默认值省略"显示噪声
//     —— 同一列在两侧若表默认字符集/排序不同，SHOW CREATE TABLE 是否输出这两个子句也会不
//     同，但 INFORMATION_SCHEMA.COLUMNS 报告的有效字符集/排序两侧相同。
//     调用方已通过 FieldsEqual 语义判等确认两侧字符集/排序等价，此处直接剥离子句以消除
//     MySQL 的显示噪声，避免反复生成"不会收敛"的 CHANGE 语句。剥离只作用于 COMMENT
//     子句之前的列属性段，以免误伤用户在列注释里提到的 "COLLATE" / "CHARACTER SET" 字面。
func normalizeColumnDDL(ddl string) string {
	ddl = integerWidthReg.ReplaceAllString(ddl, "$1")
	prefix, suffix := splitColumnCommentClause(ddl)
	prefix = charsetClauseReg.ReplaceAllString(prefix, "")
	prefix = collateClauseReg.ReplaceAllString(prefix, "")
	prefix = whitespaceRunReg.ReplaceAllString(prefix, " ")
	prefix = strings.TrimSpace(prefix)
	if suffix == "" {
		return prefix
	}
	return prefix + " " + suffix
}

var (
	integerWidthReg  = regexp.MustCompile(`(?i)(tinyint|smallint|mediumint|int|bigint)\(\d+\)`)
	charsetClauseReg = regexp.MustCompile(`(?i)\s+CHARACTER SET \w+`)
	collateClauseReg = regexp.MustCompile(`(?i)\s+COLLATE \w+`)
	whitespaceRunReg = regexp.MustCompile(`\s+`)
	commentClauseReg = regexp.MustCompile(`(?i)\s+COMMENT '`)
)

// splitColumnCommentClause 把列 DDL 行拆成 "属性段" 和 "COMMENT '...'" 两部分。
// 没有 COMMENT 子句时，suffix 为空。切点取首个 " COMMENT '" 出现处，以避免把
// 用户在注释里写的 "COMMENT '...'" 当成真正的列注释。
func splitColumnCommentClause(ddl string) (prefix, suffix string) {
	loc := commentClauseReg.FindStringIndex(ddl)
	if loc == nil {
		return ddl, ""
	}
	return ddl[:loc[0]], strings.TrimLeft(ddl[loc[0]:], " \t")
}

// normalizeIntegerType removes display width from integer types for MySQL 8.0.19+ compatibility.
// MySQL 8.0.19+ deprecated display width for integer types (TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT).
// This function normalizes types like "int(11)" to "int" while preserving modifiers like "unsigned" and "zerofill".
//
// Examples:
//   - "int(11)" -> "int"
//   - "int(11) unsigned" -> "int unsigned"
//   - "bigint(20)" -> "bigint"
//   - "tinyint(1)" -> "tinyint"
//   - "varchar(255)" -> "varchar(255)" (unchanged, not an integer type)
func normalizeIntegerType(columnType string) string {
	// Pattern matches: (tinyint|smallint|mediumint|int|bigint) followed by optional (digits)
	// Captures the type name and everything after the display width
	re := regexp.MustCompile(`(?i)^(tinyint|smallint|mediumint|int|bigint)\(\d+\)(\s+.+)?$`)

	matches := re.FindStringSubmatch(columnType)
	if len(matches) > 0 {
		// matches[1] is the type name (e.g., "int")
		// matches[2] is the modifiers (e.g., " unsigned", " zerofill"), may be empty
		if len(matches) > 2 && matches[2] != "" {
			return matches[1] + matches[2] // e.g., "int unsigned"
		}
		return matches[1] // e.g., "int"
	}

	// Not an integer type with display width, return as-is
	return columnType
}
