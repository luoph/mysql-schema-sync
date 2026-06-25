package internal

import (
	"fmt"
	"regexp"
	"strings"
)

type alterType int

const (
	alterTypeNo alterType = iota
	alterTypeCreate
	alterTypeDropTable
	alterTypeAlter
)

func (at alterType) String() string {
	switch at {
	case alterTypeNo:
		return "not_change"
	case alterTypeCreate:
		return "create"
	case alterTypeDropTable:
		return "drop"
	case alterTypeAlter:
		return "alter"
	default:
		return "unknown"
	}
}

// TableAlterData 表的变更情况
type TableAlterData struct {
	SchemaDiff *SchemaDiff
	Table      string
	Comment    string
	SQL        []string
	Type       alterType
}

func (ta *TableAlterData) Split() []*TableAlterData {
	rs := make([]*TableAlterData, len(ta.SQL))
	for i := 0; i < len(ta.SQL); i++ {
		rs[i] = &TableAlterData{
			SchemaDiff: ta.SchemaDiff,
			Table:      ta.Table,
			Comment:    ta.Comment,
			Type:       ta.Type,
			SQL:        []string{ta.SQL[i]},
		}
	}
	return rs
}

func (ta *TableAlterData) String() string {
	var lines []string
	lines = append(lines, fmt.Sprintf("-- Table : %s", ta.Table))
	lines = append(lines, fmt.Sprintf("-- Type : %s", ta.Type))
	if comment := strings.TrimSpace(ta.Comment); comment != "" {
		lines = append(lines, fmt.Sprintf("-- Comment: %s", comment))
	}
	if len(ta.SQL) > 0 {
		// 单表粒度事务：PG 支持事务性 DDL，BEGIN/COMMIT 可做到"要么全成、要么全回滚"；
		// MySQL 的每条 DDL 隐式提交，BEGIN/COMMIT 在 MySQL 下仅为脚本语义标记，
		// 中途失败时已提交的 DDL 无法回滚，恢复依赖于幂等守卫的可重跑性。
		lines = append(lines, "BEGIN;")
		lines = append(lines, strings.Join(ta.SQL, "\n"))
		lines = append(lines, "COMMIT;")
	}
	return strings.Join(lines, "\n")
}

var autoIncrReg = regexp.MustCompile(`\sAUTO_INCREMENT=[1-9]\d*\s`)

func fmtTableCreateSQL(sql string) string {
	return autoIncrReg.ReplaceAllString(sql, " ")
}
