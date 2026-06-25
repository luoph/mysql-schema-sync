package internal

import (
	"strings"
	"testing"

	"github.com/xanygo/anygo/xt"
)

// 断言一段生成的 SQL 文本不含裸 DDL（即均已幂等化）。
func assertNoBareDDL(t *testing.T, sql string) {
	t.Helper()
	for _, line := range strings.Split(sql, "\n") {
		l := strings.ToUpper(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ";")))
		switch {
		case strings.HasPrefix(l, "CREATE TABLE ") && !strings.HasPrefix(l, "CREATE TABLE IF NOT EXISTS"):
			t.Errorf("bare CREATE TABLE: %q", line)
		case (strings.HasPrefix(l, "CREATE INDEX ") || strings.HasPrefix(l, "CREATE UNIQUE INDEX ")) && !strings.Contains(l, "IF NOT EXISTS"):
			t.Errorf("bare CREATE INDEX: %q", line)
		}
	}
}

func TestPG_NoBareDDL_AddColumns(t *testing.T) {
	sc := &SchemaSync{Config: &Config{}, SourceDb: &MyDb{dialect: &PostgresDialect{}}}
	got := sc.getAlterDataBySchema("user",
		testLoadFile("testdata/pg/user_0.sql"),
		testLoadFile("testdata/pg/user_1.sql"),
		&Config{})
	out := got.String()
	assertNoBareDDL(t, out)
	xt.Equal(t, true, strings.Contains(out, "ADD COLUMN IF NOT EXISTS"))
}

func TestMySQL_NoBareDDL_AddColumns(t *testing.T) {
	sc := &SchemaSync{Config: &Config{}, SourceDb: &MyDb{dialect: &MySQLDialect{}}}
	got := sc.getAlterDataBySchema("user",
		testLoadFile("testdata/user/user_0.sql"),
		testLoadFile("testdata/user/user_1.sql"),
		&Config{})
	out := got.String()
	assertNoBareDDL(t, out)
	// 守卫块标志
	xt.Equal(t, true, strings.Contains(out, "PREPARE __mss_stmt FROM @__mss_sql"))
	xt.Equal(t, false, strings.Contains(out, "\nADD ")) // 不应再有裸 ADD 子句独占一行
	// 守卫前的裸形式 ALTER TABLE `user` ADD `col` 不应再独占一行出现
	xt.Equal(t, false, strings.Contains(out, "\nALTER TABLE "+"`"+"user"+"`"+" ADD "+"`"))
}
