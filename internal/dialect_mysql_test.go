package internal

import (
	"strings"
	"testing"

	"github.com/xanygo/anygo/xt"
)

func TestMySQLDialect_CleanTableSchema(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("strip auto_increment keeps engine and charset", func(t *testing.T) {
		in := "CREATE TABLE `t` (\n  `id` bigint NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=123 DEFAULT CHARSET=utf8mb4"
		out := d.CleanTableSchema(in)
		xt.Equal(t, false, strings.Contains(out, "AUTO_INCREMENT=123"))
		// ENGINE / DEFAULT CHARSET 必须保留：未来接入引擎/字符集同步时依赖此信息
		xt.Equal(t, true, strings.Contains(out, "ENGINE=InnoDB"))
		xt.Equal(t, true, strings.Contains(out, "DEFAULT CHARSET=utf8mb4"))
	})

	t.Run("preserves COMMENT clause", func(t *testing.T) {
		in := "CREATE TABLE `t` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='用户表'"
		out := d.CleanTableSchema(in)
		xt.Equal(t, true, strings.Contains(out, "COMMENT='用户表'"))
	})
}

func TestMySQLDialect_ParseSchema_UniqueIndex(t *testing.T) {
	d := &MySQLDialect{}

	schema := "CREATE TABLE `t` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `email` varchar(64) NOT NULL,\n" +
		"  `code` varchar(32) NOT NULL,\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  UNIQUE KEY `uk_email` (`email`),\n" +
		"  KEY `idx_code` (`code`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	mys := d.ParseSchema(schema)

	uk, hasUK := mys.IndexAll["uk_email"]
	xt.Equal(t, true, hasUK)
	xt.Equal(t, indexTypeUnique, uk.IndexType)

	idx, hasIdx := mys.IndexAll["idx_code"]
	xt.Equal(t, true, hasIdx)
	xt.Equal(t, indexTypeIndex, idx.IndexType)

	pk, hasPK := mys.IndexAll["PRIMARY KEY"]
	xt.Equal(t, true, hasPK)
	xt.Equal(t, indexTypePrimary, pk.IndexType)
}

func TestMySQLDialect_ParseSchema_CheckConstraint(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("double-paren form (MySQL 8 normalized)", func(t *testing.T) {
		schema := "CREATE TABLE `t` (\n" +
			"  `price` decimal(10,2) NOT NULL,\n" +
			"  CONSTRAINT `chk_price` CHECK ((`price` > 0))\n" +
			")"
		mys := d.ParseSchema(schema)
		chk, has := mys.IndexAll["chk_price"]
		xt.Equal(t, true, has)
		xt.Equal(t, checkConstraint, chk.IndexType)
	})

	t.Run("single-paren form", func(t *testing.T) {
		schema := "CREATE TABLE `t` (\n" +
			"  `age` int NOT NULL,\n" +
			"  CONSTRAINT `chk_age` CHECK (`age` > 0)\n" +
			")"
		mys := d.ParseSchema(schema)
		chk, has := mys.IndexAll["chk_age"]
		xt.Equal(t, true, has)
		xt.Equal(t, checkConstraint, chk.IndexType)
	})
}

func TestMySQLDialect_GenCommentTableSQL(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("set", func(t *testing.T) {
		xt.Equal(t, "ALTER TABLE `t` COMMENT = '用户表';", d.GenCommentTableSQL("t", "用户表"))
	})
	t.Run("clear", func(t *testing.T) {
		xt.Equal(t, "ALTER TABLE `t` COMMENT = '';", d.GenCommentTableSQL("t", ""))
	})
	t.Run("escape single quote", func(t *testing.T) {
		xt.Equal(t, "ALTER TABLE `t` COMMENT = 'it''s';", d.GenCommentTableSQL("t", "it's"))
	})
	t.Run("inline flag is true", func(t *testing.T) {
		xt.Equal(t, true, d.TableCommentInline())
	})
}

func TestMySQLEscapeSQLLiteral(t *testing.T) {
	xt.Equal(t, "a''b", mysqlEscapeSQLLiteral("a'b"))
	xt.Equal(t, `a\\b`, mysqlEscapeSQLLiteral(`a\b`))
	xt.Equal(t, `\\''`, mysqlEscapeSQLLiteral(`\'`))
}

func TestMySQLGuard(t *testing.T) {
	t.Run("add semantics uses COUNT=0", func(t *testing.T) {
		got := mysqlGuard("information_schema.COLUMNS",
			"TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND COLUMN_NAME='c'",
			"ALTER TABLE `t` ADD `c` int", false)
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `t` ADD `c` int', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND COLUMN_NAME='c');",
			got[0])
		xt.Equal(t, "PREPARE __mss_stmt FROM @__mss_sql;", got[1])
		xt.Equal(t, "EXECUTE __mss_stmt;", got[2])
		xt.Equal(t, "DEALLOCATE PREPARE __mss_stmt;", got[3])
	})
	t.Run("drop semantics uses COUNT>0 and escapes ddl", func(t *testing.T) {
		got := mysqlGuard("information_schema.STATISTICS",
			"TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND INDEX_NAME='idx'",
			"ALTER TABLE `t` DROP INDEX `idx`", true)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `t` DROP INDEX `idx`', 'SELECT 1') FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='t' AND INDEX_NAME='idx');",
			got[0])
	})
}

func TestMySQLDialect_GuardColumns(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("add column guarded", func(t *testing.T) {
		got := d.GenAddColumn("user", "`age` int NOT NULL", "email", false, 1)
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `age` int NOT NULL AFTER `email`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='age');",
			got[0])
	})
	t.Run("add first column guarded", func(t *testing.T) {
		got := d.GenAddColumn("user", "`age` int NOT NULL", "", true, 0)
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `age` int NOT NULL FIRST', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='age');",
			got[0])
	})
	t.Run("drop column guarded", func(t *testing.T) {
		got := d.GenDropColumn("user", "age")
		xt.Equal(t, 4, len(got))
		xt.Equal(t,
			"SET @__mss_sql = (SELECT IF(COUNT(*)>0, 'ALTER TABLE `user` DROP COLUMN `age`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='age');",
			got[0])
	})
}

func TestMySQLDialect_Idempotent_Table(t *testing.T) {
	d := &MySQLDialect{}

	t.Run("create table if not exists keeps auto_increment strip", func(t *testing.T) {
		in := "CREATE TABLE `t` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4"
		out := d.GenCreateTable(in)
		xt.Equal(t, true, strings.HasPrefix(out, "CREATE TABLE IF NOT EXISTS `t`"))
		xt.Equal(t, false, strings.Contains(out, "AUTO_INCREMENT=9"))
		xt.Equal(t, true, strings.HasSuffix(out, ";"))
	})
	t.Run("drop table if exists", func(t *testing.T) {
		xt.Equal(t, "DROP TABLE IF EXISTS `user`;", d.GenDropTable("user"))
	})
}
