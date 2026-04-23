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
