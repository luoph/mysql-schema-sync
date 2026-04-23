package internal

import "database/sql"

// IndexEnumerator 是 Dialect 的可选能力：通过直接查询数据库元信息枚举"非 constraint
// 支撑"的索引（普通 btree、GIN、HNSW、partial、表达式索引等）。
//
// 背景：PostgreSQL 的 SHOW CREATE TABLE 等价输出并不会把"不对应 UNIQUE / PRIMARY KEY /
// FOREIGN KEY / CHECK 约束"的索引写进来，因此仅靠 ParseSchema 解析 CREATE TABLE 字符串
// 会漏掉普通索引。实现该接口的 Dialect 在 sync 流程中会被额外调用以补全索引集合。
// MySQL 的 SHOW CREATE TABLE 已包含所有索引，所以不需要实现此接口。
type IndexEnumerator interface {
	GetTableIndexes(db *sql.DB, tableName string) ([]*DbIndex, error)
}

// IndexCommenter 是 Dialect 的可选能力：为索引生成 COMMENT ON INDEX DDL。
// 仅当一侧索引 Comment 与另一侧不同、而 SQL 相同时，会单独 emit 该语句以避免
// 不必要的索引重建。MySQL 的索引注释嵌在 CREATE TABLE 里，不需要实现。
type IndexCommenter interface {
	GenCommentIndexSQL(indexName, comment string) string
}

// TableCommentEnumerator 是 Dialect 的可选能力：读取与生成表级注释 DDL。
// MySQL 的表注释原本就嵌在 CREATE TABLE 的 COMMENT= 子句里，不需要独立处理，
// 因此只有 PostgreSQL 需要实现这个能力。
type TableCommentEnumerator interface {
	GetTableComment(db *sql.DB, tableName string) (string, error)
	GenCommentTableSQL(tableName, comment string) string
}

// DbTrigger 表示一个用户触发器。Definition 保存完整可执行的
// CREATE TRIGGER DDL（如 pg_get_triggerdef 的输出），方便目标库直接重放。
type DbTrigger struct {
	Name       string
	Table      string
	Definition string
}

// TriggerEnumerator 是 Dialect 的可选能力：枚举指定表上的用户触发器。
type TriggerEnumerator interface {
	GetTableTriggers(db *sql.DB, tableName string) ([]*DbTrigger, error)
	// GenDropTrigger 生成 DROP TRIGGER 语句。
	GenDropTrigger(trg *DbTrigger) string
	// GenAddTrigger 返回可用于目标库执行的 DDL 语句（通常就是 trg.Definition 加分号）。
	GenAddTrigger(trg *DbTrigger) string
}

// DbFunction 表示一个用户自定义函数/存储过程。
// Signature 是 (arg_type_list) 形式的参数签名，用于唯一识别重载函数；
// Definition 保存完整可执行的 CREATE OR REPLACE FUNCTION DDL。
type DbFunction struct {
	Name       string
	Signature  string
	Definition string
}

// FunctionEnumerator 是 Dialect 的可选能力：枚举数据库内用户自定义函数（排除
// extension 自带或语言实现为 "internal" / "c" 的系统函数）。
type FunctionEnumerator interface {
	GetFunctions(db *sql.DB) ([]*DbFunction, error)
	GenDropFunction(fn *DbFunction) string
	GenAddFunction(fn *DbFunction) string
}
