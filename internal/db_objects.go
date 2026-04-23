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
// 两种方言都需要实现，只是语义不同：
//   - PostgreSQL 的表注释是独立的 COMMENT ON TABLE 语句，CREATE TABLE 不包含注释；
//   - MySQL 的表注释由 CREATE TABLE 的 COMMENT='...' 子句内嵌，同时也支持
//     ALTER TABLE ... COMMENT='...' 修改。
//
// TableCommentInline 区分这两种语义：返回 true 时，整表新建路径依赖
// CreateTable DDL 自带的 COMMENT 子句，不再单独 emit；返回 false 时，
// 必须追加独立的 COMMENT ON TABLE 语句。ALTER 路径两者都调用 GenCommentTableSQL
// 输出表注释变更 DDL。
type TableCommentEnumerator interface {
	GetTableComment(db *sql.DB, tableName string) (string, error)
	GenCommentTableSQL(tableName, comment string) string
	TableCommentInline() bool
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

// DbExtension 表示一个已安装的 PostgreSQL 扩展。Name 足以作为同步键。
// 版本同步（ALTER EXTENSION ... UPDATE）跨版本语义复杂，目前不做，
// 仅保证"目标库装了所需 extension"这一 pre-condition。
type DbExtension struct {
	Name    string
	Version string
}

// ExtensionEnumerator 是 Dialect 的可选能力：枚举并生成 CREATE/DROP EXTENSION DDL。
// 执行顺序约束：pre-阶段（pre_extension_sync）必须早于所有可能依赖 extension
// 的对象（普通函数、表、索引，例如 vector extension 的 vector_cosine_ops 在
// HNSW 索引里被引用）；post-阶段（post_extension_sync）则必须晚于孤立对象的
// 清理，避免因 extension 被级联移除而误删用户对象。
type ExtensionEnumerator interface {
	GetExtensions(db *sql.DB) ([]*DbExtension, error)
	GenAddExtension(ext *DbExtension) string
	GenDropExtension(ext *DbExtension) string
}
