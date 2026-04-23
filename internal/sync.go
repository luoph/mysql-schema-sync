package internal

import (
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/xanygo/anygo/cli/xcolor"
)

// SchemaSync 配置文件
type SchemaSync struct {
	Config   *Config
	SourceDb *MyDb
	DestDb   *MyDb
}

// NewSchemaSync 对一个配置进行同步
func NewSchemaSync(config *Config) *SchemaSync {
	s := new(SchemaSync)
	s.Config = config
	s.SourceDb = NewMyDb(config.SourceDSN, dbTypeSource)
	s.DestDb = NewMyDb(config.DestDSN, dbTypeDest)
	return s
}

// GetNewTableNames 获取所有新增加的表名
func (sc *SchemaSync) GetNewTableNames() []string {
	sourceTables := sc.SourceDb.GetTableNames()
	destTables := sc.DestDb.GetTableNames()

	var newTables []string
	for _, name := range sourceTables {
		if !inStringSlice(name, destTables) {
			newTables = append(newTables, name)
		}
	}
	return newTables
}

// AllDBTables 合并源数据库和目标数据库的表名
func (sc *SchemaSync) AllDBTables() []string {
	sourceTables := sc.SourceDb.GetTableNames()
	destTables := sc.DestDb.GetTableNames()
	tables := slices.Clone(destTables)
	for _, name := range sourceTables {
		if !inStringSlice(name, tables) {
			tables = append(tables, name)
		}
	}
	return tables
}

func (sc *SchemaSync) getDialect() Dialect {
	if sc.SourceDb != nil && sc.SourceDb.dialect != nil {
		return sc.SourceDb.dialect
	}
	return &MySQLDialect{}
}

func (sc *SchemaSync) getAlterDataByTable(table string, cfg *Config) *TableAlterData {
	sSchema := sc.SourceDb.GetTableSchema(table)
	dSchema := sc.DestDb.GetTableSchema(table)
	return sc.getAlterDataBySchema(table, sSchema, dSchema, cfg)
}

func (sc *SchemaSync) getAlterDataBySchema(table string, sSchema string, dSchema string, cfg *Config) *TableAlterData {
	d := sc.getDialect()
	alter := new(TableAlterData)
	alter.Table = table
	alter.Type = alterTypeNo

	var sourceFields, destFields map[string]*FieldInfo
	var sourceFieldsErr, destFieldsErr error

	if sc.SourceDb != nil && sc.DestDb != nil {
		sourceFields, sourceFieldsErr = sc.SourceDb.TableFieldsFromInformationSchema(table)
		destFields, destFieldsErr = sc.DestDb.TableFieldsFromInformationSchema(table)
	}

	cleanSSchema := d.CleanTableSchema(sSchema)
	cleanDSchema := d.CleanTableSchema(dSchema)

	if sourceFieldsErr == nil && destFieldsErr == nil && sourceFields != nil && destFields != nil {
		log.Printf("[Debug] Using structured field comparison for table %q", table)
		alter.SchemaDiff = &SchemaDiff{
			Table:  table,
			Source: NewSchemaWithFieldInfos(d.ParseSchema(cleanSSchema), sourceFields),
			Dest:   NewSchemaWithFieldInfos(d.ParseSchema(cleanDSchema), destFields),
		}
	} else {
		if sourceFieldsErr != nil {
			log.Printf("[Debug] Failed to get source fields for table %q: %s", table, errString(sourceFieldsErr))
		}
		if destFieldsErr != nil {
			log.Printf("[Debug] Failed to get dest fields for table %q: %s", table, errString(destFieldsErr))
		}
		log.Printf("[Debug] Using legacy text-based comparison for table %q", table)
		alter.SchemaDiff = &SchemaDiff{
			Table:  table,
			Source: d.ParseSchema(cleanSSchema),
			Dest:   d.ParseSchema(cleanDSchema),
		}
	}

	// 合并"非 constraint 支撑"索引（PG 普通/GIN/HNSW/partial/表达式索引）。
	// 这些索引不会出现在 ParseSchema 从 CREATE TABLE 字符串解析的 IndexAll 里，
	// 需要直接查数据库元信息补齐。MySQL 等 dialect 未实现 IndexEnumerator 时此调用为 no-op。
	sc.mergeExtraIndexes(alter.SchemaDiff.Source, sc.SourceDb, table)
	sc.mergeExtraIndexes(alter.SchemaDiff.Dest, sc.DestDb, table)

	// 注入触发器集合。MySQL dialect 未实现 TriggerEnumerator，无副作用。
	sc.loadTriggers(alter.SchemaDiff.Source, sc.SourceDb, table)
	sc.loadTriggers(alter.SchemaDiff.Dest, sc.DestDb, table)

	// 注入表注释。MySQL 的表注释嵌在 CREATE TABLE 内，不走这条路径，dialect
	// 未实现 TableCommentEnumerator 时此调用为 no-op。
	sc.loadTableComment(alter.SchemaDiff.Source, sc.SourceDb, table)
	sc.loadTableComment(alter.SchemaDiff.Dest, sc.DestDb, table)

	// 使用 cleaned 串做全文早出对比：MySQL 的 AUTO_INCREMENT 会随数据行变化而
	// 经常不同，原始串对比会误触发后续 diff 流程；cleaned 后只保留结构相关信息。
	// len 判断仍用原始串，避免把"仅剥除空白产物的空串"误判为源/目标不存在。
	if cleanSSchema == cleanDSchema {
		return alter
	}
	if len(sSchema) == 0 {
		alter.Type = alterTypeDropTable
		alter.Comment = "源数据库不存在，删除目标数据库多余的表"
		alter.SQL = append(alter.SQL, d.GenDropTable(table))
		return alter
	}
	if len(dSchema) == 0 {
		alter.Type = alterTypeCreate
		alter.Comment = "目标数据库不存在，创建"
		alter.SQL = append(alter.SQL, d.GenCreateTable(sSchema))
		idxCommenter, _ := d.(IndexCommenter)
		for _, idx := range alter.SchemaDiff.Source.IndexAll {
			if idx.IndexType != indexTypeIndex {
				continue
			}
			upperDef := strings.ToUpper(strings.TrimSpace(idx.SQL))
			if strings.HasPrefix(upperDef, "CREATE INDEX") || strings.HasPrefix(upperDef, "CREATE UNIQUE INDEX") {
				alter.SQL = append(alter.SQL, strings.TrimRight(idx.SQL, ";")+";")
				if idxCommenter != nil && idx.Comment != "" {
					alter.SQL = append(alter.SQL, idxCommenter.GenCommentIndexSQL(idx.Name, idx.Comment))
				}
			}
		}
		if te, ok := d.(TriggerEnumerator); ok {
			for _, trg := range alter.SchemaDiff.Source.Triggers {
				alter.SQL = append(alter.SQL, te.GenAddTrigger(trg))
			}
		}
		// 仅非 inline dialect（PG）需要独立 emit COMMENT ON TABLE；
		// MySQL 的注释已经通过 GenCreateTable 保留在 CREATE TABLE 子句里。
		if tce, ok := d.(TableCommentEnumerator); ok && !tce.TableCommentInline() && alter.SchemaDiff.Source.TableComment != "" {
			alter.SQL = append(alter.SQL, tce.GenCommentTableSQL(table, alter.SchemaDiff.Source.TableComment))
		}
		return alter
	}

	alterClauses, standaloneSQL := sc.getSchemaDiff(alter)
	triggerSQLs := sc.diffTriggers(alter)
	tableCommentSQL := sc.diffTableComment(alter)
	if len(alterClauses) == 0 && len(standaloneSQL) == 0 && len(triggerSQLs) == 0 && tableCommentSQL == "" {
		return alter
	}
	alter.Type = alterTypeAlter
	if len(alterClauses) > 0 {
		alter.SQL = append(alter.SQL, d.WrapAlterSQL(table, alterClauses, cfg.SingleSchemaChange)...)
	}
	alter.SQL = append(alter.SQL, standaloneSQL...)
	alter.SQL = append(alter.SQL, triggerSQLs...)
	if tableCommentSQL != "" {
		alter.SQL = append(alter.SQL, tableCommentSQL)
	}

	return alter
}

// getSchemaDiff returns ALTER TABLE clauses and standalone SQL statements
func (sc *SchemaSync) getSchemaDiff(alter *TableAlterData) (alterClauses []string, standaloneSQL []string) {
	d := sc.getDialect()
	sourceMyS := alter.SchemaDiff.Source
	destMyS := alter.SchemaDiff.Dest
	table := alter.Table
	var beforeFieldName string
	var fieldCount int

	useStructuredComparison := len(sourceMyS.FieldInfos) > 0 && len(destMyS.FieldInfos) > 0

	if useStructuredComparison {
		log.Printf("[Debug] Using two-phase field comparison for table %s", table)
		for fieldName, value := range sourceMyS.Fields.Iter() {
			if sc.Config.IsIgnoreField(table, fieldName) {
				log.Printf("ignore column %s.%s", table, fieldName)
				continue
			}
			var newClauses []string

			if destValue, has := destMyS.Fields.Get(fieldName); has {
				sourceFieldInfo := sourceMyS.FieldInfos[fieldName]
				destFieldInfo := destMyS.FieldInfos[fieldName]

				if value == destValue {
					if d.SupportsColumnOrder() && sc.Config.FieldOrder && sourceFieldInfo != nil && destFieldInfo != nil {
						if sourceFieldInfo.OrdinalPosition != destFieldInfo.OrdinalPosition {
							alterSQL := fmt.Sprintf("MODIFY COLUMN %s", d.FieldDef(sourceFieldInfo))
							if len(beforeFieldName) > 0 {
								alterSQL += fmt.Sprintf(" AFTER %s", d.Quote(beforeFieldName))
							} else {
								alterSQL += " FIRST"
							}
							newClauses = append(newClauses, alterSQL)
							log.Printf("[Debug] field %s.%s: order differs (source pos=%d, dest pos=%d), generating MODIFY",
								table, fieldName, sourceFieldInfo.OrdinalPosition, destFieldInfo.OrdinalPosition)
						} else {
							log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "not change (text identical)")
						}
					} else {
						log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "not change (text identical)")
					}
					if len(newClauses) == 0 {
						beforeFieldName = fieldName
						fieldCount++
						continue
					}
				} else {
					if sourceFieldInfo != nil && destFieldInfo != nil {
						if d.FieldsEqual(sourceFieldInfo, destFieldInfo) {
							// FieldsEqual 认为语义相等，进一步检查归一化后的 DDL 文本是否仍有差异
							// 例如: 显式 CHARACTER SET vs 隐式继承表默认字符集
							normalizedValue := normalizeColumnDDL(value)
							normalizedDestValue := normalizeColumnDDL(destValue)
							if normalizedValue != normalizedDestValue {
								// 归一化后文本仍然不同，存在真实的 DDL 差异（如字符集表示不同）
								alterSQL := d.GenChangeColumnText(fieldName, value)
								if alterSQL != "" {
									newClauses = append(newClauses, alterSQL)
								}
								log.Printf("[Debug] field %s.%s: semantically equal but DDL text differs after normalization, generating CHANGE", table, fieldName)
							} else if d.SupportsColumnOrder() && sc.Config.FieldOrder && sourceFieldInfo.OrdinalPosition != destFieldInfo.OrdinalPosition {
								alterSQL := fmt.Sprintf("MODIFY COLUMN %s", d.FieldDef(sourceFieldInfo))
								if len(beforeFieldName) > 0 {
									alterSQL += fmt.Sprintf(" AFTER %s", d.Quote(beforeFieldName))
								} else {
									alterSQL += " FIRST"
								}
								newClauses = append(newClauses, alterSQL)
								log.Printf("[Debug] field %s.%s: semantically equal but order differs, generating MODIFY", table, fieldName)
							} else {
								log.Printf("[Debug] field %s.%s: text differs but semantically equal (integer display width only), skipping", table, fieldName)
								beforeFieldName = fieldName
								fieldCount++
								continue
							}
						} else {
							// 优先使用源 DDL 文本生成 CHANGE，保留 SHOW CREATE TABLE 的原始表示
							// 如果 GenChangeColumnText 返回空（如 PostgreSQL），退回使用结构化生成
							if alterSQL := d.GenChangeColumnText(fieldName, value); alterSQL != "" {
								newClauses = append(newClauses, alterSQL)
							} else {
								newClauses = append(newClauses, d.GenChangeColumn(fieldName, sourceFieldInfo, destFieldInfo)...)
							}
							log.Printf("[Debug] field %s.%s: confirmed difference via structured comparison", table, fieldName)
						}
					} else {
						alterSQL := d.GenChangeColumnText(fieldName, value)
						if alterSQL != "" {
							newClauses = append(newClauses, alterSQL)
						}
						log.Printf("[Debug] field %s.%s: text differs, using text-based change", table, fieldName)
					}
				}
				beforeFieldName = fieldName

				if sourceFieldInfo != nil && destFieldInfo != nil && sourceFieldInfo.ColumnComment != destFieldInfo.ColumnComment {
					if commentSQL := d.GenCommentColumnSQL(table, fieldName, sourceFieldInfo.ColumnComment); commentSQL != "" {
						standaloneSQL = append(standaloneSQL, commentSQL)
					}
				}
			} else {
				colDef := value
				if sfi := sourceMyS.FieldInfos[fieldName]; sfi != nil {
					colDef = d.FieldDef(sfi)
				}
				newClauses = append(newClauses, d.GenAddColumn(colDef, beforeFieldName, fieldCount == 0, fieldCount))
				beforeFieldName = fieldName

				if sfi := sourceMyS.FieldInfos[fieldName]; sfi != nil && sfi.ColumnComment != "" {
					if commentSQL := d.GenCommentColumnSQL(table, fieldName, sfi.ColumnComment); commentSQL != "" {
						standaloneSQL = append(standaloneSQL, commentSQL)
					}
				}
			}

			if len(newClauses) > 0 {
				alterClauses = append(alterClauses, newClauses...)
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "alterSQL=", newClauses)
			} else {
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "not change")
			}
			fieldCount++
		}
	} else {
		log.Printf("[Debug] Using legacy text-based field comparison for table %s", table)
		for fieldName, value := range sourceMyS.Fields.Iter() {
			if sc.Config.IsIgnoreField(table, fieldName) {
				log.Printf("ignore column %s.%s", table, fieldName)
				continue
			}
			var alterSQL string
			if destDt, has := destMyS.Fields.Get(fieldName); has {
				if value != destDt {
					alterSQL = d.GenChangeColumnText(fieldName, value)
				}
				beforeFieldName = fieldName
			} else {
				alterSQL = d.GenAddColumn(value, beforeFieldName, fieldCount == 0, fieldCount)
				beforeFieldName = fieldName
			}

			if len(alterSQL) != 0 {
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "alterSQL=", alterSQL)
				alterClauses = append(alterClauses, alterSQL)
			} else {
				log.Println("[Debug] check column.alter ", fmt.Sprintf("%s.%s", table, fieldName), "not change")
			}
			fieldCount++
		}
	}

	// 源库已经删除的字段
	if sc.Config.Drop {
		for _, name := range destMyS.Fields.Keys() {
			if sc.Config.IsIgnoreField(table, name) {
				log.Printf("ignore column %s.%s", table, name)
				continue
			}
			if _, has := sourceMyS.Fields.Get(name); !has {
				alterSQL := d.GenDropColumn(name)
				alterClauses = append(alterClauses, alterSQL)
				log.Println("[Debug] check column.drop ", fmt.Sprintf("%s.%s", table, name), "alterSQL=", alterSQL)
			}
		}
	}

	// 比对索引
	idxCommenter, _ := d.(IndexCommenter)
	for indexName, idx := range sourceMyS.IndexAll {
		if sc.Config.IsIgnoreIndex(table, indexName) {
			log.Printf("ignore index %s.%s", table, indexName)
			continue
		}
		dIdx, has := destMyS.IndexAll[indexName]
		log.Println("[Debug] indexName---->[", fmt.Sprintf("%s.%s", table, indexName),
			"] dest_has:", has, "\ndest_idx:", dIdx, "\nsource_idx:", idx)
		var indexSQLs []string
		sqlChanged := has && !sc.definitionsEqual(idx.SQL, dIdx.SQL)
		if has {
			if sqlChanged {
				indexSQLs = d.GenAddIndex(table, idx, true)
			}
		} else {
			indexSQLs = d.GenAddIndex(table, idx, false)
		}
		// COMMENT ON INDEX：只要源索引有注释就随建索引一同 emit；
		// SQL 相同但注释变化时单独 emit，避免无谓的索引重建。
		if idxCommenter != nil {
			if !has || sqlChanged {
				if idx.Comment != "" {
					indexSQLs = append(indexSQLs, idxCommenter.GenCommentIndexSQL(indexName, idx.Comment))
				}
			} else if has && idx.Comment != dIdx.Comment {
				indexSQLs = append(indexSQLs, idxCommenter.GenCommentIndexSQL(indexName, idx.Comment))
			}
		}
		if len(indexSQLs) > 0 {
			classifySQL(indexSQLs, &alterClauses, &standaloneSQL)
			log.Println("[Debug] check index.alter ", fmt.Sprintf("%s.%s", table, indexName), "sql=", indexSQLs)
		}
	}

	// drop index
	if sc.Config.Drop {
		for indexName, dIdx := range destMyS.IndexAll {
			if sc.Config.IsIgnoreIndex(table, indexName) {
				log.Printf("ignore index %s.%s", table, indexName)
				continue
			}
			if _, has := sourceMyS.IndexAll[indexName]; !has {
				dropSQL := d.GenDropIndex(table, dIdx)
				if len(dropSQL) != 0 {
					classifySQL([]string{dropSQL}, &alterClauses, &standaloneSQL)
					log.Println("[Debug] check index.drop ", fmt.Sprintf("%s.%s", table, indexName), "sql=", dropSQL)
				}
			}
		}
	}

	// 比对外键
	for foreignName, idx := range sourceMyS.ForeignAll {
		if sc.Config.IsIgnoreForeignKey(table, foreignName) {
			log.Printf("ignore foreignName %s.%s", table, foreignName)
			continue
		}
		dIdx, has := destMyS.ForeignAll[foreignName]
		log.Println("[Debug] foreignName---->[", fmt.Sprintf("%s.%s", table, foreignName),
			"] dest_has:", has, "\ndest_idx:", dIdx, "\nsource_idx:", idx)
		var fkSQLs []string
		if has {
			if !sc.definitionsEqual(idx.SQL, dIdx.SQL) {
				fkSQLs = d.GenAddForeignKey(table, idx, true)
			}
		} else {
			fkSQLs = d.GenAddForeignKey(table, idx, false)
		}
		if len(fkSQLs) > 0 {
			classifySQL(fkSQLs, &alterClauses, &standaloneSQL)
			log.Println("[Debug] check foreignKey.alter ", fmt.Sprintf("%s.%s", table, foreignName), "sql=", fkSQLs)
		}
	}

	// drop 外键
	if sc.Config.Drop {
		for foreignName, dIdx := range destMyS.ForeignAll {
			if sc.Config.IsIgnoreForeignKey(table, foreignName) {
				log.Printf("ignore foreignName %s.%s", table, foreignName)
				continue
			}
			if _, has := sourceMyS.ForeignAll[foreignName]; !has {
				log.Println("[Debug] foreignName --->[", fmt.Sprintf("%s.%s", table, foreignName), "]", "didx:", dIdx)
				dropSQL := d.GenDropForeignKey(table, dIdx)
				if len(dropSQL) != 0 {
					classifySQL([]string{dropSQL}, &alterClauses, &standaloneSQL)
					log.Println("[Debug] check foreignKey.drop ", fmt.Sprintf("%s.%s", table, foreignName), "sql=", dropSQL)
				}
			}
		}
	}

	return alterClauses, standaloneSQL
}

// mergeExtraIndexes 把通过 IndexEnumerator 枚举到的非约束索引合并进 MySchema.IndexAll。
// 若 IndexAll 中已经存在同名条目（由 ParseSchema 从 CREATE TABLE 字符串解析得到的 PK/UNIQUE），
// 以已有为准，不覆盖。
func (sc *SchemaSync) mergeExtraIndexes(mys *MySchema, db *MyDb, table string) {
	if mys == nil || db == nil {
		return
	}
	indexes, err := db.TableIndexesExtra(table)
	if err != nil {
		log.Printf("[Debug] enumerate extra indexes for %q failed: %s", table, errString(err))
		return
	}
	if mys.IndexAll == nil {
		mys.IndexAll = make(map[string]*DbIndex)
	}
	for _, idx := range indexes {
		if _, has := mys.IndexAll[idx.Name]; has {
			continue
		}
		mys.IndexAll[idx.Name] = idx
	}
}

// loadTableComment 通过 TableCommentEnumerator 读取表注释并填充到 MySchema.TableComment。
func (sc *SchemaSync) loadTableComment(mys *MySchema, db *MyDb, table string) {
	if mys == nil || db == nil {
		return
	}
	comment, err := db.TableComment(table)
	if err != nil {
		log.Printf("[Debug] read table comment for %q failed: %s", table, errString(err))
		return
	}
	mys.TableComment = comment
}

// loadTriggers 通过 TriggerEnumerator 把表上用户触发器填充进 MySchema.Triggers。
func (sc *SchemaSync) loadTriggers(mys *MySchema, db *MyDb, table string) {
	if mys == nil || db == nil {
		return
	}
	triggers, err := db.TableTriggers(table)
	if err != nil {
		log.Printf("[Debug] enumerate triggers for %q failed: %s", table, errString(err))
		return
	}
	if len(triggers) == 0 {
		return
	}
	if mys.Triggers == nil {
		mys.Triggers = make(map[string]*DbTrigger, len(triggers))
	}
	for _, trg := range triggers {
		mys.Triggers[trg.Name] = trg
	}
}

// diffTableComment 对比两侧表注释，返回 COMMENT ON TABLE DDL；无变化返回空串。
// 源库注释清空（TableComment 为 ""）且目标有内容时，输出 COMMENT ON TABLE ... IS NULL 以撤销。
func (sc *SchemaSync) diffTableComment(alter *TableAlterData) string {
	d := sc.getDialect()
	tce, ok := d.(TableCommentEnumerator)
	if !ok {
		return ""
	}
	src := alter.SchemaDiff.Source.TableComment
	dst := alter.SchemaDiff.Dest.TableComment
	if src == dst {
		return ""
	}
	return tce.GenCommentTableSQL(alter.Table, src)
}

// diffTriggers 对比两侧 trigger，返回待执行的 DDL（DROP + CREATE）。
// 语义：
//   - 目标有、源没有 → DROP（仅在 cfg.Drop 时）
//   - 源有、目标没有 → CREATE
//   - 两边都有但 Definition 不同 → DROP + CREATE（PG 无法 ALTER 触发器定义）
func (sc *SchemaSync) diffTriggers(alter *TableAlterData) []string {
	d := sc.getDialect()
	te, ok := d.(TriggerEnumerator)
	if !ok {
		return nil
	}
	source := alter.SchemaDiff.Source.Triggers
	dest := alter.SchemaDiff.Dest.Triggers

	var sqls []string
	for name, src := range source {
		dst, has := dest[name]
		if has && sc.definitionsEqual(src.Definition, dst.Definition) {
			continue
		}
		if has {
			sqls = append(sqls, te.GenDropTrigger(dst))
		}
		sqls = append(sqls, te.GenAddTrigger(src))
	}
	if sc.Config.Drop {
		for name, dst := range dest {
			if _, has := source[name]; has {
				continue
			}
			sqls = append(sqls, te.GenDropTrigger(dst))
		}
	}
	return sqls
}

// definitionsEqual 判定两段 DDL 定义文本是否语义等价，优先走 dialect 的
// DefinitionComparer 可选实现以消除 round-trip noise；未实现时回退到精确相等。
func (sc *SchemaSync) definitionsEqual(a, b string) bool {
	if a == b {
		return true
	}
	if dc, ok := sc.getDialect().(DefinitionComparer); ok {
		return dc.DefinitionsEqual(a, b)
	}
	return false
}

// ExtensionSyncSQLs 对比源/目标库的扩展，返回最前置执行的 CREATE 语句与
// 最后执行的 DROP 语句。前者保证依赖 extension 的对象（函数/索引等）创建
// 时不会报 "type/operator does not exist"；后者仅在 cfg.Drop=true 时返回，
// 且放在所有清理步骤之后，避免级联误删用户对象。
func (sc *SchemaSync) ExtensionSyncSQLs() (pre, post []string) {
	d := sc.getDialect()
	ee, ok := d.(ExtensionEnumerator)
	if !ok {
		return nil, nil
	}
	sourceExts, err := sc.SourceDb.Extensions()
	if err != nil {
		log.Printf("enumerate source extensions failed: %s", errString(err))
		return nil, nil
	}
	destExts, err := sc.DestDb.Extensions()
	if err != nil {
		log.Printf("enumerate dest extensions failed: %s", errString(err))
		return nil, nil
	}
	destSet := make(map[string]*DbExtension, len(destExts))
	for _, e := range destExts {
		destSet[e.Name] = e
	}
	sourceSet := make(map[string]*DbExtension, len(sourceExts))
	for _, e := range sourceExts {
		sourceSet[e.Name] = e
	}
	for _, src := range sourceExts {
		if _, has := destSet[src.Name]; !has {
			pre = append(pre, ee.GenAddExtension(src))
		}
	}
	if sc.Config.Drop {
		for _, dst := range destExts {
			if _, has := sourceSet[dst.Name]; !has {
				post = append(post, ee.GenDropExtension(dst))
			}
		}
	}
	return pre, post
}

// FunctionSyncSQLs 对比源/目标库中用户自定义函数，返回表循环前需要执行的 CREATE
// 语句（保证 trigger 建立时函数已存在）与表循环后需要执行的 DROP 语句
// （保证 trigger 先被移除后再回收孤立函数）。
// 未实现 FunctionEnumerator 的 dialect（如 MySQL）返回 (nil, nil)。
func (sc *SchemaSync) FunctionSyncSQLs() (pre, post []string) {
	d := sc.getDialect()
	fe, ok := d.(FunctionEnumerator)
	if !ok {
		return nil, nil
	}
	sourceFns, err := sc.SourceDb.Functions()
	if err != nil {
		log.Printf("enumerate source functions failed: %s", errString(err))
		return nil, nil
	}
	destFns, err := sc.DestDb.Functions()
	if err != nil {
		log.Printf("enumerate dest functions failed: %s", errString(err))
		return nil, nil
	}
	fnKey := func(fn *DbFunction) string { return fn.Name + "(" + fn.Signature + ")" }

	destMap := make(map[string]*DbFunction, len(destFns))
	for _, fn := range destFns {
		destMap[fnKey(fn)] = fn
	}
	sourceMap := make(map[string]*DbFunction, len(sourceFns))
	for _, fn := range sourceFns {
		sourceMap[fnKey(fn)] = fn
	}

	for _, src := range sourceFns {
		dst, has := destMap[fnKey(src)]
		if has && sc.definitionsEqual(src.Definition, dst.Definition) {
			continue
		}
		pre = append(pre, fe.GenAddFunction(src))
	}
	if sc.Config.Drop {
		for _, dst := range destFns {
			if _, has := sourceMap[fnKey(dst)]; has {
				continue
			}
			post = append(post, fe.GenDropFunction(dst))
		}
	}
	return pre, post
}

// classifySQL separates standalone SQL from ALTER TABLE clauses
func classifySQL(sqls []string, alterClauses, standaloneSQL *[]string) {
	for _, s := range sqls {
		upper := strings.ToUpper(strings.TrimSpace(s))
		if strings.HasPrefix(upper, "CREATE ") ||
			strings.HasPrefix(upper, "DROP INDEX") ||
			strings.HasPrefix(upper, "COMMENT ON") {
			*standaloneSQL = append(*standaloneSQL, s)
		} else {
			*alterClauses = append(*alterClauses, s)
		}
	}
}

// SyncSQL4DestInTx 在单个事务中顺序执行一批 DDL 语句。
// 任意一条失败则整批回滚，避免部分成功造成的状态分裂。
// PostgreSQL 原生支持事务性 DDL；MySQL 的 DDL 会隐式提交，BEGIN/COMMIT
// 仅作为语义包装但不影响正确性。
func (sc *SchemaSync) SyncSQL4DestInTx(sqls []string) error {
	if len(sqls) == 0 {
		return nil
	}
	for _, s := range sqls {
		xcolor.Green(s)
	}
	log.Print("Exec_Tx_SQL:\n>>>>>>\n", xcolor.GreenString("%s", strings.Join(sqls, ";\n")+";"), "\n<<<<<<<<\n\n")

	t := newMyTimer()
	defer t.stop()

	tx, err := sc.DestDb.sqlDB.Begin()
	if err != nil {
		log.Println("tx begin failed:", errString(err))
		return err
	}
	for _, s := range sqls {
		rows, qerr := tx.Query(s)
		log.Println("query_one_tx:[", s, "]", errString(qerr))
		if rows != nil {
			_ = rows.Close()
		}
		if qerr != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Println("tx rollback failed:", errString(rbErr))
			}
			log.Println("EXEC_TX_SQL_FAILED:", errString(qerr))
			return qerr
		}
	}
	if err := tx.Commit(); err != nil {
		log.Println("tx commit failed:", errString(err))
		return err
	}
	log.Println("EXEC_TX_SQL_SUCCESS, used:", t.usedSecond())
	return nil
}

// SyncSQL4Dest sync schema change
func (sc *SchemaSync) SyncSQL4Dest(sqlStr string, sqls []string) error {
	sqlStr = strings.TrimSpace(sqlStr)
	xcolor.Green(sqlStr)
	log.Print("Exec_SQL:\n>>>>>>\n", xcolor.GreenString(sqlStr), "\n<<<<<<<<\n\n")
	if len(sqlStr) == 0 {
		log.Println("sql_is_empty, skip")
		return nil
	}
	t := newMyTimer()
	ret, err := sc.DestDb.Query(sqlStr)

	defer func() {
		if ret != nil {
			err := ret.Close()
			if err != nil {
				log.Println("close ret error:", errString(err))
				return
			}
		}
	}()

	if err != nil && len(sqls) > 1 {
		log.Println("Exec_mut_query failed, err=", errString(err), ", now try exec SQLs foreach")
		tx, errTx := sc.DestDb.sqlDB.Begin()
		if errTx != nil {
			log.Println("db.Begin failed", errString(err))
			return errTx
		}
		for _, sql := range sqls {
			ret, err = tx.Query(sql)
			log.Println("query_one:[", sql, "]", errString(err))
			if err != nil {
				break
			}
		}
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}
	t.stop()
	if err != nil {
		log.Println("EXEC_SQL_FAILED:", errString(err))
		return err
	}
	log.Println("EXEC_SQL_SUCCESS, used:", t.usedSecond())
	cl, err := ret.Columns()
	log.Println("EXEC_SQL_RET:", cl, err)
	return err
}
