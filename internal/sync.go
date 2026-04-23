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

	if sSchema == dSchema {
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
		for _, idx := range alter.SchemaDiff.Source.IndexAll {
			if idx.IndexType != indexTypeIndex {
				continue
			}
			upperDef := strings.ToUpper(strings.TrimSpace(idx.SQL))
			if strings.HasPrefix(upperDef, "CREATE INDEX") || strings.HasPrefix(upperDef, "CREATE UNIQUE INDEX") {
				alter.SQL = append(alter.SQL, strings.TrimRight(idx.SQL, ";")+";")
			}
		}
		return alter
	}

	alterClauses, standaloneSQL := sc.getSchemaDiff(alter)
	if len(alterClauses) == 0 && len(standaloneSQL) == 0 {
		return alter
	}
	alter.Type = alterTypeAlter
	if len(alterClauses) > 0 {
		alter.SQL = append(alter.SQL, d.WrapAlterSQL(table, alterClauses, cfg.SingleSchemaChange)...)
	}
	alter.SQL = append(alter.SQL, standaloneSQL...)

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
	for indexName, idx := range sourceMyS.IndexAll {
		if sc.Config.IsIgnoreIndex(table, indexName) {
			log.Printf("ignore index %s.%s", table, indexName)
			continue
		}
		dIdx, has := destMyS.IndexAll[indexName]
		log.Println("[Debug] indexName---->[", fmt.Sprintf("%s.%s", table, indexName),
			"] dest_has:", has, "\ndest_idx:", dIdx, "\nsource_idx:", idx)
		var indexSQLs []string
		if has {
			if idx.SQL != dIdx.SQL {
				indexSQLs = d.GenAddIndex(table, idx, true)
			}
		} else {
			indexSQLs = d.GenAddIndex(table, idx, false)
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
			if idx.SQL != dIdx.SQL {
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
