//  Copyright(C) 2025 github.com/hidu  All Rights Reserved.
//  Author: hidu <duv123+git@gmail.com>
//  Date: 2025-10-21

package internal

import (
	"fmt"
	"log"
	"strings"

	"github.com/xanygo/anygo/cli/xcolor"
)

func Execute(cfg *Config) {
	scs := newStatics(cfg)
	defer func() {
		scs.timer.stop()
		scs.sendMailNotice(cfg)
	}()

	sc := NewSchemaSync(cfg)
	allTables := sc.AllDBTables()
	// log.Println("source db table total:", len(allTables))

	// 库级对象同步（目前仅 PostgreSQL 触发）：
	//   1) preFnSQLs：表同步前执行 CREATE OR REPLACE FUNCTION，保证 trigger 建立时其
	//      关联函数已存在；
	//   2) postFnSQLs：表同步后执行 DROP FUNCTION，此时已无 trigger 依赖它。
	preFnSQLs, postFnSQLs := sc.FunctionSyncSQLs()
	if err := runDDLBatch(sc, cfg, preFnSQLs, "pre_function_sync"); err != nil {
		log.Println("pre_function_sync failed:", errString(err))
	}

	changedTables := make(map[string][]*TableAlterData)

	for _, table := range allTables {
		xcolor.Green("start checking table %q ...", table)
		if !cfg.CheckMatchTables(table) {
			xcolor.Cyan("table %q skipped by not match", table)
			continue
		}

		if cfg.CheckMatchIgnoreTables(table) {
			xcolor.Cyan("table %q skipped by ignore", table)
			continue
		}

		sd := sc.getAlterDataByTable(table, cfg)

		switch sd.Type {
		case alterTypeNo:
			xcolor.Yellow("table %q not changed", table)
			continue
		case alterTypeDropTable:
			if !sc.Config.Drop {
				xcolor.Yellow("table %q skipped, only exists in destination's database", table)
				continue
			}
			xcolor.Yellow("drop table %q, only exists in destination's database", table)
		default:
		}

		fmt.Printf("\n%s\n\n", sd)

		relationTables := sd.SchemaDiff.RelationTables()
		log.Printf("table %q RelationTables: %q", table, relationTables)

		// 将所有有外键关联的单独放
		groupKey := "multi"
		if len(relationTables) == 0 {
			groupKey = "single_" + table
		}
		if _, has := changedTables[groupKey]; !has {
			changedTables[groupKey] = make([]*TableAlterData, 0)
		}
		changedTables[groupKey] = append(changedTables[groupKey], sd)
	}

	var countSuccess int
	var countFailed int
	canRunTypePref := "single"

	// 先执行单个表的
runSync:
	for typeName, sds := range changedTables {
		if !strings.HasPrefix(typeName, canRunTypePref) {
			continue
		}
		log.Println("runSyncType:", typeName)
		var sqls []string
		var sts []*tableStatics
		for _, sd := range sds {
			for index := range sd.SQL {
				sql := strings.TrimRight(sd.SQL[index], ";")
				sqls = append(sqls, sql)

				st := scs.newTableStatics(sd.Table, sd, index)
				sts = append(sts, st)
			}
		}

		sql := strings.Join(sqls, ";\n") + ";"
		var ret error

		if sc.Config.Sync {
			ret = sc.SyncSQL4Dest(sql, sqls)
			if ret == nil {
				countSuccess++
			} else {
				countFailed++
			}
		}
		for _, st := range sts {
			st.alterRet = ret
			st.schemaAfter = sc.DestDb.GetTableSchema(st.table)
			st.timer.stop()
		}
	} // end for

	// 最后再执行多个表的 alter
	if canRunTypePref == "single" {
		canRunTypePref = "multi"
		goto runSync
	}

	// 表循环完成后再执行函数清理；此时源已不存在的 trigger 已被 DROP，孤立函数可安全回收。
	if err := runDDLBatch(sc, cfg, postFnSQLs, "post_function_sync"); err != nil {
		log.Println("post_function_sync failed:", errString(err))
	}

	if sc.Config.Sync {
		log.Println("execute_all_sql_done, success_total:", countSuccess, "failed_total:", countFailed)
	}
}

// runDDLBatch 打印并（cfg.Sync=true 时）向目标库执行一批独立 DDL 语句。
// 用于库级对象同步（例如 CREATE OR REPLACE FUNCTION / DROP FUNCTION），
// 每条语句独立执行，单条失败不影响其余语句。
func runDDLBatch(sc *SchemaSync, cfg *Config, sqls []string, tag string) error {
	if len(sqls) == 0 {
		return nil
	}
	log.Printf("\n-- %s (%d) --\n", tag, len(sqls))
	for _, s := range sqls {
		fmt.Println(s)
	}
	if !cfg.Sync {
		return nil
	}
	var firstErr error
	for _, s := range sqls {
		if err := sc.SyncSQL4Dest(s, []string{s}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
