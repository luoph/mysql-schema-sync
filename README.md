# db-schema-sync

数据库 Schema 自动同步工具

用于将 `线上` 数据库 Schema **变化**同步到 `本地测试环境`！
只同步 Schema、不同步数据。

支持数据库：**MySQL**、**PostgreSQL**（自动根据 DSN 格式识别）

## 功能

1. 同步**新表**
2. 同步**字段** 变动：新增、修改
3. 同步**索引** 变动：新增、修改
4. 同步**外键** 变动：新增、修改
5. 同步**字段注释**（PostgreSQL 使用 `COMMENT ON COLUMN`）
6. 同步**字段顺序**（仅 MySQL，PostgreSQL 不支持）
7. 支持**预览**（只对比不同步变动）
8. **邮件**通知变动结果
9. 支持屏蔽更新**表、字段、索引、外键**
10. 支持本地比线上额外多一些表、字段、索引、外键
11. 支持分区表（同步除分区以外的变更）
12. 支持每条 DDL 只执行单个修改操作（兼容 TiDB），通过 `single_schema_change` 控制

## 安装

```bash
go install github.com/luoph/mysql-schema-sync@master
```

## 配置

参考默认配置文件 config.json 配置同步源、目的地址。
修改邮件接收人，当运行失败或者有表结构变化的时候你可以收到邮件通知。

默认情况不会对多出的**表、字段、索引、外键**删除。若需要删除可以使用 `-drop` 参数。

默认情况不会同步字段顺序差异。若需要同步字段顺序，可以使用 `-field-order` 参数（仅 MySQL，注意：此操作可能需要重建表，影响性能）。

### MySQL 配置示例

```json
{
    "source": "user:pass@tcp(127.0.0.1:3306)/source_db",
    "dest": "user:pass@tcp(127.0.0.1:3306)/dest_db",
    "alter_ignore": {
        "tb1*": {
            "column": ["aaa", "a*"],
            "index": ["aa"],
            "foreign": []
        }
    },
    "tables": [],
    "tables_ignore": [],
    "email": {
        "send_mail": false,
        "smtp_host": "smtp.163.com:25",
        "from": "xxx@163.com",
        "password": "xxx",
        "to": "xxx@163.com"
    }
}
```

### PostgreSQL 配置示例

```json
{
    "source": "postgres://user:pass@127.0.0.1:5432/source_db?sslmode=disable",
    "dest": "postgres://user:pass@127.0.0.1:5432/dest_db?sslmode=disable",
    "alter_ignore": {},
    "tables": [],
    "tables_ignore": []
}
```

DSN 自动识别规则：
- `postgres://` 或 `postgresql://` 开头 → PostgreSQL
- 包含 `host=` → PostgreSQL（keyword 格式）
- 其他 → MySQL

> **注意**：源库和目标库必须是同一种数据库类型，不支持跨库同步。

### JSON 配置项说明

| 配置项 | 说明 |
|--------|------|
| source | 数据库同步源 |
| dest | 待同步的数据库 |
| tables | 需要同步的表（为空则全部），如 `["goods", "order_*"]` |
| tables_ignore | 忽略的表 |
| alter_ignore | 忽略修改的配置，支持通配符 `*`，可配置 column、index、foreign |
| email | 同步完成后发送邮件通知 |
| single_schema_change | 每条 DDL 是否只执行单个修改操作 |

## 运行

### 直接运行

```shell
./db-schema-sync -conf mydb_conf.json -sync
```

### 预览并生成变更 SQL

```shell
./db-schema-sync -drop -conf mydb_conf.json 2>/dev/null > db_alter.sql
```

### 使用 shell 调度

```shell
bash check.sh
```

每个 json 文件配置一个目的数据库，check.sh 脚本会依次运行每份配置。
log 存储在当前的 log 目录中。

### 自动定时运行

添加 crontab 任务：

```shell
30 * * * * cd /your/path/xxx/ && bash check.sh >/dev/null 2>&1
```

### 参数说明

```shell
db-schema-sync [-conf] [-dest] [-source] [-sync] [-drop] [-field-order]
```

```text
  -conf string
        配置文件名称
  -dest string
        待同步的数据库
        MySQL:      test@(10.10.0.1:3306)/test_1
        PostgreSQL: postgres://test@10.10.0.1:5432/test_1
        该项不为空时，忽略 -conf 参数
  -drop
        是否对本地多出的字段和索引进行删除（默认否）
  -field-order
        是否同步字段顺序，仅 MySQL（默认否）
  -http
        启用 web 站点显示运行结果报告的地址，如 :8080（默认否）
  -source string
        数据库同步源
        MySQL:      test@(127.0.0.1:3306)/test_0
        PostgreSQL: postgres://test@127.0.0.1:5432/test_0
  -sync
        是否将修改同步到数据库中去（默认否）
  -tables string
        待检查同步的数据库表，为空则是全部
        eg: product_base,order_*
  -single_schema_change
        每条 DDL 是否只执行单个修改操作（默认否）
```
