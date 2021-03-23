
# SQL迁移工具

SQL迁移工具帮助用户将SQL语法转换为ANSI 2003 的SQL语法。目前仅支持Hive 和 Impala SQL语法。

## 使用SQL迁移工具
下载`hetu-sql-migration-cli-{version number}-executable.jar`， 并重命名为 `openlk-sql-migration-cli`， 可以运行该命令`chmod +x`让它变成可执行的， 然后运行它。

**交互模式**

该工具支持交互模式运行。例如：

```shell
./openlk-sql-migration-cli --type hive
```

```sql
lk:HIVE>
lk:HIVE> INSERT INTO TABLE table1 VALUES(10, "NAME");
==========converted result==========
INSERT INTO table1
 VALUES
  ROW (10, 'NAME')

=================Success=============
```

| 参数名称| 描述|
|----------|----------|
| `--type` or `-t`| 输入SQL语句的类型，如`hive`、`impala`。可选参数，默认值为`hive`。|


下面是一些常用的命令：

| 命令| 描述|
|----------|----------|
| `!chtype value`| 修改当前会话的源sql类型|
| `exit`或`quit`| 退出交互模式|
| `history`| 获得前面的输入语句|
| `help`| 替换帮助信息|

**批处理模式**

该工具还可以获取参数并以批处理模式运行。该模式有五个参数，分别是“file”、“sourceType”、“execute”、“output”和“config”。  各参数的含义如下：

| 参数名称| 描述|
|----------|----------|
| `--file` or `-f`| 包含SQL语句的文件，以“;”分隔。文件中的所有SQL都可以进行批量转换。|
| `--type` or `-t`| 输入SQL语句的类型，如`hive`、`impala`。可选参数，默认值为`hive`。|
| `--output` or `-o`| 转换后SQL结果的存放目录。|
| `--config` or `-c`| SQL迁移工具的配置文件。|
| `--debug` or `-d`| 需要打印debug到控制界面时，设置值为true。|

*提示：*

*如果用户有大量的SQL语句需要转换，建议是将所有语句合并到一个文件中，并使用批处理模式*

批处理模式使用示例：


```shell
    ./openlk-sql-migration-cli --file /home/Query01.sql --output ./
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Migration Completed.
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Result is saved to .//Query01_1590485230193.html
```


当指定`file`时，必须提供参数`output`。转换后的结果会保存在`output`目录下的html文件中，用户可以使用任意浏览器打开该html文件查看详细转化结果。

**执行模式**

可以使用命令直接执行查询，并在转换完成后退出工具。使用`execute`的示例：


```shell
./openlk-sql-migration-cli --execute "INSERT INTO TABLE T1 VALUES(10, 'openLooKeng')" --type hive


==========converted result==========
INSERT INTO t1
 VALUES
  ROW (10, 'openLooKeng')

=================Success=============
```


如果用户只指定了参数`execute`，则转换后的结果会打印到屏幕上。用户可自行指定`output`参数，将结果保存到目标文件中。

用户还可以提供`config`参数来控制转换行为。`config`示例：

文件名为“config.properties”，内容如下：

``` shell
convertDecimalLiteralsAsDouble=true


./openlk-sql-migration-cli --execute "INSERT INTO TABLE T1 select 2.0 * 3" --config config.properties


==========converted result==========
INSERT INTO t1
SELECT (DECIMAL '2.0' * 3)

=================Success=============
```


配置文件目前只支持一个属性`convertDecimalLiteralsAsDouble`。该属性意思是是否将十进制文本转换为double默认值为`false`，意味着将十进制文字转换为“decimal”类型。

## Hive语句检查列表：

以下Hive语句完全支持：

| SQL|
|----------|
| USE DATABASE/SCHEMA|
| SHOW TABLES|
| DROP VIEW|
| DESCRIBE view\_name|
| CREATE ROLE|
| GRANT ROLE|
| REVOKE ROLE|
| DROP ROLE|
| SHOW ROLES|
| SHOW CURRENT ROLES|
| SET ROLE|
| GRANT|
| REVOKE|
| DELETE|
| EXPLAIN ANALYZE|
| SHOW|
| SHOW FUCNTIONS|

以下Hive语句部分支持，即不支持某些关键字或属性：

| SQL| 描述| openLooKeng语法参考|
|----------|----------|----------|
| CREATE DATABASE/SCHEMA| 不支持带“COMMENT”、“WITH DBPROPERTIES”的语句| [CREATE SCHEMA](../sql/create-schema.md)|
| DROP DATABASE/SCHEMA| 不支持带“CASCADE”的语句| [DROP SCHEMA](../sql/drop-schema.md)|
| SHOW DATABASE/SCHEMA| 不支持带“like”的语句| [SHOW SCHEMA](../sql/show-schemas.md)|
| CREATE TABLE| 不支持带“SKEWED BY”、“ROW FORMAT”的语句| [CREATE TABLE](../sql/create-table.md)|
| DROP TABLE| 不支持带“PURGE”的语句| [DROP TABLE](../sql/drop-table.md)|
| ALTER TABLE| 只支持“重命名表”和“添加单列”| [ALTER TABLE](../sql/alter-table.md)|
| SHOW CREATE TABLE| 对Hive而言，SHOW TABLE可用于表和视图。但在openLooKeng中，这只能应用于表。| [SHOW CREATE TABLE](../sql/show-create-table.md)|
| DESCRIBE| 支持带列名的语句| [DESCRIBE](../sql/describe.md)|
| CREATE VIEW| 不支持带“COMMENT”、“WITH DBPROPERTIES”的语句| [CREATE VIEW](../sql/create-view.md)|
| SHOW COLUMNS| 不支持带“like”的语句| [SHOW COLUMNS](../sql/show-columns.md)|
| SHOW GRANT| 不支持指定用户或角色的语句| [SHOW GRANT](../sql/show-grants.md)|
| INSERT| 不支持带“partition”的语句| [INSERT](../sql/insert.md)|
| SELECT| 不支持带“cluster by”、“offset”的语句| [SELECT](../sql/select.md)|

由于特性差异，如下Hive语句暂不支持：

| SQL|
|----------|
| ALTER DATABASE/SCHEMA|
| DESCRIBE DATABASE/SCHEMA|
| SHOW TABLE EXTENDED|
| SHOW TBLPROPERTIES|
| TRUNCATE TABLE|
| MSCK REPAIR TABLE|
| ALTER PARTITION|
| ALTER COLUMN|
| ALTER VIEW|
| SHOW VIEWS|
| CREATE MATERIALIZED VIEW|
| DROP MATERIALIZED VIEW|
| ALTER MATERIALIZED VIEW|
| SHOW MATERIALIZED VIEWS|
| CREATE FUNCTION|
| DROP FUNCTION|
| RELOAD FUNCTION|
| CREATE INDEX|
| DROP INDEX|
| ALTER INDEX|
| SHOW INDEX(ES)|
| SHOW PARTITIONS|
| Describe partition|
| CREATE MACRO|
| DROP MACRO|
| SHOW ROLE GRANT|
| SHOW PRINCIPALS|
| SHOW LOCKS|
| SHOW CONF|
| SHOW TRANSACTIONS|
| SHOW COMPACTIONS|
| ABORT TRANSACTIONS|
| LOAD|
| UPDATE|
| MERGE|
| EXPORT|
| IMPORT|
| EXPLAIN|
| SET|
| RESET|

## Impala 语句检查列表：

以下Impala语句完全支持：

 SQL                 |
| ------------------- |
| CREATE SCHEMA       |
| RENAME TABLE        |
| DROP VIEW           |
| CREATE ROLE         |
| SHOW CREATE TABLE   |
| SHOW CREATE VIEW    |
| SHOW ROLES          |
| EXPLAIN             |

以下Impala语句部分支持，即不支持某些关键字或属性：

| SQL                    | Description                                                  | openLooKeng Syntax Reference                     |
| ---------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| DROP SCHEMA            | 不支持带 "CASCADE"的语句                   | [DROP SCHEMA](../sql/drop-schema.md)             |
| CREATE TABLE           | 不支持带 "ROW FORMAT", "WITH SERDEPROPERTIES", "CACHED IN" 的语句     | [CREATE TABLE](../sql/create-table.md)           |
| CREATE TABLE LIKE      | 不支持带 "PARQUET" 的语句     | [CREATE TABLE](../sql/create-table.md)           |
| DROP TABLE             | 不支持带 "PURGE" 的语句                     | [DROP TABLE](../sql/drop-table.md)               |
| CREATE VIEW            | 不支持带 "IF NOT EXISTS", "ALIAS" 的语句 | [CREATE VIEW](../sql/create-view.md)             |
| ALTER VIEW             | 不支持别名. "ALTER VIEW" 会被转化为 "CREATE OR REPLACE VIEW"  | [ALTER TABLE](../sql/create-view.md)             |
| DESCRIBE               | 仅仅支持DESCRIBE表, 其他都不支持                     | [DESCRIBE](../sql/describe.md)                   |
| GRANT ROLE             | 不支持将角色赋予组(GROUP)                      | [GRANT ROLES](../sql/grant-roles.md)                       |
| GRANT                  | 仅支持"SELECT","INSERT" 权限, 且仅支持赋予给角色                      | [GRANT](../sql/grant.md)                       |
| REVOKE ROLE            | 不支持将角色从组(GROUP) 中收回                 | [REVOKE ROLES](../sql/revoke-roles.md)                       |
| REVOKE                 | 仅支持"SELECT","INSERT" 权限, 且仅支持从角色收中收回                    | [REVOKE](../sql/revoke.md)                       |
| INSERT INTO            | 不支持带 "WITH", "HINT", "PARTITION" 的语句                    | [INSERT INTO](../sql/insert.md)                       |
| DELETE                 | 不支持带 "JOIN" 的语句                    | [DELETE](../sql/delete.md)                       |
| SHOW SCHEMAS           | 不支持带多个通配符的语句                    | [DELETE](../sql/show-schemas.md)                       |
| SHOW TABLES            | 不支持带多个通配符的语句                    | [DELETE](../sql/show-tables.md)                       |
| ADD COMMENTS           | 不支持给数据库和列添加评论                  | [COMMENT](../sql/comment.md)                       |
| SET SESSION            | 仅支持 "SET" 和 "SET ALL"                                          | [SET SESSION](../sql/set-session.md)                       |
| ADD COLUMNS            | 不支持在一条语句添加多列，也不支持设置kudu属性                          | [ALTER TABLE](../sql/alter-table.md)                       |
| SHOW FUNCTIONS         | 仅支持显示全部的函数或带“LIKE”的语句                          | [SHOW FUNCTIONS](../sql/show-functions.md)                       |


由于特性差异，如下Impala语句暂不支持：

| SQL                      |
| ------------------------ |
| ALTER SCHEMA    |
| CREATE KUDU TABLE |
| REPLACE COLUMNS       |
| DROP SINGLE COLUMN           |
| ALTER TABLE OWNER           |
| ALTER KUDU TABLE           |
| TRUNCATE TABLE           |
| RENAME VIEW           |
| ALTER VIEW OWNER           |
| COMPUTE STATS           |
| DROP STATS           |
| CREATE FUNCTION           |
| REFRESH FUNCTION           |
| UPDATE TABLE           |
| UPSERT           |
| SHOW TABLE/COLUMN STATS           |
| SHOW PARTITIONS           |
| SHOW FILES           |
| SHOW ROLE GRANT           |
| DROP SINGLE COLUMN           |
| SHUTDOWN           |
| INVALIDATE META           |
| LOAD DATA           |
| REFRESH META           |
| REFRESH AUTH           |


## 限制

不支持对SQL语句中的UDF和函数进行转换。