+++

weight = 2
title = "SQL迁移工具"
+++

# SQL迁移工具

SQL迁移工具帮助用户将SQL语法转换为openLooKeng兼容的SQL语法。目前仅支持Hive SQL语法。

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
| SHOW FUCNTIONS| 不支持带“like”的语句| [SHOW FUCNTIONS](../sql/show-functions.md)|
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

## 使用SQL迁移工具

**交互模式**

该工具支持交互模式运行。例如：

    java -jar hetu-sql-migration-tool-010.jar
    presto:HIVE>
    presto:HIVE> INSERT INTO TABLE table1 VALUES(10, "NAME");
    ==========converted result==========
    INSERT INTO table1
     VALUES
      ROW (10, 'NAME')
    
    =================Success=============


下面是一些常用的命令：

| 命令| 描述|
|----------|----------|
| `exit`或`quit`| 退出交互模式|
| `history`| 获得前面的输入语句|
| `help`| 替换帮助信息|

**批处理模式**

该工具还可以获取参数并以批处理模式运行。该模式有五个参数，分别是“file”、“sourceType”、“execute”、“output”和“config”。  各参数的含义如下：

| 参数名称| 描述|
|----------|----------|
| `file`| 包含SQL语句的文件，以“；”分隔。  文件中的所有SQL都可以进行批量转换。|
| `sourceType`| 输入SQL语句的类型，如`hive`、`impala`。可选参数，默认值为`hive`。|
| `output`| 转换后SQL结果的存放目录。|
| `config`| SQL迁移工具的配置文件。|

*提示：*

*如果用户有大量的SQL语句需要转换，更好的方法是将所有语句合并到一个文件中，并使用批处理模式*

批处理模式使用示例：


        java -jar hetu-sql-migration-tool-010.jar --file /home/Query01.sql --output ./
        May 26, 2020 5:27:10 PM io.airlift.log.Logger info
        INFO: Migration Completed.
        May 26, 2020 5:27:10 PM io.airlift.log.Logger info
        INFO: Result is saved to .//Query01_1590485230193.sql
        May 26, 2020 5:27:10 PM io.airlift.log.Logger info
        INFO: Result is saved to .//Query01_1590485230193.csv


当指定`file`时，必须提供参数`output`。转换后的结果会保存为`output`目录下的两个文件：

1. “.sql”文件保存转换成功的SQL语句。

2. “.csv”文件保存所有转换的中间结果，包括原始SQL、转换后的SQL、源SQL类型、状态和消息。

**执行模式**

可以使用命令直接执行查询，并在转换完成后退出工具。使用`execute`的示例：


    java -jar hetu-sql-migration-tool-010.jar --execute "INSERT INTO TABLE T1 VALUES(10, 'presto')" --sourceType hive
    
    ==========converted result==========
    INSERT INTO t1
     VALUES
      ROW (10, 'presto')
    
    =================Success=============


如果用户只指定了参数`execute`，则转换后的结果会打印到屏幕上。用户可自行指定`output`参数，将结果保存到目标文件中。

用户还可以提供`config`参数来控制转换行为。`config`示例：

文件名为“config.properties”，内容如下：

    convertDecimalLiteralsAsDouble=true
    
    java -jar hetu-sql-migration-tool-010.jar --execute "INSERT INTO TABLE T1 select 2.0 * 3" --config config.properties
    
    ==========converted result==========
    INSERT INTO t1
    SELECT (DECIMAL '2.0' * 3)
    
    =================Success=============


配置文件目前只支持一个属性`convertDecimalLiteralsAsDouble`。该属性意思是是否将十进制文本转换为double默认值为`false`，意味着将十进制文字转换为“decimal”类型。

## 限制

- 不支持对SQL语句中的UDF和函数进行转换。