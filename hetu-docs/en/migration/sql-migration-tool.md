+++

weight = 2
titile = "SQL Migration Tool"
+++

# SQL Migration Tool

SQL Migration tool helps user to transform SQL Syntax to openLooKeng compatible SQL syntax. Currently, only Hive SQL syntax is supported.

## Check list of  Hive Statements:

Below Hive statements are fully supported:

| SQL                 |
| ------------------- |
| USE DATABASE/SCHEMA |
| SHOW TABLES         |
| DROP VIEW           |
| DESCRIBE view_name  |
| CREATE ROLE         |
| GRANT ROLE          |
| REVOKE ROLE         |
| DROP ROLE           |
| SHOW ROLES          |
| SHOW CURRENT ROLES  |
| SET ROLE            |
| GRANT               |
| REVOKE              |
| DELETE              |
| EXPLAIN ANALYZE     |
| SHOW                |


Below Hive statements are partially supported, which mean some keywords or attributes are not supported:

| SQL                    | Description                                                  | openLooKeng Syntax Reference                     |
| ---------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| CREATE DATABASE/SCHEMA | statement with "COMMENT", "WITH DBPROPERTIES" is not supported | [CREATE SCHEMA](../sql/create-schema.html)         |
| DROP DATABASE/SCHEMA   | statement with  "CASCADE" is not supported                   | [DROP SCHEMA](../sql/drop-schema.html)             |
| SHOW DATABASE/SCHEMA   | statement with  "like" is not supported                      | [SHOW SCHEMA](../sql/show-schemas.html)            |
| CREATE TABLE           | statement with "SKEWED BY","ROW FORMAT" is not supported     | [CREATE TABLE](../sql/create-table.html)           |
| DROP TABLE             | statement with  "PURGE" is not supported                     | [DROP TABLE](../sql/drop-table.html)               |
| ALTER TABLE            | only "Rename table" and "Add a single column "are supported  | [ALTER TABLE](../sql/alter-table.html)             |
| SHOW CREATE TABLE      | To hive show table can works on both table and view. But in openLooKeng,  this can only be applied to table. | [SHOW CREATE TABLE](../sql/show-create-table.html) |
| DESCRIBE               | statement with column name is supported                      | [DESCRIBE](../sql/describe.html)                   |
| CREATE VIEW            | statement with   "COMMENT", "WITH DBPROPERTIES" is not supported | [CREATE VIEW](../sql/create-view.html)             |
| SHOW FUCNTIONS         | statement with "like" is not supported                       | [SHOW FUCNTIONS](../sql/show-functions.html)       |
| SHOW COLUMNS           | statement with  "like" is not supported                      | [SHOW COLUMNS](../sql/show-columns.html)           |
| SHOW GRANT             | Statement with Specified  user or role is not supported      | [SHOW GRANT](../sql/show-grants.html)              |
| INSERT                 | statement with "partition"  is not supported                 | [INSERT](../sql/insert.html)                       |
| SELECT                 | statement with  "cluster by",  "offset" is not supported     | [SELECT](../sql/select.html)                       |


Below Hive statements are not supported, because of feature differences:

| SQL                      |
| ------------------------ |
| ALTER DATABASE/SCHEMA    |
| DESCRIBE DATABASE/SCHEMA |
| SHOW TABLE EXTENDED      |
| SHOW TBLPROPERTIES       |
| TRUNCATE TABLE           |
| MSCK REPAIR TABLE        |
| ALTER PARTITION          |
| ALTER COLUMN             |
| ALTER VIEW               |
| SHOW VIEWS               |
| CREATE MATERIALIZED VIEW |
| DROP MATERIALIZED VIEW   |
| ALTER MATERIALIZED VIEW  |
| SHOW MATERIALIZED VIEWS  |
| CREATE FUNCTION          |
| DROP FUNCTION            |
| RELOAD FUNCTION          |
| CREATE INDEX             |
| DROP INDEX               |
| ALTER INDEX              |
| SHOW INDEX(ES)           |
| SHOW PARTITIONS          |
| Describe partition       |
| CREATE MACRO             |
| DROP MACRO               |
| SHOW ROLE GRANT          |
| SHOW PRINCIPALS          |
| SHOW LOCKS               |
| SHOW CONF                |
| SHOW TRANSACTIONS        |
| SHOW COMPACTIONS         |
| ABORT TRANSACTIONS       |
| LOAD                     |
| UPDATE                   |
| MERGE                    |
| EXPORT                   |
| IMPORT                   |
| EXPLAIN                  |
| SET                      |
| RESET                    |



## Usage of SQL Migration Tool

**Interactive mode**

This tool can be run in interactive mode. The example is like:

```shell
java -jar hetu-sql-migration-tool-010.jar
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



Here are some frequently used command:

| Command          | Description                          |
| ---------------- | ------------------------------------ |
| `exit` or `quit` | to exit the interactive mode         |
| `history`        | to get the previous input statements |
| `help`           | displace the help information        |



**Batch mode**

This tool also can take parameters and running in batch mode. It has five parameters, \"file\", \"sourceType\", \"execute\", \"output\" and "config\".  The meaning of each parameters lists as below:

| Parameter    | Description                                                  |
| ------------ | ------------------------------------------------------------ |
| `file`       | A file that contains SQL statements,  separated by \";\".  All of the SQLs in the file can be converted in batch process. |
| `sourceType` | The type of input SQL statement, such as `hive`, `impala`. It\'s optional parameter and the default value is `hive`. |
| `output`     | the directory to save the converted SQL results.             |
| `config`     | the config file of SQL Migration Tool.                       |

*Tip:*

*If user has large number of sql statements to convert, the suggested way is to consolidate all the statements into a single file, and use the batch mode.*

Here is an example of batch mode usage:

```shell
    java -jar hetu-sql-migration-tool-010.jar --file /home/Query01.sql --output ./
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Migration Completed.
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Result is saved to .//Query01_1590485230193.sql
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Result is saved to .//Query01_1590485230193.csv
```

When `file` is specified, parameter `output` must be provided. The converted result will be saved into two files in `output` directory:

1. The ".sql" file saves the successfully converted SQL statements.

2. The ".csv" file saves all the intermediate results of conversion, including original SQL, converted SQL, source SQL type, status and message.



**Execute mode**

It is possible to execute a query directly with the command and have the tool exit after transformation completion. Here is the example of using  `execute`:


```shell
java -jar hetu-sql-migration-tool-010.jar --execute "INSERT INTO TABLE T1 VALUES(10, 'openLooKeng')" --sourceType hive


==========converted result==========
INSERT INTO t1
 VALUES
  ROW (10, 'openLooKeng')

=================Success=============
```

If user specify the parameter `execute` only, the converted result will be printed onto the screen. Optionally, user can specify `output` parameter, the result will be saved into target file.

User can also provide `config` parameter to control the conversion behavior. Below is an example for `config`:

file name "config.properties" with content as below:

``` shell
convertDecimalLiteralsAsDouble=true


java -jar hetu-sql-migration-tool-010.jar --execute "INSERT INTO TABLE T1 select 2.0 * 3" --config config.properties


==========converted result==========
INSERT INTO t1
SELECT (DECIMAL '2.0' * 3)

=================Success=============
```

Currently, the config file only supports one property `convertDecimalLiteralsAsDouble`. It means whether to convert decimal literals as double or not. The default value is `false`,  which means converting decimal literals to type \"decimal\" . 



## Limitations

Converting the UDFs and functions in SQL statements are not supported.