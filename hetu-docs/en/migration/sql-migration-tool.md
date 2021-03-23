
# SQL Migration Tool

SQL Migration tool helps user to transform SQL Syntax to ANSI 2003 SQL syntax. Currently, only Hive and Impala SQL syntax are supported.

## Usage of SQL Migration Tool
Download `hetu-sql-migration-cli-{version number}-executable.jar`, rename it to `openlk-sql-migration-cli`, make
it executable with `chmod +x`, then run it.

**Interactive mode**

This tool can be run in interactive mode. The example is like:

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

| Parameter    | Description                                                  |
| ------------ | ------------------------------------------------------------ |
| `--type` or `-t` | The type of input SQL statement, such as `hive`, `impala`. It\'s optional parameter and the default value is `hive`. |


Here are some frequently used command:

| Command          | Description                          |
| ---------------- | ------------------------------------ |
| `!chtype value;` | change the source sql type of current session          |
| `exit` or `quit` | to exit the interactive mode         |
| `history`        | to get the previous input statements |
| `help`           | displace the help information        |



**Batch mode**

This tool also can take parameters and running in batch mode. It has five parameters, \"file\", \"sourceType\", \"execute\", \"output\" and \"config\".  The meaning of each parameters lists as below:

| Parameter    | Description                                                  |
| ------------ | ------------------------------------------------------------ |
| `--file` or `-f`       | A file that contains SQL statements,  separated by \";\".  All of the SQLs in the file can be converted in batch process. |
| `--type` or `-t` | The type of input SQL statement, such as `hive`, `impala`. It\'s optional parameter and the default value is `hive`. |
| `--output` or `-o`     | the directory to save the converted SQL results. The result file's naming convention will be the input file's name + timestamp + .html suffix.             |
| `--config` or `-c`     | the config file of SQL Migration Tool.                       |
| `--debug` or `-d`     | if set value to 'true', then print the debug information in console.                       |

*Tip:*

*If user has large number of sql statements to convert, the suggested way is to consolidate all the statements into a single file, and use the batch mode.*

Here is an example of batch mode usage:

```shell
    ./openlk-sql-migration-cli --file /home/Query01.sql --output ./
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Migration Completed.
    May 26, 2020 5:27:10 PM io.airlift.log.Logger info
    INFO: Result is saved to .//Query01_1590485230193.html
```

When `file` is specified, parameter `output` must be provided. The converted result will be a html file in `output` directory. 
You can open that html file via any web browser, and then review the conversion details. 


**Execute mode**

It is possible to execute a query directly with the command and have the tool exit after transformation completion. Here is the example of using  `execute`:


```shell
./openlk-sql-migration-cli --execute "INSERT INTO TABLE T1 VALUES(10, 'openLooKeng')" --type hive


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


./openlk-sql-migration-cli --execute "INSERT INTO TABLE T1 select 2.0 * 3" --config config.properties


==========converted result==========
INSERT INTO t1
SELECT (DECIMAL '2.0' * 3)

=================Success=============
```

Currently, the config file only supports one property `convertDecimalLiteralsAsDouble`. It means whether to convert decimal literals as double or not. The default value is `false`,  which means converting decimal literals to type \"decimal\" . 


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
| SHOW FUCNTIONS      |


Below Hive statements are partially supported, which mean some keywords or attributes are not supported:

| SQL                    | Description                                                  | openLooKeng Syntax Reference                     |
| ---------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| CREATE DATABASE/SCHEMA | statement with "COMMENT", "WITH DBPROPERTIES" is not supported | [CREATE SCHEMA](../sql/create-schema.md)         |
| DROP DATABASE/SCHEMA   | statement with  "CASCADE" is not supported                   | [DROP SCHEMA](../sql/drop-schema.md)             |
| SHOW DATABASE/SCHEMA   | statement with  "like" is not supported                      | [SHOW SCHEMA](../sql/show-schemas.md)            |
| CREATE TABLE           | statement with "SKEWED BY","ROW FORMAT" is not supported     | [CREATE TABLE](../sql/create-table.md)           |
| DROP TABLE             | statement with  "PURGE" is not supported                     | [DROP TABLE](../sql/drop-table.md)               |
| ALTER TABLE            | only "Rename table" and "Add a single column "are supported  | [ALTER TABLE](../sql/alter-table.md)             |
| SHOW CREATE TABLE      | To hive show table can works on both table and view. But in openLooKeng,  this can only be applied to table. | [SHOW CREATE TABLE](../sql/show-create-table.md) |
| DESCRIBE               | statement with column name is supported                      | [DESCRIBE](../sql/describe.md)                   |
| CREATE VIEW            | statement with   "COMMENT", "WITH DBPROPERTIES" is not supported | [CREATE VIEW](../sql/create-view.md)             |
| SHOW COLUMNS           | statement with  "like" is not supported                      | [SHOW COLUMNS](../sql/show-columns.md)           |
| SHOW GRANT             | Statement with Specified  user or role is not supported      | [SHOW GRANT](../sql/show-grants.md)              |
| INSERT                 | statement with "partition"  is not supported                 | [INSERT](../sql/insert.md)                       |
| SELECT                 | statement with  "cluster by",  "offset" is not supported     | [SELECT](../sql/select.md)                       |


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

## Check list of  Impala Statements:

Below Impala statements are fully supported:

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

Below Impala statements are partially supported, which mean some keywords or attributes are not supported:

| SQL                    | Description                                                  | openLooKeng Syntax Reference                     |
| ---------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| DROP SCHEMA            | statement with "CASCADE" is not supported                   | [DROP SCHEMA](../sql/drop-schema.md)             |
| CREATE TABLE           | statement with "ROW FORMAT", "WITH SERDEPROPERTIES", "CACHED IN" is not supported     | [CREATE TABLE](../sql/create-table.md)           |
| CREATE TABLE LIKE      | statement with "PARQUET" is not supported     | [CREATE TABLE](../sql/create-table.md)           |
| DROP TABLE             | statement with "PURGE" is not supported                     | [DROP TABLE](../sql/drop-table.md)               |
| CREATE VIEW            | statement with "IF NOT EXISTS", "ALIAS" is not supported | [CREATE VIEW](../sql/create-view.md)             |
| ALTER VIEW             | Alias is not supported, and it will be converted to "CREATE OR REPLACE VIEW"  | [ALTER TABLE](../sql/create-view.md)             |
| DESCRIBE               | Only table is supported to use describe                      | [DESCRIBE](../sql/describe.md)                   |
| GRANT ROLE             | Granting role to Group is not supported                      | [GRANT ROLES](../sql/grant-roles.md)                       |
| GRANT                  | Only "SELECT","INSERT" privileges are supported, and only ROLE can be granted to                      | [GRANT](../sql/grant.md)                       |
| REVOKE ROLE            | Revoking role from Group is not supported                    | [REVOKE ROLES](../sql/revoke-roles.md)                       |
| REVOKE                 | Only "SELECT","INSERT" privileges are supported, and only ROLE can be revoked from                    | [REVOKE](../sql/revoke.md)                       |
| INSERT INTO            | statement with "WITH", "HINT", "PARTITION" is not supported                    | [INSERT INTO](../sql/insert.md)                       |
| DELETE                 | statement with "JOIN" is not supported                    | [DELETE](../sql/delete.md)                       |
| SHOW SCHEMAS           | statement with more than one wildcard is not supported                    | [DELETE](../sql/show-schemas.md)                       |
| SHOW TABLES            | statement with more than one wildcard is not supported                    | [DELETE](../sql/show-tables.md)                       |
| ADD COMMENTS           | Adding comments to databases or columns is not supported                  | [COMMENT](../sql/comment.md)                       |
| SET SESSION            | Only support "SET" and "SET ALL"                                          | [SET SESSION](../sql/set-session.md)                       |
| ADD COLUMNS            | ADD multiple columns within single statement is not supported, kudu properties are not supported.    | [ALTER TABLE](../sql/alter-table.md)                       |
| SHOW FUNCTIONS         | Only support show all functions or statement with "LIKE".                          | [SHOW FUNCTIONS](../sql/show-functions.md)                       |


Below Impala statements are not supported, because of feature differences:

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




## Limitations

Converting the UDFs and functions in SQL statements are not supported.