# DM(dameng) Connector

## Overview

The DM connector allows querying and creating tables in an external DM database. This can be used to join data between different systems like DM and Hive, or between two different DM instances.

## Configuration

Before using the DM connector, you should prepare:

- JDBC connection details for connecting to the DM database

To configure the DM connector, create a catalog properties file in`etc/catalog` named, for example, `dameng.properties`. Create the file with the following contents, replacing the connection properties as appropriate for your setup:

``` properties
connector.name=dameng
connection-url=jdbc:dm://host:port/SYSDBA
connection-user=username
connection-password=password
```

- Adding the DM driver

The DM JDBC driver is not provided in a common repository. If you are an DM database user, you can visit the official DM website, and download and deploy the DM JDBC driver to the repository on the condition that the license for the DM JDBC driver is complied with. The DM JDBC driver (**DmJdbcDriver***X*.**jar** where *X* is a number and varies according to the DM version) may be installed as a part of the DM client. After obtaining the DM JDBC driver, you can deploy the **DmJdbcDriver***X*.**jar** file to the openLooKeng plugin folder on the coordinator and worker. For example, if the JDBC driver file is **DmJdbcDriver***X*.**jar** and the openLooKeng plugin package folder is **/opt/hetu-server/plugin**, run the following command: **cp DmJdbcDriverX.jar /opt/hetu-server/plugin/dm**。 Restart the coordinator and worker. Then, the DM connector can work properly.

- Whether to enable the query pushdown function

The pushdown function of the DM connector is enabled by default, and you do not need to perform any operation. You can also set the parameter as follows:

``` properties
jdbc.pushdown-enabled=true
#true indicates that pushdown is enabled, and false indicates that pushdown is disabled.
```

- Mode for the push-down feature

If you want to enable the connector all push down feature for DM connector, you do not need to do any things for DM connector's push down feature, which is FULL_PUSHDOWN on by default. But you can also set as below:

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN  
#FULL_PUSHDOWN: All push down. BASE_PUSHDOWN: Partial push down, which indicates that filter, aggregation, limit, topN and project can be pushed down.
```

- More configurations

DM and Oracle are of the same origin, some configurations of Oracle connector are reused when implementing DM connector. Please refer to [ORACLE](./oracle.md).

## Querying DM Using openLooKeng

For the DM connector named **dameng**, each DM database user can run the **SHOW SCHEMAS** command to obtain the available schemas:

    SHOW SCHEMAS FROM dameng;

If you have obtained the available schemas, run the **SHOW TABLES** command to view the tables owned by the DM database named **data**:

    SHOW TABLES FROM dameng.data;

To view a list of columns in a table named **hello** in data schema, run either of the following commands:

    DESCRIBE dameng.data.hello;
    SHOW COLUMNS FROM dameng.data.hello;

You can access the **hello** table in the **data** schema:

    SELECT * FROM dameng.data.hello;

The connector's permissions in these schemas are your permissions configured in the connection property file. If you cannot access the tables, a specific connector cannot access them.

## DM Update/Delete Support

### Create DM Table

Example：

```sql
CREATE TABLE dameng_table (
    id int,
    name varchar(255));
```

### INSERT on DM tables

Example：

```sql
INSERT INTO dameng_table
  VALUES
     (1, 'foo'),
     (2, 'bar');
```

### UPDATE on DM tables

Example：

```sql
UPDATE dameng_table
  SET name='john'
  WHERE id=2;
```

Above example updates the column `name`'s value to `john` of row with column `id` having value `2`.

SELECT result before UPDATE:

```sql
lk:default> SELECT * FROM dameng_table;
id | name
----+------
  2 | bar
  1 | foo
(2 rows)
```

SELECT result after UPDATE

```sql
lk:default> SELECT * FROM dameng_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

### DELETE on DM tables

Example：

```sql
DELETE FROM dameng_table
  WHERE id=2;
```

Above example delete the row with column `id` having value `2`.

SELECT result before DELETE:

```sql
lk:default> SELECT * FROM dameng_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

SELECT result after DELETE:

```sql
lk:default> SELECT * FROM dameng_table;
 id | name
----+------
  1 | foo
(1 row)
```

## Restrictions on the DM Connector

- The openLooKeng can connect to DM Database 8.

- The DM Connector does not support query pushdown for DM Update yet.