# Oracle Connector

## Overview

The Oracle connector allows querying and creating tables in an external Oracle database. It can be used to join data between different databases, such as Oracle and Hive, or two different Oracle instances.

## Configuration

### Basic Configuration

Before using the Oracle connector, you should prepare:

- JDBC connection details for connecting to the Oracle database

The details should be written in a regular openLooKeng connector configuration (for example, the openLooKeng catalog named **oracle** uses **oracle.properties**). The file must contain the following content and replace the connection properties based on the settings:

Basic properties:

``` properties
connector.name=oracle
connection-url=jdbc:oracle:thin:@host:port/ORCLCDB
connection-user=username
connection-password=password
```

- Adding the Oracle driver

The Oracle JDBC driver is not provided in a common repository. If you are an Oracle database user, you can visit the official Oracle website, and download and deploy the Oracle JDBC driver to the repository on the condition that the license for the Oracle JDBC driver is complied with. The Oracle JDBC driver (**ojdbc***X*.**jar** where *X* is a number and varies according to the Oracle version) may be installed as a part of the Oracle client or downloaded from the official Oracle website. After obtaining the Oracle JDBC driver, you can deploy the **jdbc.jar** file to the openLooKeng plugin folder on the coordinator and worker. For example, if the JDBC driver file is **ojdbc***X***.jar** and the openLooKeng plugin package folder is **/usr/lib/presto/lib/plugin**, run the following command: **cp ojdbcX.jar /usr/lib/presto/lib/plugin/oracle**。 Restart the coordinator and worker. Then, the Oracle connector can work properly.

- Whether to enable the query pushdown function

The pushdown function of the Oracle connector is enabled by default, and you do not need to perform any operation. You can also set the parameter as follows:

``` properties
jdbc.pushdown-enabled=true
#true indicates that pushdown is enabled, and false indicates that pushdown is disabled.
```

- Mode for the push-down feature

If you want to enable the connector all push down feature for oracle connector, you do not need to do any things for oracle connector's push down feature, which is FULL_PUSHDOWN on by default. But you can also set as below:

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN  
#FULL_PUSHDOWN: All push down. BASE_PUSHDOWN: Partial push down, which indicates that filter, aggregation, limit, topN and project can be pushed down.
```

### Multiple Oracle Databases or Servers

If you want to connect to multiple Oracle databases, configure another instance of the Oracle plugin as a separate catalog. To add another Oracle catalog, create a new property file with a different name (the file name extension is .properties) in **../conf/catalog**. For example, if a file named **oracle2.properties** is created in **../conf/catalog**, add a connector named **oracle2**.

## Querying Oracle Using openLooKeng

For the Oracle connector named **oracle**, each Oracle database user can run the **SHOW SCHEMAS** command to obtain the available schemas:

    SHOW SCHEMAS FROM oracle;

If you have obtained the available schemas, run the **SHOW TABLES** command to view the tables owned by the Oracle database named **data**:

    SHOW TABLES FROM oracle.data;

To view a list of columns in a table named **hello** in data schema, run either of the following commands:

    DESCRIBE oracle.data.hello;
    SHOW COLUMNS FROM oracle.data.hello;

You can access the **hello** table in the **data** schema:

    SELECT * FROM oracle.data.hello;

The connector's permissions in these schemas are your permissions configured in the connection property file. If you cannot access the tables, a specific connector cannot access them.

## Oracle Update/Delete Support

### Create Oracle Table

Example：

```sql
CREATE TABLE oracle_table (
    id int,
    name varchar(255));
```

### INSERT on Oracle tables

Example：

```sql
INSERT INTO oracle_table
  VALUES
     (1, 'foo'),
     (2, 'bar');
```

### UPDATE on Oracle tables

Example：

```sql
UPDATE oracle_table
  SET name='john'
  WHERE id=2;
```

Above example updates the column ` name`'s value to `john` of row with column `id` having value `2`.

SELECT result before UPDATE:

```sql
lk:default> SELECT * FROM oracle_table;
id | name
----+------
  2 | bar
  1 | foo
(2 rows)
```

SELECT result after UPDATE

```sql
lk:default> SELECT * FROM oracle_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

### DELETE on Oracle tables

Example：

```sql
DELETE FROM oracle_table
  WHERE id=2;
```

Above example delete the row with column `id` having value `2`.

SELECT result before DELETE:

```sql
lk:default> SELECT * FROM oracle_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

SELECT result after DELETE:

```sql
lk:default> SELECT * FROM oracle_table;
 id | name
----+------
  1 | foo
(1 row)
```

## Mapping Data Types Between openLooKeng and Oracle

**Type-related configuration items**

| Configuration Item| Description| Default Value|
|----------|----------|----------|
| unsupported-type.handling-strategy| Specifies how to handle an unsupported column data type:<br />`FAIL` - report an error.<br />`IGNORE` \- The column cannot be accessed.<br /> `CONVERT_TO_VARCHAR` - Convert the column to a unbounded `VARCHAR`.| `FAIL`|
| oracle.number.default-scale| If the precision and number of decimal places are not specified for the **number** data type in the Oracle database, the **number** data type is converted to an openLooKeng data type. The precision is converted based on this item.| 0|
| oracle.number.rounding-mode| Rounding mode of the Oracle `NUMBER` data type. This item is useful when the size specified by the Oracle `NUMBER` data type is greater than that supported by Presto. The possible values are as follows:<br /> `UNNECESSARY` - Rounding mode, to assert that the requested operation has an accurate result, so no rounding is required.<br /> `CEILING` - Round to positive infinity. <br />`FLOOR` - Round to negative infinity. <br />`HALF_DOWN` - Rounds to the nearest neighbor. If the distances of two neighbors are the same, round down to the nearest neighbor.<br />`HALF_EVEN` - If the distances of two neighbors are the same, round down to the even neighbor.<br />`HALF_UP` \- Round to the nearest neighbor. If the distances of two neighbors are the same, round up to the nearest neighbor. <br />`UP` - Round up to zero.<br /> `DOWN` - Round down to zero.| `UNNECESSARY`|

### Type Mapping From Oracle to openLooKeng

The openLooKeng supports selecting the following Oracle database types. The following table lists the type mapping from Oracle to openLooKeng.

Data type mapping

> | Oracle Database Type| openLooKeng Type| Description|
> |:----------|:----------|:----------|
> | DECIMAL(p, s)| DECIMAL(p, s)|
> | NUMBER(p)| DECIMAL(p, 0)|
> | FLOAT(p)| DOUBLE|
> | BINARY\_FLOAT| REAL|
> | BINARY\_DOUBLE| DOUBLE|
> | VARCHAR2(n CHAR)| VARCHAR(n)|
> | VARCHAR2(n BYTE)| VARCHAR(n)|
> | CHAR(n)| CHAR(n)|
> | NCHAR(n)| CHAR(n)|
> | CLOB| VARCHAR|
> | NCLOB| VARCHAR|
> | RAW(n)| VARCHAR|
> | BLOB| VARBINARY|
> | DATE| TIMESTAMP|
> | TIMESTAMP(p)| TIMESTAMP|
> | TIMESTAMP(p) WITH TIME ZONE| TIMESTAMP WITH TIME ZONE|



### Type Mapping From openLooKeng to Oracle

The openLooKeng supports creating tables with the following types in an Oracle database. The following table shows the mappings from openLooKeng to Oracle data types.

Data type mapping

> | openLooKeng Type| Oracle Database Type| Description|
> |:----------|:----------|:----------|
> | TINYINT| NUMBER(3)|
> | SMALLINT| NUMBER(5)|
> | INTEGER| NUMBER(10)|
> | BIGINT| NUMBER(19)|
> | DECIMAL(p, s)| NUMBER(p, s)|
> | REAL| BINARY\_FLOAT|
> | DOUBLE| BINARY\_DOUBLE|
> | VARCHAR| NCLOB|
> | VARCHAR(n)| VARCHAR2(n CHAR) or NCLOB|
> | CHAR(n)| CHAR(n CHAR) or NCLOB|
> | VARBINARY| BLOB|
> | DATE| DATE|
> | TIMESTAMP| TIMESTAMP(3)|
> | TIMESTAMP WITH TIME ZONE| TIMESTAMP(3) WITH TIME ZONE|



### Common Functions Between openLooKeng and Oracle

There are some common functions between the openLooKeng and Oracle databases. For details about the functions, see the function and operator documents in the openLooKeng and the official Oracle website. The following table lists the common functions.

> | Function| Description|
> |----------|----------|
> | abs| Mathematical function|
> | acos| Mathematical function|
> | asin| Mathematical function|
> | atan| Mathematical function|
> | ceil| Mathematical function|
> | cos| Mathematical function|
> | cosh| Mathematical function|
> | exp| Mathematical function|
> | floor| Mathematical function|
> | ln| Mathematical function|
> | round| Mathematical function|
> | sign| Mathematical function|
> | sin| Mathematical function|
> | sqrt| Mathematical function|
> | tan| Mathematical function|
> | tanh| Mathematical function|
> | mod| Mathematical function|
> | concat| String function|
> | greatest| String function|
> | least| String function|
> | length| String function|
> | lower| String function|
> | ltrim| String function|
> | replace| String function|
> | rpad| String function|
> | rtrim| String function|
> | trim| String function|
> | upper| String function|
> | reverse| String function|
> | regexp\_like| String function|
> | regexp\_replace| String function|
> | avg| Aggregation function|
> | count| Aggregation function|
> | max| Aggregation function|
> | min| Aggregation function|
> | stddev| Aggregation function|
> | sum| Aggregation function|
> | variance| Aggregation function|



## Syntax Differences Between Oracle SQL and OpenLooKeng SQL

openLooKeng supports the standard SQL:2003 syntax, which is different from the Oracle SQL syntax. To run Oracle SQL statements in the openLooKeng, you need to perform equivalent syntax conversion on the SQL statements. For details about the Oracle and openLooKeng SQL syntax, see the official documents.

## Support for Oracle Synonyms

To ensure performance, the openLooKeng disables the Oracle `SYNONYM` function by default. You can enable this function by the following configuration:

```
oracle.synonyms.enabled=true
```

## Restrictions on the Oracle Connector

- The openLooKeng can connect to Oracle Database 11g and Oracle Database 12c.

- The Oracle Connector does not support query pushdown for Oracle Update yet.

### Oracle Number Type

The precision of the **number** type in the Oracle database is variable, while the openLooKeng does not support variable precision. For this reason, if the precision is not specified when an Oracle table uses the **number** type, the openLooKeng converts the **number** type of the Oracle database to the **decimal** type of the openLooKeng. As a result, some precision loss occurs in a particular value range.