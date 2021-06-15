# ClickHouse Connector

## Overview

The ClickHouse connector allows querying on an external ClickHouse database. This can be used to join data between different systems like ClickHouse and Hive, or between two different ClickHouse instances.

## Configuration

### Basic configuration

To configure the ClickHouse connector, create a catalog properties file in`etc/catalog` named, for example, `clickhouse.properties`, to mount the ClickHouse connector as the `clickhouse` catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup.

Base property setting: 

```
connector.name=clickhouse
connection-url=jdbc:clickhouse://example.net:8123
connection-user=username
connection-password=yourpassword
```

- Allow ClickHouse connector to drop table or not

```
allow-drop-table=true
```

- Enable the query push down feature or not

The push down feature of ClickHouse connector is turn on by default, and you can also set as below:

```
clickhouse.query.pushdown.enabled=true
```

- Table name is case sensitive or not.

The syntax of ClickHouse is case sensitive. If there are uppercase fields in your database table, you can set them as follows.

```
case-insensitive-name-matching=true
```

### Multiple ClickHouse Servers

You can have as many catalogs as you need, so if you have additional ClickHouse servers, simply add another properties file to `etc/catalog` with a different name (making sure it ends in `.properties`). For example, if
you name the property file `clickhouse2.properties`, openLooKeng will create a catalog named `clickhouse2` using the configured connector.

## Querying ClickHouse through openLooKeng

The ClickHouse connector provides a schema for every ClickHouse *database*. You can see the available ClickHouse databases by running `SHOW SCHEMAS`:

    SHOW SCHEMAS FROM clickhouse;

If you have a ClickHouse database named `data`, you can view the tables in this database by running `SHOW TABLES`:

    SHOW TABLES FROM clickhouse.data;

You can see a list of the columns in the `hello` table in the `data` database using either of the following:

    DESCRIBE clickhouse.data.hello;
    SHOW COLUMNS FROM clickhouse.data.hello;

Finally, you can also access the `hello` table in the `data` database:

    SELECT * FROM clickhouse.data.hello;

If you used a different name for your catalog properties file, use that catalog name instead of `clickhouse` in the above examples.

## Mapping Data Types Between openLooKeng and ClickHouse

### ClickHouse-to-openLooKeng Type Mapping

openLooKeng support selecting the following ClickHouse Detabase types. The table shows the mapping from ClickHouse data type.

Data type projection table:

| ClickHouse type       | openLooKeng type |
| :-------------------- | :--------------- |
| Int8                  | TINYINT          |
| Int16                 | SMALLINT         |
| Int32                 | INTEGER          |
| Int64                 | BIGINT           |
| float32               | REAL             |
| float64               | DOUBLE           |
| DECIMAL(P,S)          | DECIMAL(P,S)     |
| DECIMAL32(S)          | DECIMAL(P,S)     |
| DECIMAL64(S)          | DECIMAL(P,S)     |
| DECIMAL128(S)         | DECIMAL(P,S)     |
| String                | VARCHAR          |
| DateTime              | TIME             |
| Fixedstring(N)        | CHAR             |
| UInt8                 | SMALLINT         |
| UInt16                | INT              |
| UInt32                | BIGINT           |
| UInt64                | NA               |
| Int128,Int256,UInt256 | NA               |

### openLooKeng-to-ClickHouse Type Mapping

openLooKeng support creating tables with the following type into a ClickHouse Database. The table shows the mapping from openLooKeng to ClickHouse data types.

| openLooKeng type         | ClickHouse type |
| :----------------------- | :-------------- |
| BOOLEAN                  | Int8            |
| TINYINT                  | Int8            |
| SMALLINT                 | Int16           |
| INTEGER                  | Int32           |
| BIGINT                   | Int64           |
| REAL                     | float32         |
| DOUBLE                   | float64         |
| DECIMAL(P,S)             | DECIMAL(P,S)    |
| varchar                  | String          |
| varchar(n)               | String          |
| CHAR(n)                  | FixedString(n)  |
| VARBINARY                | String          |
| JSON                     | NA              |
| DATE                     | Date            |
| TIME                     | DateTime        |
| TIME WITH TIME ZONE      | NA              |
| TIMESTAMP                | TIMESTAMP       |
| TIMESTAMP WITH TIME ZONE | NA              |

### Functions that support pushdown

Note: The \"\$n\" is placeholder to present an argument in a function.

#### Aggregate Functions

```
count($1)
min($1)
max($1)
sum($1)
avg($1)
CORR($1,$2)
STDDEV($1)
stddev_pop($1)
stddev_samp($1)
skewness($1)
kurtosis($1)
VARIANCE($1)
var_samp($1)
```

#### Math functions

```
ABS($1)
ACOS($1)
ASIN($1)
ATAN($1)
ATAN2($1,$2)
CEIL($1)
CEILING($1)
COS($1)
e()
EXP($1)
FLOOR($1)
LN($1)
LOG10($1)
LOG2($1)
MOD($1,$2)
pi()
POW($1,$2)
POWER($1,$2)
RAND()
RANDOM()
ROUND($1)
ROUND($1,$2)
SIGN($1)
SIN($1)
SQRT($1)
TAN($1)
```

#### Functions for Working with Strings

```
CONCAT($1,$2)
LENGTH($1)
LOWER($1)
LTRIM($1)
REPLACE($1,$2)
REPLACE($1,$2,$3)
RTRIM($1)
STRPOS($1,$2)
SUBSTR($1,$2,$3)
POSITION($1,$2)
TRIM($1)
UPPER($1)
```

#### Functions for Working with Dates and Times

```
YEAR($1)
MONTH($1)
QUARTER($1)
WEEK($1)
DAY($1)
HOUR($1)
MINUTE($1)
SECOND($1)
DAY_OF_WEEK($1)
DAY_OF_MONTH($1)
DAY_OF_YEAR($1)
```

Note: The functions supported by openLooKeng can also be used in the ClickHouse connector, but functions not in the above list will not be pushed down.

## ClickHouse Connector Limitations

### Syntax

CREATE TABLE statement is not supported.

The INSERT statement needs to use CAST, for example, the data type in the table_name_test table is smallint:

```
insert into table_name_test values (cast(1 as small int));
```

The ClickHouse syntax supports the use of aliases in where clauses, but not in openLooKeng.

### Type

Types such as uuid in ClickHouse are not supported, and all supported types are listed in the mapping table.

