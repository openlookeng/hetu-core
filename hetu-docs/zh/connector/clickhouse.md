
# ClickHouse连接器

## 概述

Clickhouse连接器允许在外部ClickHouse数据库中查询和创建表。这可用于在ClickHouse和Hive等不同系统之间或在两个不同的ClickHouse实例之间联接数据。

## 配置

### 基础配置

要配置ClickHouse连接器，在`etc/catalog`中创建一个目录属性文件，例如`clickhouse.properties`，将ClickHouse连接器挂载为`clickhouse`目录。使用以下内容创建文件，并根据设置替换连接属性。

基本属性设置：

```
connector.name=clickhouse
connection-url=jdbc:clickhouse://example.net:8123
connection-user=username
connection-password=yourpassword
```

- 是否允许连接器删除表

```
allow-drop-table=true
```

- 是否开启查询下推功能。

连接器的下推功能默认打开，也可以如下设置：

```
clickhouse.query.pushdown.enabled=true
```

- 是否区分表名大小写

与openLooKeng不同，ClickHouse的语法是大小写敏感的，如果您的数据库表中存在大写字段，可以按如下设置。

```
case-insensitive-name-matching=true
```

### 多套ClickHouse数据库或服务器

可以根据需要创建任意多的目录，因此，如果有额外的ClickHouse服务器，只需添加另一个不同的名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`clickhouse2.properties`，openLooKeng将使用配置的连接器创建一个名为`clickhouse2`的目录。

## 通过openLooKeng查询ClickHouse

通过`SHOW SCHEMAS`来查看可用的ClickHouse数据库：

    SHOW SCHEMAS FROM clickhouse;

如果有一个名为`data`的ClickHouse数据库，可以通过执行`SHOW TABLES`查看数据库中的表：

    SHOW TABLES FROM clickhouse.data;

若要查看数据模式中名为**hello**的表中的列的列表，请使用以下命令中的一种：

    DESCRIBE clickhouse.data.hello;
    SHOW COLUMNS FROM clickhouse.data.hello;

你可以访问数据模式中的**hello**表：

    SELECT * FROM clickhouse.data.hello;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`clickhouse`。

## openLooKeng和ClickHouse之间的映射数据类型

### ClickHouse到openLooKeng类型映射

下表显示了ClickHouse数据类型到openLooKeng的的映射关系。

数据类型投影表：

| ClickHouse类型        | openLooKeng类型 | 说明 |
| :-------------------- | :-------------- | :--- |
| Int8                  | TINYINT         |      |
| Int16                 | SMALLINT        |      |
| Int32                 | INTEGER         |      |
| Int64                 | BIGINT          |      |
| float32               | REAL            |      |
| float64               | DOUBLE          |      |
| DECIMAL(P,S)          | DECIMAL(P,S)    |      |
| DECIMAL32(S)          | DECIMAL(P,S)    |      |
| DECIMAL64(S)          | DECIMAL(P,S)    |      |
| DECIMAL128(S)         | DECIMAL(P,S)    |      |
| String                | VARCHAR         |      |
| DateTime              | TIMESTAMP       |      |
| Fixedstring(N)        | CHAR            |      |
| UInt8                 | SMALLINT        |      |
| UInt16                | INT             |      |
| UInt32                | BIGINT          |      |
| UInt64                | BIGINT          |      |
| Int128,Int256,UInt256 | 不涉及          |      |

### openLooKeng到ClickHouse类型映射

下表显示了从openLooKeng到ClickHouse数据类型的映射关系。

| openLooKeng类型          | ClickHouse数据库类型 | 说明 |
| :----------------------- | :------------------- | :--- |
| BOOLEAN                  | Int8                 |      |
| TINYINT                  | Int8                 |      |
| SMALLINT                 | Int16                |      |
| INTEGER                  | Int32                |      |
| BIGINT                   | Int64                |      |
| REAL                     | float32              |      |
| DOUBLE                   | float64              |      |
| DECIMAL(P,S)             | DECIMAL(P,S)         |      |
| varchar                  | String               |      |
| varchar(n)               | String               |      |
| CHAR(n)                  | FixedString(n)       |      |
| VARBINARY                | String               |      |
| JSON                     | 不涉及               |      |
| DATE                     | DATE                 |      |
| TIME                     | 不涉及               |      |
| TIME WITH TIME ZONE      | 不涉及               |      |
| TIMESTAMP                | DateTime             |      |
| TIMESTAMP WITH TIME ZONE | 不涉及               |      |

### openLooKeng到ClickHouse函数映射

下面列举ClickHouse插件支持下推的函数。说明：“$n”是占位符，用于在函数中表示参数。

#### 聚合统计函数

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

#### 数学函数

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

#### 字符串函数

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

#### 时间日期函数

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

## ClickHouse连接器的限制

### 语法

暂不支持CREATE TABLE操作。

INSERT语句需要使用CAST强制转换，例如table_name_test表中的数据类型为smallint：

```
insert into table_name_test values (cast(1 as small int));
```

由于查询语句的执行顺序不同，ClickHouse支持 as 表达式的别名在where中使用，但在openLooKeng中不允许。

### 类型限制

暂不支持ClickHouse中的uuid等类型，所有支持类型已在映射表中列出。
