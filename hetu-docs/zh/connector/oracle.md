
# Oracle连接器

## 概述

Oracle连接器允许在外部Oracle数据库中查询和创建表。这可用于在Oracle和Hive等不同系统之间或在两个不同的Oracle实例之间联接数据。

## 配置

### 基础配置

首先，在开始使用oracle连接器之前，应该先完成以下步骤：

- 用于连接Oracle的JDBC连接详情

应该以常规openLooKeng连接器配置编写（例如名为**oracle**的openLooKeng目录使用**oracle.properties**）。文件应包含以下内容，并根据设置替换连接属性： 

基本属性设置：

``` properties
connector.name=oracle
connection-url=jdbc:oracle:thin:@主机:端口/ORCLCDB
connection-user=用户名
connection-password=密码
```

- 添加Oracle驱动

Oracle JDBC驱动不在普通存储库中提供，如果您是oracle数据库的用户，您可以选择前往Oracle官方网站，在确保遵守Oracle JDBC驱动所适用的license的条件下，下载和安装Oracle JDBC驱动到存储库中。Oracle JDBC驱动（ojdbc***X***.jar ***X***为数字，根据Oracle的版本不同而不同）可能会作为Oracle客户端安装的一部分进行安装，或者从Oracle办公网站下载。获取了Oracle  JDBC驱动后，您可以将**jdbc jar**文件部署到协调节点和工作节点上的openLooKeng插件文件夹中。例如，jdbc驱动文件为**ojdbcX.jar**，openLooKeng插件包文件夹为 **/usr/lib/presto/lib/plugin**，则拷贝命令如下： **cp ojdbcX.jar /usr/lib/presto/lib/plugin/oracle**。重启协调节点和工作节点进程，oracle连接器即可正常工作。

- 是否开启查询下推功能

如果要启用oracle连接器的连接器下推功能，不需要做任何操作，oracle连接器的下推功能默认是打开的。但也可以按如下设置：

``` properties
jdbc.pushdown-enabled=true
#true表示打开下推，false表示关闭。
```

- 下推模式选择。

如果要启用oracle连接器的全部下推功能，不需要做任何操作，oracle连接器的下推模式默认是全部下推的。但也可以按如下设置：

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN
#FULL_PUSHDOWN，表示全部下推；BASE_PUSHDOWN，表示部分下推，其中部分下推是指filter/aggregation/limit/topN/project这些可以下推。
```

### 多套Oracle数据库或服务器

如果要连接到多个Oracle数据库，请将Oracle插件的另一个实例配置为一个单独的目录。如需添加其他Oracle目录，请在 **../conf/catalog** 下添加不同名称的另一属性文件（注意结尾为 **.properties** ）。例如，在 **../conf/catalog** 目录下新增一个名称为 **oracle2.properties** 的文件，则新增一个名称为oracle2的连接器。

## 通过openLooKeng查询Oracle

对于名为**oracle**的oracle连接器，每个Oracle数据库的用户都可以通过oracle连接器获取其可用的模式，命令为**SHOW SCHEMAS**：

    SHOW SCHEMAS FROM oracle;

如果已经拥有了可用模式，可以通过**SHOW TABLES**命令查看名为**data**的Oracle数据库拥有的表：

    SHOW TABLES FROM oracle.data;

若要查看数据模式中名为**hello**的表中的列的列表，请使用以下命令中的一种：

    DESCRIBE oracle.data.hello;
    SHOW COLUMNS FROM oracle.data.hello;

你可以访问数据模式中的**hello**表：

    SELECT * FROM oracle.data.hello;

连接器在这些模式中的权限是在连接属性文件中配置的用户的权限。如果用户无法访问这些表，则特定的连接器将无法访问这些表。

## Oracle Update/Delete 支持

### 使用Oracle连接器创建表

示例：

```sql
CREATE TABLE oracle_table (
    id int,
    name varchar(255));
```

### 对表执行INSERT

示例：

```sql
INSERT INTO oracle_table
  VALUES
     (1, 'foo'),
     (2, 'bar');
```

### 对表执行UPDATE

示例：

```sql
UPDATE oracle_table
  SET name='john'
  WHERE id=2;
```

上述示例将值为`2`的列`id`的行的列`name`的值更新为`john`。

UPDATE前的SELECT结果：

```sql
lk:default> SELECT * FROM oracle_table;
id | name
----+------
  2 | bar
  1 | foo
(2 rows)
```

UPDATE后的SELECT结果

```sql
lk:default> SELECT * FROM oracle_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

### 对表执行DELETE

示例：

```sql
DELETE FROM oracle_table
  WHERE id=2;
```

以上示例删除了值为`2`的列`id`的行。

DELETE前的SELECT结果：

```sql
lk:default> SELECT * FROM oracle_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

DELETE后的SELECT结果：

```sql
lk:default> SELECT * FROM oracle_table;
 id | name
----+------
  1 | foo
(1 row)
```

## openLooKeng和Oracle之间的数据类型映射

**类型相关配置说明**

| 配置项                             | 描述                                                         | 默认值        |
| ---------------------------------- | ------------------------------------------------------------ | ------------- |
| unsupported-type.handling-strategy | 配置如何处理不受支持的列数据类型:<br />`FAIL`-直接报错。<br />`IGNORE` -无法访问列。<br /> `CONVERT_TO_VARCHAR`-列转换为无界`VARCHAR`。 | `FAIL`        |
| oracle.number.default-scale        | Oracle的number数据类型未指定精度和小数位数时，转换为openLooKeng数据类型，会根据该配置进行精度转换。 | 0             |
| oracle.number.rounding-mode        | Oracle`NUMBER`数据类型的舍入模式。当Oracle`NUMBER`数据类型指定的规模大于Presto支持的规模时，此功能很有用。可能的值为：<br /> `UNNECESSARY` -舍入模式，以断言所请求的操作具有精确的结果，因此不需要舍入。<br /> `CEILING` -向正无穷大舍入。 <br />`FLOOR` -向负无穷大舍入。 <br />`HALF_DOWN` -向最近的邻居舍入，若是两个邻居距离相等，则向下舍入模式<br />`HALF_EVEN`-向最近的邻居舍入，若是两个邻居距离相等，则向偶数邻居舍入。<br />`HALF_UP`-向最近的邻居舍入，若是两个邻居距离相等，则向上舍入。 <br />`UP` -舍入模式向零舍入。<br /> `DOWN` -舍入模式向零舍入。 | `UNNECESSARY` |

### Oracle到openLooKeng类型映射

openLooKeng支持选择以下Oracle数据库类型。下表显示了Oracle数据类型的映射关系。

数据类型映射表

> | Oracle数据库类型 | openLooKeng类型| 说明|
> |:----------|:----------|:----------|
> | DECIMAL(p, s)| DECIMAL(p, s)| |
> | NUMBER(p)                   | DECIMAL(p, 0)            |  |
> | FLOAT(p)                    | DOUBLE                   |      |
> | BINARY_FLOAT                | REAL                     |      |
> | BINARY_DOUBLE               | DOUBLE                   |      |
> | VARCHAR2(n CHAR)            | VARCHAR(n)               |      |
> | VARCHAR2(n BYTE)            | VARCHAR(n)               |      |
> | CHAR(n)                     | CHAR(n)                  |      |
> | NCHAR(n)                    | CHAR(n)                  |      |
> | CLOB                        | VARCHAR                  |      |
> | NCLOB                       | VARCHAR                  |      |
> | RAW(n)                      | VARCHAR                  |      |
> | BLOB                        | VARBINARY                |      |
> | DATE                        | TIMESTAMP                |      |
> | TIMESTAMP(p)                | TIMESTAMP                |      |
> | TIMESTAMP(p) WITH TIME ZONE | TIMESTAMP WITH TIME ZONE |      |

### openLooKeng到Oracle类型映射

openLooKeng支持在Oracle数据库中创建以下类型的表。下表显示了从openLooKeng到Oracle数据类型的映射关系。

数据类型映射表

> | openLooKeng类型| Oracle数据库类型 | 说明|
> |:----------|:----------|:----------|
> | TINYINT                  | NUMBER(3)                   | |
> | SMALLINT                 | NUMBER(5)                   |      |
> | INTEGER                  | NUMBER(10)                  |      |
> | BIGINT                   | NUMBER(19)                  |      |
> | DECIMAL(p, s)            | NUMBER(p, s)                |      |
> | REAL                     | BINARY_FLOAT                |      |
> | DOUBLE                   | BINARY_DOUBLE               |      |
> | VARCHAR                  | NCLOB                       |      |
> | VARCHAR(n)               | VARCHAR2(n CHAR) or NCLOB   |      |
> | CHAR(n)                  | CHAR(n CHAR) or NCLOB       |      |
> | VARBINARY                | BLOB                        |      |
> | DATE                     | DATE                        |      |
> | TIMESTAMP                | TIMESTAMP(3)                |      |
> | TIMESTAMP WITH TIME ZONE | TIMESTAMP(3) WITH TIME ZONE |      |



### openLooKeng与Oracle之间的常用共有函数

openLooKeng与Oracle之间存在一些常用的共有函数。具体函数功能请参考openLooKeng中函数和运算符部分的文档和Oracle官网。下表列出了这些常用共有函数。

> | 函数名称 |说明|
> |----------|----------|
> |   abs   | 数学函数 |
> | acos   | 数学函数 |
> | asin   | 数学函数 |
> |  atan  | 数学函数 |
> |  ceil  | 数学函数 |
> |  cos  | 数学函数 |
> |  cosh  | 数学函数 |
> |  exp  | 数学函数 |
> |  floor   | 数学函数 |
> |  ln  | 数学函数 |
> | round   | 数学函数 |
> |  sign  | 数学函数 |
> |  sin  | 数学函数 |
> |  sqrt  | 数学函数 |
> |   tan | 数学函数 |
> |  tanh  | 数学函数 |
> |  mod  | 数学函数 |
> | concat   | 字符串函数 |
> | greatest   | 字符串函数 |
> | least   | 字符串函数 |
> | length   | 字符串函数 |
> |  lower  | 字符串函数 |
> |  ltrim  | 字符串函数 |
> |  replace  | 字符串函数 |
> |  rpad  | 字符串函数 |
> |  rtrim  | 字符串函数 |
> |  trim  | 字符串函数 |
> |  upper  | 字符串函数 |
> |  reverse  | 字符串函数 |
> | regexp_like   | 字符串函数 |
> |  regexp_replace  | 字符串函数 |
> | avg   | 聚合函数 |
> |  count  | 聚合函数 |
> |  max  | 聚合函数 |
> | min   | 聚合函数 |
> |  stddev  | 聚合函数 |
> |  sum  | 聚合函数 |
> |  variance  | 聚合函数 |


## Oracle SQL与openLooKeng SQL语法差异

openLooKeng支持标准的SQL 2003语法，与Oracle SQL语法存在差异。要将Oracle的SQL语句运行在openLooKeng中，需要对SQL语句进行等价的语法转换。Oracle与openLooKeng SQL语法详细细节，请参考官方文档。


## Oracle Synonyms的支持

基于性能的考虑，openLooKeng默认关闭了Oracle 的`SYNONYM` 功能。可以通过以下配置来开启：

```
oracle.synonyms.enabled=true
```

## Oracle连接器的限制

- openLooKeng支持连接Oracle 11g和Oracle 12c。

- Oracle连接器暂不支持Update下推功能。

### Oralce的number数据类型

Oralce中的number类型的精度长度是可变的，但openLooKeng不支持。由于这个原因，若oracle表在使用number类型时，未明确指定精度，openLooKeng将oracle的number类型转换openLooKeng的decimal类型时，在特定的取值范围内会造成一些精度损失。