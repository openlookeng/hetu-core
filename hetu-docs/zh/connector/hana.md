
# Hana连接器

## 概述

Hana连接器允许在外部Hana数据库中查询和创建表。这可用于在Hana和Hive等不同系统之间或在两个不同的Hana实例之间联接数据。

## 配置

### 基础配置

首先，在开始使用Hana连接器之前，应该先完成以下步骤。

- 用于连接SAP HANA的JDBC连接详情

应该以常规openLooKeng连接器配置编写（例如名为**hana**的openLooKeng目录使用**hana.properties**）。文件应包含以下内容，并根据设置替换连接属性：

基本属性设置：

| connector.name=hana|
|----------|
| connection-url=jdbc:sap://主机:端口|
| connection-user=用户名|
| connection-password=密码|
| allow-drop-table=true #是否允许hana连接器丢弃表|

- 添加SAP HANA驱动

SAP HANA JDBC驱动不在普通存储库中提供，因此需要从SAP HANA下载并手动安装到存储库中。SAP HANA JDBC驱动（**ngdbc.jar**）可能会作为SAP HANA客户端安装的一部分进行安装，或者从SAP HANA办公网站下载。获取了SAP HANA JDBC驱动后，就可以将**jdbc jar**文件部署到协调节点和工作节点上的openLooKeng插件文件夹中。例如，jdbc驱动文件为**ngdbc.jar**，openLooKeng插件包文件夹为 **/usr/lib/presto/lib/plugin**，则拷贝命令如下： **cp ngdbc.jar /usr/lib/presto/lib/plugin/hana**。重启协调节点和工作节点进程，hana连接器即可正常工作。

- 是否开启查询下推功能。

如果要启用hana连接器的连接器下推功能，不需要做任何操作，hana连接器的下推功能默认是打开的。但也可以按如下设置：

| jdbc.pushdown-enabled=true|
|----------|
| #true表示打开下推，false表示关闭。|

- 下推模式选择。

如果要启用hana连接器的全部下推功能，不需要做任何操作，hana连接器的下推模式默认是全部下推的。但也可以按如下设置：

| jdbc.pushdown-module=FULL_PUSHDOWN|
|----------|
| #FULL_PUSHDOWN，表示全部下推；BASE_PUSHDOWN，表示部分下推，其中部分下推是指filter/aggregation/limit/topN/project这些可以下推。|

### 多套Hana数据库或服务器

如果要连接到多个Hana数据库，请将Hana插件的另一个实例配置为一个单独的目录。如需添加其他SAP HANA目录，请在 **../conf/catalog** 下添加不同名称的另一属性文件（注意结尾为 **.properties** ）。例如，在 **../conf/catalog** 目录下新增一个名称为 **hana2.properties** 的文件，则新增一个名称为hana2的连接器。

## 通过openLooKeng查询Hana

对于名为**hana**的SAP HANA连接器，每个SAP HANA数据库的用户都可以通过hana连接器获取其可用的模式，命令为**SHOW SCHEMAS**：

    SHOW SCHEMAS FROM hana;

如果已经拥有了可用模式，可以通过**SHOW TABLES**命令查看名为**data**的SAP HANA数据库拥有的表：

    SHOW TABLES FROM hana.data;

若要查看数据模式中名为**hello**的表中的列的列表，请使用以下命令中的一种：

    DESCRIBE hana.data.hello;
    SHOW COLUMNS FROM hana.data.hello;

你可以访问数据模式中的**hello**表：

    SELECT * FROM hana.data.hello;

连接器在这些模式中的权限是在连接属性文件中配置的用户的权限。如果用户无法访问这些表，则特定的连接器将无法访问这些表。

## openLooKeng和Hana之间的映射数据类型

### Hana到openLooKeng类型映射

openLooKeng支持读取以下SAP HANA的数据类型：下表显示了SAP HANA数据类型到openLooKeng数据类型的映射关系。

数据类型投影表：

> | SAP HANA数据类型| openLooKeng类型| 说明|
> |:----------|:----------|:----------|
> | DECIMAL(p, s)| DECIMAL(p, s)| |
> | SMALLDECIMAL| DOUBLE| 见smalldecimal映射|
> | TINYINT| TINYINT| |
> | SMALLINT| SMALLINT| |
> | INTEGER| INTEGER| |
> | BIGINT| BIGINT| |
> | REAL| REAL| |
> | DOUBLE| DOUBLE| |
> | FLOAT(n)| n \< 25，-> real, 25 \<= n \<= 53或未声明，-> double| |
> | BOOLEAN| BOOLEAN| |
> | VARCHAR| VARCHAR| |
> | NVARCHAR| VARCHAR| |
> | ALPHANUM| CHAR| |
> | SHORTTEXT| CHAR| |
> | VARBINARY| VARBINARY| |
> | DATE| DATE| |
> | TIME| TIME| |
> | TIMESTAMP| TIMESTAMP| |
> | SECONDDATE| NA| 不支持 |
> | BLOB| NA| 不支持 |
> | CLOB| NA| 不支持 |
> | NCLOB| NA| 不支持 |
> | TEXT| NA| 不支持 |
> | BINTEXT| NA| 不支持 |



**说明**

smalldecimal映射：不造成精度损失的排列是IEEE754 double的精确表示数值。详见：<https://en.wikipedia.org/wiki/Double-precision_floating-point_format>。

### openLooKeng到Hana类型映射

openLooKeng支持通过Hana connector在SAP HANA数据库中创建以下类型的表。下表显示了从openLooKeng到SAP HANA数据类型的映射关系。

> | openLooKeng类型| SAP HANA数据库类型| 说明|
> |:----------|:----------|:----------|
> | BOOLEAN| BOOLEAN| |
> | TINYINT| TINYINT| |
> | SMALLINT| SMALLINT| |
> | INTEGER| INTEGER| |
> | BIGINT| BIGINT| |
> | REAL| REAL| |
> | DOUBLE| DOUBLE| |
> | DECIMAL| DECIMAL| |
> | VARCHAR| VARCHAR| |
> | CHAR| CHAR| |
> | VARBINARY| VARBINARY| |
> | JSON| NA| 不支持 |
> | DATE| DATE| |
> | TIME| NA| 不支持 |
> | TIME WITH TIME ZONE| NA| 不支持|
> | TIMESTAMP| NA| 不支持|
> | TIMESTAMP WITH TIME ZONE| NA| 不支持|



### openLooKeng到Hana函数映射

可映射到SAP HANA函数的openLooKeng函数如下表所示。说明：“$n”是占位符，用于在函数中表示参数。

> | openLooKeng函数|HANA函数| 说明|
> |----------|----------|----------|
> | DATE_ADD(unit, $1, $2)| ADD_SECONDS($2, $1)<br>或ADD_DAYS($2, $1)<br>或ADD_MONTHS($2, $1)<br>或ADD_YEARS($2, $1)| 'unit'是second、minute、hour时，映射到ADD_SECONDS。<br>当'unit'为day或week时，映射到ADD_DAYS。<br>当'unit'为month、quarter时，映射到ADD_MONTHS。<br>当'unit'为year时，映射ADD_YEARS。|
> | CORR($1,$2)| CORR($1, $2)| |
> | STDDEV($1)| STDDEV($1)| |
> | VARIANCE($1)| VAR($1)| |
> | ABS($1)| ABS($1)| |
> | ACOS($1)| ACOS($1)| |
> | ASIN($1)| ASIN($1)| |
> | ATAN($1)| ATAN($1)| |
> | ATAN2($1,$2)| ATAN2($1, $2)| |
> | CEIL($1)| CEIL($1)| |
> | CEILING($1)| CEIL($1)| |
> | COS($1)| COS($1)| |
> | EXP($1)| EXP($1)| |
> | FLOOR($1)| FLOOR($1)| |
> | LN($1)| LN($1)| |
> | LOG10($1)| LOG(10, $1)| |
> | LOG2($1)| LOG(2, $1)| |
> | LOG($1,$2)| LOG($1, $2)| |
> | MOD($1,$2)| MOD($1, $2)| |
> | POW($1,$2)| POW($1, $2)| |
> | POWER($1,$2)| POWER($1, $2)| |
> | RAND()| RAND()| |
> | RANDOM()| RAND()| |
> | ROUND($1)| ROUND($1)| |
> | ROUND($1,$2)| ROUND($1, $2)| |
> | SIGN($1)| SIGN($1)| |
> | SIN($1)| SIN($1)| |
> | SQRT($1)| SQRT($1)| |
> | TAN($1)| TAN($1)| |
> | CONCAT($1,$2)| CONCAT($1, $2)| |
> | LENGTH($1)| LENGTH($1)| |
> | LOWER($1)| LOWER($1)| |
> | LPAD($1,$2,$3)| LPAD($1, $2, $3)| |
> | LTRIM($1)| LTRIM($1)| |
> | REPLACE($1,$2)| REPLACE($1, $2, ‘’)| |
> | REPLACE($1,$2,$3)| REPLACE($1, $2, $3)| |
> | RPAD($1,$2,$3)| RPAD($1, $2, $3)| |
> | RTRIM($1)| RTRIM($1)| |
> | STRPOS($1,$2)| LOCATE($1, $2)| |
> | SUBSTR($1,$2,$3)| SUBSTR($1, $2, $3)| |
> | POSITION($1,$2)| LOCATE($2, $1)| |
> | TRIM($1)| TRIM($1)| |
> | UPPER($1)| UPPER($1)| |
> | YEAR($1)| EXTRACT(YEAR FROM $1)| |
> | MONTH($1)| EXTRACT(MONTH FROM $1)| |
> | DAY($1)| EXTRACT(DAY FROM $1)| |
> | HOUR($1)| EXTRACT(HOUR FROM $1)| |
> | MINUTE($1)| EXTRACT(MINUTE FROM $1)| |
> | SECOND($1)| EXTRACT(SECOND FROM $1)| |
> | DAY_OF_WEEK($1)| WEEKDAY($1)| |


## Hana SQL迁移到openLooKeng SQL指南

### Hana和openLooKeng的SQL语法差异

例如，在hana SQL中，将行数据转换为列数据的`map`函数是openLooKeng SQL所不支持的。但是可以使用`case`作为替代实现。

例如，如果有一个名为**SCORES**的表：

| 名称| 课程| 分数|
|:----------|:----------|:----------|
| zs| 英语| 90|
| zs| 数学| 80|
| zs| 科学| 99|
| ls| 数学| 80|
| ls| 科学| 99|
| ls| 英语| 90|
| ww| 科学| 99|
| ww| 数学| 80|
| ww| 英语| 90|

在hana中，可以使用`map`函数将行数据转换为列数据：

```sql
SELECT
  NAME,
  SUM(MAP(SUBJECT,'English',SCORE,0)) AS "English",
  SUM(MAP(SUBJECT,'Math',SCORE,0)) AS "Math",
  SUM(MAP(SUBJECT,'Science',SCORE,0)) AS "Science"
FROM SCORES
GROUP BY NAME
```

在Prersto中，可以使用`case`作为替代实现：

Hana与openLooKeng SQL语法的其他差异，请参考以下官方文档列表：

| 名称| 网址|
|:----------|:----------|
| openLooKeng| [SQL Grammar](../sql/_index.md) |
| Hana| [SQL Grammar](https://help.sap.com/viewer/7c78579ce9b14a669c1f3295b0d8ca16/Cloud/en-US/20ff532c751910148657c32fe3431a9f.html) |

### 时间依赖类型的差异

当使用openlk-cli连接openLooKeng服务器处理无时区时间和时间戳时，返回给cli显示的结果将取决于openlk-cli的启动配置。例如，我们带用户时区启动openlk-cli，如下所示：

    java -jar -Duser.timezone=Asia/Tokyo -jar ./hetu-cli-*.jar
    --client-request-timeout 30m --server ip:8080 --session legacy_timestamp=false

当处理时间和时间戳依赖类型时，openlk-cli将显示带有时区的时间依赖类型：

```sql
lk> select current_time;
                  _col0
          -------------------------
           21:19:49.122 Asia/Tokyo
lk> select current_timezone();
              _col0
           ------------
            Asia/Tokyo
             (1 row)
```

如果带用户时区启动openlk-cli，如下所示：

```shell
java -jar ./hetu-cli-*.jar
--client-request-timeout 30m --server ip:8080
--session legacy_timestamp=false --catalog hana2
```

当处理时间和时间戳依赖类型时，openlk-cli将显示不带时区的时间依赖类型：取而代之的是显示UTC/GMT时区：

```sql
lk> select current_timezone();
          _col0
        --------
         +08:00
         (1 row)
lk> select current_time;
          _col0
          ---------------------
          20:20:45.659 +08:00
          (1 row)
```

但在hana中，时间依赖类型的行为取决于hana服务器。例如，我们直接通过jdbc启动hana客户端：

```shell
java -jar -Duser.timezone=Asia/Tokyo ngdbc.jar -u
database,passwd -n ip:34215 -c "SELECT CURRENT_TIME FROM DUMMY"
       Connected.
       |          |
       ------------
       | 20:38:57 |
```

## Hana连接器的限制

由于hana数据类型与openLooKeng数据类型的差异，在将openLooKeng数据类型投影为hana数据类型时存在一些限制。

### Hana的Smalldecimal数据类型

Hana中的smalldecimal精度和零标度长度可变，但openLooKeng不支持。由于这个原因，openLooKeng将hana中的smalldecimal转换成了openLooKeng中的double，这在特定的取值范围内会造成一些精度损失。

### Hana的tiny int数据类型

Hana中的tiny int是一个8位无符号整数，在openLooKeng中会导致一些精度损失。