# DM(达梦数据库)连接器

## 概述

DM连接器允许在外部DM数据库中查询和创建表。这可用于在DM和Hive等不同系统之间或在两个不同的DM实例之间联接数据。

## 配置

首先，在开始使用DM连接器之前，应该先完成以下步骤：

- 用于连接DM的JDBC连接详情

要配置DM连接器，在`etc/catalog`中创建一个目录属性文件，例如`dameng.properties`。使用以下内容创建文件，并根据设置替换连接属性：

```properties
connector.name=dameng
connection-url=jdbc:dm://主机:端口
connection-user=用户名
connection-password=密码
```

- 添加DM的JDBC驱动

DM JDBC驱动不在普通存储库中提供，如果您是DM数据库的用户，您可以选择前往DM官方网站，在确保遵守DM JDBC驱动所适用的license的条件下，下载和安装DM JDBC驱动到存储库中。DM JDBC驱动（DmJdbcDriver***X***.jar ***X***为数字，根据DM的版本不同而不同）可能会作为DM客户端安装的一部分进行安装。获取了DM JDBC驱动后，您可以将**jdbc jar**文件部署到协调节点和工作节点上的openLooKeng插件文件夹中。例如，jdbc驱动文件为**DmJdbcDriverX.jar**，openLooKeng插件包文件夹为 **/opt/hetu-server/plugin**，则拷贝命令如下： **cp DmJdbcDriverX.jar /opt/hetu-server/plugin/dm**。重启协调节点和工作节点进程，DM连接器即可正常工作。

- 是否开启查询下推功能

如果要启用DM连接器的连接器下推功能，不需要做任何操作，DM连接器的下推功能默认是打开的。但也可以按如下设置：

``` properties
jdbc.pushdown-enabled=true
#true表示打开下推，false表示关闭。
```

- 下推模式选择。

如果要启用DM连接器的全部下推功能，不需要做任何操作，DM连接器的下推模式默认是全部下推的。但也可以按如下设置：

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN
#FULL_PUSHDOWN，表示全部下推；BASE_PUSHDOWN，表示部分下推，其中部分下推是指filter/aggregation/limit/topN/project这些可以下推。
```

## 通过openLooKeng查询DM

对于名为**dameng**的DM连接器，每个DM数据库的用户都可以通过DM连接器获取其可用的模式，命令为**SHOW SCHEMAS**：

    SHOW SCHEMAS FROM dameng;

如果已经拥有了可用模式，可以通过**SHOW TABLES**命令查看名为**data**的DM数据库拥有的表：

    SHOW TABLES FROM dameng.data;

若要查看数据模式中名为**hello**的表中的列的列表，请使用以下命令中的一种：

    DESCRIBE dameng.data.hello;
    SHOW COLUMNS FROM dameng.data.hello;

你可以访问数据模式中的**hello**表：

    SELECT * FROM dameng.data.hello;

连接器在这些模式中的权限是在连接属性文件中配置的用户的权限。如果用户无法访问这些表，则特定的连接器将无法访问这些表。

## DM Update/Delete 支持

### 使用DM连接器创建表

示例：

```sql
CREATE TABLE dameng_table (
    id int,
    name varchar(255));
```

### 对表执行INSERT

示例：

```sql
INSERT INTO dameng_table
  VALUES
     (1, 'foo'),
     (2, 'bar');
```

### 对表执行UPDATE

示例：

```sql
UPDATE dameng_table
  SET name='john'
  WHERE id=2;
```

上述示例将值为`2`的列`id`的行的列`name`的值更新为`john`。

UPDATE前的SELECT结果：

```sql
lk:default> SELECT * FROM dameng_table;
id | name
----+------
  2 | bar
  1 | foo
(2 rows)
```

UPDATE后的SELECT结果

```sql
lk:default> SELECT * FROM dameng_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

### 对表执行DELETE

示例：

```sql
DELETE FROM dameng_table
  WHERE id=2;
```

以上示例删除了值为`2`的列`id`的行。

DELETE前的SELECT结果：

```sql
lk:default> SELECT * FROM dameng_table;
 id | name
----+------
  2 | john
  1 | foo
(2 rows)
```

DELETE后的SELECT结果：

```sql
lk:default> SELECT * FROM dameng_table;
 id | name
----+------
  1 | foo
(1 row)
```

## DM连接器的限制

- openLooKeng支持连接DM8。

- DM连接器暂不支持Update下推功能。