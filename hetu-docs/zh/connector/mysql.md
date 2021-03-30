
# MySQL连接器

MySQL连接器允许在外部MySQL数据库中查询和创建表。这可用于在MySQL和Hive等不同系统之间或在两个不同的MySQL实例之间联接数据。

## 配置

要配置MySQL连接器，在`etc/catalog`中创建一个目录属性文件，例如`mysql.properties`，将MySQL连接器挂载为`mysql`目录。使用以下内容创建文件，并根据设置替换连接属性：

``` properties
connector.name=mysql
connection-url=jdbc:mysql://example.net:3306
connection-user=root
connection-password=secret
```
- 是否开启查询下推功能

如果要启用mysql连接器的连接器下推功能，不需要做任何操作，mysql连接器的下推功能默认是打开的。但也可以按如下设置：

``` properties
jdbc.pushdown-enabled=true
#true表示打开下推，false表示关闭。
```

- 下推模式选择

mysql连接器的下推模式默认是部分下推的，如果要启用mysql连接器的全部下推功能，可以按如下设置：

``` properties
jdbc.pushdown-module=FULL_PUSHDOWN
#FULL_PUSHDOWN，表示全部下推；BASE_PUSHDOWN，表示部分下推，其中部分下推是指filter/aggregation/limit/topN/project这些可以下推。
```

### 外部函数注册
MySQL连接器支持注册外部函数。

配置支持下推的外部函数注册命名空间`catalog.schema`。
例如在`etc/catalog/mysql.properties`中配置：
```Properties
jdbc.pushdown.remotenamespace=mysqlfun.default
```

### 外部函数下推
将外部函数下推到MySQL数据源执行。

配置支持下推的外部函数注册命名空间`catalog.schema`。
例如在`etc/catalog/mysql.properties`中配置：
```Properties
jdbc.pushdown.remotenamespace=mysqlfun.default
```
可以声明自己支持多个函数命名空间中的函数，在`jdbc.pushdown.remotenamespace`配置项中使用'|'分割既可。例如：
```Properties
jdbc.pushdown.remotenamespace=mysqlfun1.default|mysqlfun2.default|mysqlfun3.default
#表示当前Connector实例同时支持mysqlfun1.default、mysqlfun2.default、mysqlfun3.default三个函数命名空间最终的函数下推到当前连接的数据源中执行。
```

### 多个MySQL服务器

可以根据需要创建任意多的目录，因此，如果有额外的MySQL服务器，只需添加另一个不同名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 查询MySQL

MySQL连接器为每个MySQL*数据库*提供一个模式。可通过执行`SHOW SCHEMAS`来查看可用MySQL数据库：

    SHOW SCHEMAS FROM mysql;

如果有一个名为`web`的MySQL数据库，可以通过执行`SHOW TABLES`查看数据库中的表：

    SHOW TABLES FROM mysql.web;

可以使用以下方法之一查看`web`数据库中`clicks`表中的列的列表：

    DESCRIBE mysql.web.clicks;
    SHOW COLUMNS FROM mysql.web.clicks;

最后，可以访问`web`数据库中`clicks`的表：

    SELECT * FROM mysql.web.clicks;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`mysql`。

## MySQL连接器限制

暂不支持以下SQL语句：

[DELETE](../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)