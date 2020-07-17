
# SQL Server连接器

SQL Server连接器允许在外部SQL Server数据库中查询和创建表。这可用于在SQL Server和Hive等不同系统之间或在两个不同的SQL Server实例之间联接数据。

## 配置

要配置SQL Server连接器，在`etc/catalog`中创建一个目录属性文件，例如`sqlserver.properties`，将SQL Server连接器挂载为`sqlserver`目录。使用以下内容创建文件，并根据设置替换连接属性：

``` properties
connector.name=sqlserver
connection-url=jdbc:sqlserver://[serverName[\instanceName][:portNumber]]
connection-user=root
connection-password=secret
```

### 多个SQL Server数据库或服务器

SQL Server连接器只能访问SQL Server服务器中的单个数据库。因此，如果有多个SQL Server数据库，或者想要连接到多个SQL Server实例，则必须配置多个目录，每个实例一个。

要添加另一个目录，只需添加另一个属性文件到具有不同名称的`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 查询SQL Server服务器

SQL Server连接器提供对配置数据库中指定用户可见的所有模式的访问。对于以下示例，假设SQL Server目录为`sqlserver`。

可通过执行`SHOW SCHEMAS`来查看可用的模式：

    SHOW SCHEMAS FROM sqlserver;

如果有一个名为`web`的模式，那么可以通过执行`SHOW TABLES`来查看这个模式中的表：

    SHOW TABLES FROM sqlserver.web;

可以使用以下方法之一查看`web`数据库中`clicks`表中的列的列表：

    DESCRIBE sqlserver.web.clicks;
    SHOW COLUMNS FROM sqlserver.web.clicks;

最后，可以访问`web`模式中的`clicks`表：

    SELECT * FROM sqlserver.web.clicks;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`sqlserver`。

## SQL Server连接器限制

openLooKeng支持连接SQL Server 2016、SQL Server 2014、SQL Server 2012以及Azure SQL Database。

openLooKeng支持以下SQL Server数据类型。下表显示了SQL Server和openLooKeng数据类型之间的映射。

SQL Server类型   openLooKeng类型

-------------------------------


| SQL Server类型| openLooKeng类型|
|:----------|:----------|
| `bigint`| `bigint`|
| `smallint`| `smallint`|
| `int`| `integer`|
| `float`| `double`|
| `char(n)`| `char(n)`|
| `varchar(n)`| `varchar(n)`|
| `date`| `date`|

[SQL Server数据类型](https://msdn.microsoft.com/en-us/library/ms187752.aspx )的完整列表。

暂不支持以下SQL语句：

[DELETE](../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)