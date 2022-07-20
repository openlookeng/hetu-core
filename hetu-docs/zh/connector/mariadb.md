# MariaDB连接器

MariaDB连接器允许在外部MariaDB数据库中查询和创建表。这可用于在MySQL和MariaDB等不同系统之间或在两个不同的MariaDB实例之间联接数据。

## 配置

要配置MariaDB连接器，在openLooKeng的安装目录中`etc/catalog`中创建一个目录属性文件，例如`maria.properties`，将MariaDB连接器挂载为maria目录。使用以下内容创建文件，并根据设置替换连接属性：

```properties
connector.name=maria
connection-url=jdbc:mariadb://部署了MairaDB的ip:MariaDB服务端口(默认3306)
connection-user=您的数据源用户
connection-password=您的数据源密码
```

- 是否开启查询下推功能

如果要启用MariaDB连接器的连接器下推功能，不需要做任何操作，MariaDB连接器的下推功能默认是打开的。但也可以按如下设置：

```properties
#true表示打开下推，false表示关闭。
jdbc.pushdown-enabled=true
```

- 下推模式选择

MariaDB连接器的下推模式默认是部分下推的，如果要启用MariaDB连接器的全部下推功能，可以按如下设置：

```properties
#FULL_PUSHDOWN，表示全部下推；BASE_PUSHDOWN，表示部分下推，其中部分下推是指filter/aggregation/limit/topN/project这些可以下推。
jdbc.pushdown-module=FULL_PUSHDOWN
```

### 外部函数注册

MariaDB连接器支持注册外部函数。

配置支持下推的外部函数注册命名空间`catalog.schema`。 例如在`etc/catalog/maria.properties`中配置：

```Properties
jdbc.pushdown.remotenamespace=mariafun.default
```

### 外部函数下推

将外部函数下推到MariaDB数据源执行。

配置支持下推的外部函数注册命名空间`catalog.schema`。 例如在`etc/catalog/maria.properties`中配置：

```Properties
jdbc.pushdown.remotenamespace=mariafun.default
```

可以声明自己支持多个函数命名空间中的函数，在`jdbc.pushdown.remotenamespace`配置项中使用'|'分割既可。例如：

```Properties
jdbc.pushdown.remotenamespace=mariafun1.default|mariafun2.default|mariafun3.default
#表示当前Connector实例同时支持mysqlfun1.default、mysqlfun2.default、mysqlfun3.default三个函数命名空间最终的函数下推到当前连接的数据源中执行。
```

### 多个MariaDB服务器

可以根据需要创建任意多的目录，因此，如果有额外的MariaDB服务器，只需添加另一个不同名称的属性文件到`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 查询MySQL

MariaDB连接器为每个MariaDB*数据库*提供一个模式。可通过执行`SHOW SCHEMAS`来查看可用MariaDB数据库：

```sql
-- 我们的catalog名称为maria
SHOW SCHEMAS FROM maria;
```

如果有一个名为`test`的MySQL数据库，可以通过执行`SHOW TABLES`查看数据库中的表：

```sql
SHOW TABLES FROM maria.test;
```

可以使用以下方法之一查看`test`数据库中`user`表中的列的列表：

```sql
DESCRIBE maria.test.user;
SHOW COLUMNS FROM maria.test.user;
```

最后，可以访问`test`数据库中`user`的表：

```sql
SELECT * FROM maria.test.user;
```

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`maria`。

## MariaDB连接器限制

暂不支持以下SQL语句：

[DELETE](../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)
