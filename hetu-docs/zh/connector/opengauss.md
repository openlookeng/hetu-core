
# openGauss连接器

openGauss连接器允许在外部openGauss数据库中查询和创建表。这可用于在openGauss和Hive等不同系统之间或在两个不同的openGauss实例之间联接数据。

## 配置

要配置openGauss连接器，在`etc/catalog`中创建一个目录属性文件，例如`opengauss.properties`，将openGauss连接器挂载为`opengauss`目录。使用以下内容创建文件，并根据设置替换连接属性：

``` properties
connector.name=opengauss
connection-url=jdbc:postgresql://example.net:15400/database
connection-user=root
connection-password=secret
```

### 多个openGauss数据库或服务器

openGauss连接器只能访问openGauss服务器中的单个数据库。因此，如果有多个openGauss数据库，或者想要连接到多个openGauss服务器，则必须配置多个openGauss连接器实例。

要添加另一个目录，只需添加另一个属性文件到具有不同名称的`etc/catalog`中（确保它以`.properties`结尾）。例如，如果将属性文件命名为`sales.properties`，openLooKeng将使用配置的连接器创建一个名为`sales`的目录。

## 查询openGauss

openGauss连接器为每个openGauss模式提供一个模式。可通过执行`SHOW SCHEMAS`来查看可用的openGauss模式：

    SHOW SCHEMAS FROM opengauss;

如果有一个名为`public`的openGauss模式，那么可以通过执行`SHOW TABLES`来查看这个模式中的表：

    SHOW TABLES FROM opengauss.public;

可以使用以下方法之一查看数据库`public`中`hetutb`表中的列的列表：

    DESCRIBE opengauss.public.hetutb;
    SHOW COLUMNS FROM opengauss.public.hetutb;

最后，可以访问`public`模式中的`hetutb`表：

    SELECT * FROM opengauss.public.hetutb;

如果对目录属性文件使用不同的名称，请使用该目录名称，而不要使用上述示例中的`opengauss`。

**注意**

> - openGuass数据库兼容类型为O（即DBCOMPATIBILITY = A）时不支持`Date`数据类型。

> - openGuass驱动暂不支持将数据库连接设置为只读模式以启用数据库的查询优化。

> - openGuass的`Character`数据类型单位是字节（例如：`VARCHAR(n)`数据类型中`n`是指字节长度），openLooKeng的`Character`数据类型单位是字符（例如：`VARCHAR(n)`数据类型中`n`是指字符长度），openGuass连接器不支持直接使用`create-table-as`方式创建含`Character`数据类型数据的表，需要手动指定`Character`数据类型的字节长度。

> - 不支持配置`use-connection-pool`。

*openGauss后续版本如果支持上述限制，我们会进行相应的适配。*

## openGauss连接器限制

暂不支持以下SQL语句：

[DELETE](../sql/delete.md)、[GRANT](../sql/grant.md)、[REVOKE](../sql/revoke.md)、[SHOW GRANTS](../sql/show-grants.md)、[SHOW ROLES](../sql/show-roles.md)、[SHOW ROLE GRANTS](../sql/show-role-grants.md)