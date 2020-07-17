
# CREATE SCHEMA

## 摘要

``` sql
CREATE {SCHEMA|DATABASE} [ IF NOT EXISTS ] schema_name
[ WITH ( property_name = expression [, ...] ) ]
```

## 说明

创建一个空模式。模式是保存表、视图和其他数据库对象的容器。

如果使用可选的 `IF NOT EXISTS` 子句，则在模式已存在时禁止显示错误。

可以使用可选的 `WITH` 子句来设置创建的模式的属性。要列出所有可用的模式属性，请运行以下查询：

    SELECT * FROM system.metadata.schema_properties

## 示例

在当前目录中创建模式 `web`：

    CREATE SCHEMA web
    CREATE DATABASE web

在 `hive` 目录中创建模式 `sales`：

    CREATE SCHEMA hive.sales

如果模式 `traffic` 尚不存在，则创建该模式：

    CREATE SCHEMA IF NOT EXISTS traffic

## 另请参见

[ALTER SCHEMA](./alter-schema.md)、[DROP SCHEMA](./drop-schema.md)