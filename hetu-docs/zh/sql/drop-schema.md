
# DROP SCHEMA

## 摘要

``` sql
DROP {SCHEMA|DATABASE} [ IF EXISTS ] schema_name [{CASCADE | RESTRICT}]
```

## 说明

删除一个现有的模式。该模式必须为空。

如果使用可选的 `IF EXISTS` 子句，则在该模式不存在时禁止显示错误。

## 示例

删除模式 `web`：

    DROP SCHEMA web
    DROP DATABASE web

如果模式 `sales` 存在，则删除该模式：

    DROP TABLE IF EXISTS sales

## 限制

从功能上而言，尚不支持 `CASCADE` 和 `RESTRICT`。

## 另请参见

[ALTER SCHEMA](./alter-schema.md)、[CREATE SCHEMA](./create-schema.md)