
# DROP TABLE

## 摘要

``` sql
DROP TABLE  [ IF EXISTS ] table_name
```

## 说明

删除一个现有的表。

如果使用可选的 `IF EXISTS` 子句，则在该表不存在时禁止显示错误。

## 示例

删除表 `orders_by_date`：

    DROP TABLE orders_by_date

如果表 `orders_by_date` 存在，则删除该表：

    DROP TABLE IF EXISTS orders_by_date

## 另请参见

[ALTER TABLE](./alter-table.md)、[CREATE TABLE](./create-table.md)