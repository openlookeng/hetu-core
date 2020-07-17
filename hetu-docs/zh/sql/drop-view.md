
# DROP VIEW

## 摘要

``` sql
DROP VIEW [ IF EXISTS ] view_name
```

## 说明

删除一个现有的视图。

如果使用可选的 `IF EXISTS` 子句，则在该视图不存在时禁止显示错误。

## 示例

删除视图 `orders_by_date`：

    DROP VIEW orders_by_date

如果视图 `orders_by_date` 存在，则删除该视图：

    DROP VIEW IF EXISTS orders_by_date

## 另请参见

[CREATE VIEW](./create-view.md)