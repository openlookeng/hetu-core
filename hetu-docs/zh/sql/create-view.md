
# CREATE VIEW

## 摘要

``` sql
CREATE [ OR REPLACE ] VIEW view_name
[ SECURITY { DEFINER | INVOKER } ]
AS query
```

## 说明

创建 [SELECT](./select.md) 查询的视图。视图是一个可以由将来的查询引用的逻辑表。视图不包含任何数据。相反，视图存储的查询在每次被其他查询引用时执行。

如果使用可选的 `OR REPLACE` 子句，则在视图已存在时替换该视图，而不是产生错误。

## 安全

在默认的 `DEFINER` 安全模式下，使用视图拥有者（视图的创建者或定义者）的权限来访问视图中引用的表，而不是使用执行查询的用户的权限。这样，对于用户可能无法直接访问的基础表，可以提供对这些表的受限访问。

在 `INVOKER` 安全模式下，使用执行查询的用户（视图的调用者）的权限来访问在视图中引用的表。以该模式创建的视图只是一个存储查询。

无论安全模式如何，`current_user` 函数都始终返回执行查询的用户，因此可以在视图中使用该函数来滤除行或限制访问。

## 示例

在表 `orders` 上创建简单视图 `test`：

    CREATE VIEW test AS
    SELECT orderkey, orderstatus, totalprice / 2 AS half
    FROM orders

创建对 `orders` 进行汇总的视图 `orders_by_date`：

    CREATE VIEW orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

创建一个替换现有视图的视图：

    CREATE OR REPLACE VIEW test AS
    SELECT orderkey, orderstatus, totalprice / 4 AS quarter
    FROM orders

## 另请参见

[DROP VIEW](./drop-view.md)、[SHOW CREATE VIEW](./show-create-view.md)