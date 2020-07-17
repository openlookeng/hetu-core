
# CREATE TABLE AS

## 摘要

``` sql
CREATE TABLE [ IF NOT EXISTS ] table_name [ ( column_alias, ... ) ]
[ COMMENT table_comment ]
[ WITH ( property_name = expression [, ...] ) ]
AS query
[ WITH [ NO ] DATA ]
```

## 说明

创建一个包含 [SELECT](./select.md) 查询结果的表。使用 [CREATE TABLE](./create-table.md) 可以创建空表。

如果使用可选的 `IF NOT EXISTS` 子句，则在表已存在时禁止显示错误。

可以使用可选的 `WITH` 子句来设置创建的表的属性。要列出所有可用的表属性，请运行以下查询：

    SELECT * FROM system.metadata.table_properties

## 示例

使用查询结果和给定的列名创建表 `orders_column_aliased`：

    CREATE TABLE orders_column_aliased (order_date, total_price)
    AS
    SELECT orderdate, totalprice
    FROM orders

创建对 `orders` 进行汇总的表 `orders_by_date`：

    CREATE TABLE orders_by_date
    COMMENT 'Summary of orders by date'
    WITH (format = 'ORC')
    AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

如果表 `orders_by_date` 尚不存在，则创建该表：

    CREATE TABLE IF NOT EXISTS orders_by_date AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate

创建模式与 `nation` 相同的表 `empty_nation`，但表中不含数据。

    CREATE TABLE empty_nation AS
    SELECT *
    FROM nation
    WITH NO DATA

## 另请参见

[CREATE TABLE](./create-table.md)、[SELECT](./select.md)