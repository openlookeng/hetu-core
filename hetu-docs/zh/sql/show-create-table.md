
# SHOW CREATE TABLE

## 摘要

``` sql
SHOW CREATE TABLE table_name
```

## 说明

显示创建指定的表的 SQL 语句。

## 示例

显示可用于创建 `orders` 表的 SQL 语句：

    SHOW CREATE TABLE sf1.orders;

``` sql
Create Table
-----------------------------------------
CREATE TABLE tpch.sf1.orders (
orderkey bigint,
orderstatus varchar,
totalprice double,
orderdate varchar
)
WITH (
format = 'ORC',
partitioned_by = ARRAY['orderdate']
)
(1 row)
```

## 另请参见

[CREATE TABLE](./create-table.md)