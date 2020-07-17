
# DELETE

## 摘要

``` sql
DELETE FROM table_name [ WHERE condition ]
```

## 说明

删除表中的行。如果指定了 `WHERE` 子句，则仅删除匹配的行。否则，将删除表中的所有行。

## 示例

删除所有空运行项目：

    DELETE FROM lineitem WHERE shipmode = 'AIR';

删除低优先级订单的所有行项目：

    DELETE FROM lineitem
    WHERE orderkey IN (SELECT orderkey FROM orders WHERE priority = 'LOW');

删除所有订单：

    DELETE FROM orders;

## 限制

某些连接器对 `DELETE` 的支持有限或不支持该语句。

例如，对于 Hive 连接器，事务表和非事务表的行为是不同的。

对于事务表，可以通过 WHERE 条件来删除任何行。不过，对于非事务表，仅当 WHERE 子句匹配整个分区时才支持 DELETE。

有关更多详细信息，请参见连接器文档。