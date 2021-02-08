# BTree索引

BTree索引使用二叉树数据结构存储。索引的大小随着索引列中不同值的个数而增加。

## 过滤

1. Bloom索引用于调度时的分片过滤，被coordinator节点使用。

## 选择适用的列

位图索引在拥有较多不同值数量的列上比较适用，例如：ID。除此之外，BTree索引还要求表是分区的。

在BTree和Bloom索引之间选择时，需要考虑：
- Bloom索引只支持`=`
- Btree索引要求表是分区的
- Bloom索引是不确定的，而BTree索引是确定的。因此BTree通常有更好的过滤性能
- BTree索引比Bloom索引更大

## 支持的运算符

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal
    BETWEEN Between range
    IN      IN set

## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "real", "date"

## 用例

创建索引:
```sql
create index idx using btree on hive.hindex.orders (orderid) with (level=partition) where orderDate='01-10-2020' ;
create index idx using btree on hive.hindex.orders (orderid) with (level=partition) where orderDate in ('01-10-2020', '01-10-2020');
```

* 假设表已按照`orderDate`列分区

使用索引:
```sql
select * from hive.hindex.orders where orderid=12345
select * from hive.hindex.orders where orderid>12345
select * from hive.hindex.orders where orderid<12345
select * from hive.hindex.orders where orderid>=12345
select * from hive.hindex.orders where orderid<=12345
select * from hive.hindex.orders where orderid between (10000, 20000)
select * from hive.hindex.orders where orderid in (12345, 7890)
```