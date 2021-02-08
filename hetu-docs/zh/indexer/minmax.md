
# MinMax索引

MinMax索引简单地记录数据的最大和最小值，占用空间极小。
因此，这一索引仅仅能被用于已经排序的数据列。

## 过滤

1. MinMax索引用于调度时的分片过滤，被coordinator节点使用。

## 选择适用的列

MinMax索引仅仅能被用于已经排序的数据列。例如，ID或年龄.

## 支持的运算符

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal

## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

## 用例

创建索引:
```sql
create index idx using minmax on hive.hindex.users (age);
create index idx using minmax on hive.hindex.users (age) where regionkey=1;
create index idx using minmax on hive.hindex.users (age) where regionkey in (3, 1);
```

* 假设表已按照`regionkey`列分区

使用索引:
```sql
select name from hive.hindex.users where age=20
select name from hive.hindex.users where age>20
select name from hive.hindex.users where age<20
select name from hive.hindex.users where age>=20
select name from hive.hindex.users where age<=20
```