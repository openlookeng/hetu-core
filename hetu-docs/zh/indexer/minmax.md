
# MinMaxIndex

MinMaxIndex简单地记录数据的最大和最小值，占用空间极小。
因此，这一索引仅仅能被用于已经排序的数据列。

## 使用场景

**注意：当前，启发式索引仅支持ORC存储格式的Hive数据源。**

MinMaxIndex用于调度时的分片过滤，被coordinator节点使用。

## 选择适用的列

在对数据进行排序的列上具有过滤predicate的query可以从MinMaxIndex中得到好的效果。

例如，如果一下数据是根据`age`来排序的，那么一个像`SELECT name from users WHERE age> 25`
之类的query则可以因有效地在`age`上利用MinMaxIndex，而从中得到好的效果。

## 支持的运算符

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal

## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date", "decimal"

**注意:** 不支持采用其它数据类型来创建index。

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