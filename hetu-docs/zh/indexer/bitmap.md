
# BitMap（位图）索引

BitMap索引使用位图。索引的大小随着索引列中不同值的个数而增加。例如，一个标记性别的列很小，而一个ID列的索引则会极大（不推荐）。

注意：在ORC算子下推启用时，BitMap索引效果更好。可以通过设置`hive.properties`中的`hive.orc-predicate-pushdown-enabled=true`来启用，
或者在命令行中启用`set session hive.orc_predicate_pushdown_enabled=true;`。

参见[Properties](../admin/properties.md)获得更多信息。

## 过滤

1. BitMap索引用于过滤从ORC文件中读取的数据，且仅供worker节点使用。

## 选择适用的列

BitMap索引在拥有较少不同值数量的列上比较适用，例如：性别。

## 支持的运算符

    =       Equality
    >       Greater than
    >=      Greater than or equal
    <       Less than
    <=      Less than or equal
    BETWEEN Between range
    IN      IN set
    
## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

## 用例

创建：
```sql
create index idx using bitmap on hive.hindex.users (gender);
create index idx using bitmap on hive.hindex.users (gender) where regionkey=1;
create index idx using bitmap on hive.hindex.users (gender) where regionkey in (3, 1);
```

* 假设表已按照`regionkey`列分区

使用:
```sql
select name from hive.hindex.users where gender="female"
select * from hive.hindex.users where id>123
select * from hive.hindex.users where id<123
select * from hive.hindex.users where id>=123
select * from hive.hindex.users where id<=123
select * from hive.hindex.users where id between (100, 200)
select * from hive.hindex.users where id in (123, 199)
```