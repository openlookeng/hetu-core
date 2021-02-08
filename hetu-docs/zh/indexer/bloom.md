
# Bloom索引

Bloom索引实用布隆过滤器来过滤数据。索引体积非常小。

## 过滤

1. Bloom索引用于调度时的分片过滤，被coordinator节点使用。
2. Bloom索引也用于worker节点上，用于在读取ORC文件是过滤stripes。

## 选择适用的列

位图索引在拥有较多不同值数量的列上比较适用，例如：ID。

## 支持的运算符

    =       Equality

## 支持的列类型
    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date"

## 配置参数

### `bloom.fpp`
 
> -   **类型:** `Double`
> -   **默认值:** `0.001`
> 
> 改变布隆过滤器的FPP (false positive probability)。
> 更小的FPP会提高索引的过滤能力，但是会增加索引的体积。在大多数情况下默认值就足以够用。
> 如果创建的索引太大，可以考虑增加这个值（例如，至0.05)。

## 用例

创建索引:
```sql
create index idx using bloom on hive.hindex.users (id);
create index idx using bloom on hive.hindex.users (id) where regionkey=1;
create index idx using bloom on hive.hindex.users (id) where regionkey in (3, 1);
create index idx using bloom on hive.hindex.users (id) WITH ("bloom.fpp" = '0.001');
```

* 假设表已按照`regionkey`列分区

使用:
```sql
select name from hive.hindex.users where id=123
```