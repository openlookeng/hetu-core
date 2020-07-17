
# Minmax索引

## 用例

MinMax 索引用于拆分过滤，仅被**coordinator**节点使用。

如果查询中作为谓词一部分的列存在此索引，则引擎可以通过筛选预定Splits来提高性能，类似于Bloom索引。

例如，如果索引在列

`age`

查询语句为

```sql
select * from table where age > 50
```

*提示：对索引列中的数据进行排序将提供最佳结果。*
