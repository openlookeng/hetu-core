# 表下推优化器规则

openLooKeng支持Table下推优化器规则，以改善符合条件的查询延迟。

用户须执行以下命令为SQL查询中的所有表生成stats，以便根据表的大小对连接源重新排序-

```sql
ANALYZE tableName;
```

**User Hint Comment-**

用户还可以添加以下格式的特别注释，通过指定表名及其不重复（主键）列名，来利用表下推规则的好处。

```sql
/* #distinct@ table1 = col1, col2, ... #*/
```

此提示是可选的，因为如果表有相关统计信息， openLookeng可以标识所有不同的列。

以下查询（由TPC-H Benchmark的Query 17修改而来）就是一个满足条件的查询，将外层表推入子查询，从而提升整体查询时延。

**Original Query-**

```sql
SELECT 
  Sum(lineitem.extendedprice) / 7.0 AS avg_yearly 
FROM 
  lineitem, 
  part, 
(
  SELECT 
    0.2 * Avg(lineitem.quantity) AS s_avg, lineitem.partkey AS s_partkey 
  FROM 
    lineitem 
  GROUP BY 
    lineitem.partkey 
) 
WHERE 
  part.partkey = lineitem.partkey 
AND 
  part.brand = 'Brand#43' 
AND 
  part.container = 'LG PACK' 
AND 
  part.partkey = s_partkey 
AND 
  lineitem.quantity < s_avg /* #distinct@ part = partkey #*/;
```

在上述查询中，表`part`是相关的外部查询表，以`partkey`为唯一列，与表`lineitem`连接。下面给出一个等价的重写后的查询语句，将`part`表推入到子查询。

**Equivalent Rewritten Query-**

```sql
SELECT 
  Sum(lineitem.extendedprice) / 7.0 AS avg_yearly 
FROM 
  lineitem, 
(
  SELECT 
    0.2 * Avg(lineitem.quantity) AS s_avg, lineitem.partkey AS s_partkey 
  FROM 
    lineitem, part 
  WHERE 
    part.brand = 'Brand#43' 
  AND 
    part.container = 'LG PACK' 
  AND 
    part.partkey = lineitem.partkey 
  GROUP BY 
    lineitem.partkey
) 
WHERE 
  s_partkey = lineitem.partkey 
AND 
  lineitem.quantity < s_avg;
```

The feature is disabled by default. The user can enable it by executing the following command to set the session parameter-

该功能默认是关闭状态。用户可以通过以下命令设置session参数来启用该功能-

```sql
SET SESSION push_table_through_subquery = true;
```

