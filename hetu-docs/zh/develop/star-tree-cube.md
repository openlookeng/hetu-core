# Star-Tree

Star-tree多维数据集是一种预聚合技术，用于实现低延迟冰山查询。冰山查询用于计算属性（或属性集）的聚合函数，以便查找高于指定阈值的聚合值。通过该技术，用户能够创建具有必要聚合和维度的多维数据集。然后，当执行聚合查询时，多维数据集用于执行查询而非原始表。实际性能提升是在TableScan操作期间实现的，因为多维数据集是预计算和预聚合的。

因此，当group by基数产生的行少于原始表时，多维数据集技术非常有效。

## 支持功能

    COUNT, COUNT DISTINCT, MIN, MAX, SUM, AVG

## 启用和禁用star-tree

启用star-tree：

```sql
SET SESSION enable_star_tree_index=true;
```

禁用star-tree：

```sql
SET SESSION enable_star_tree_index=false;
```

## 配置属性

| 属性名称| 默认值| 是否必填| 说明|
|----------|----------|----------|----------|
| optimizer.enable-star-tree-index| false| 否| 启用star-tree索引|
| cube.metadata-cache-size| 5| 否| 在缓存清空前可以加载到缓存中的star-tree元数据的最大数量|
| cube.metadata-cache-ttl| 1h| 否| 在缓存清空前加载到缓存中的star-tree的最长保留时间|

## 示例

创建star-tree多维数据集：

```sql
CREATE CUBE nation_cube 
ON nation 
WITH (AGGREGATIONS=(count(*), count(distinct regionkey), avg(nationkey), max(regionkey)),
GROUP=(nationkey),
format='orc', partitioned_by=ARRAY['nationkey']);
```

向多维数据集添加数据：

```sql
INSERT INTO CUBE nation_cube WHERE nationkey > 5;
```

要使用新多维数据集，只需使用多维数据集中包含的聚合来查询原始表：

```sql
SELECT count(*) FROM nation WHERE nationkey > 5 GROUP BY nationkey;
SELECT nationkey, avg(nationkey), max(regionkey) WHERE nationkey > 5 GROUP BY nationkey;
```

## 优化器变更

Star-tree聚合规则为迭代优化器，通过将原始聚合子树和原始表扫描替换为预聚合表扫描来优化逻辑计划。

## 依赖

Star-tree索引依赖于Hetu元存储来存储多维数据集相关的元数据。有关更多信息，请查看[Hetu元存储](../admin/meta-store.md)。

## 限制

1. Star-tree多维数据集仅在group by基数远低于源表中的行数时有效。
2. 维护大型数据集的多维数据集需要大量的用户人力。
3. 仅支持增量插入多维数据集。无法从多维数据集删除特定行。