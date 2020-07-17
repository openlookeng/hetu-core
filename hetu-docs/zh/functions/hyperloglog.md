
# HyperLogLog 函数

openLooKeng 使用 [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) 数据结构实现 `approx_distinct` 函数。

## 数据结构

openLooKeng 将 HyperLogLog 数据草图实现为一组存储*最大哈希*的 32 位数据桶。这些数据桶能够以稀疏（作为数据桶 ID 到数据桶的映射）或密集（作为连续的内存块）的方式存储。HyperLogLog 数据结构以稀疏表示开始，当它更高效时会切换到密集表示。P4HyperLogLog 结构以密集方式初始化，并且在其生命周期内保持密集。

`hyperloglog_type` 隐式转换为 `p4hyperloglog_type`，您可以显式地将 `HyperLogLog` 转换为 `P4HyperLogLog`：

    cast(hll AS P4HyperLogLog)

## 序列化

可以将数据草图序列化为 `varbinary`，也可以从中反序列化数据草图。这样就可以存储数据草图供以后使用。这与合并多个草图的功能相结合，使您能够计算查询分区元素的 `approx_distinct`，然后以很小的代价计算整个查询。

例如，计算每日唯一用户的 `HyperLogLog` 之后，可以通过合并每日数据以增量方式计算每周或每月的唯一用户。这类似于通过汇总每日收入来计算每周收入。可以将 `approx_distinct` 与 `GROUPING SETS` 的结合使用转换为使用 `HyperLogLog`。示例：

    CREATE TABLE visit_summaries (
      visit_date date,
      hll varbinary
    );
    
    INSERT INTO visit_summaries
    SELECT visit_date, cast(approx_set(user_id) AS varbinary)
    FROM user_visits
    GROUP BY visit_date;
    
    SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users
    FROM visit_summaries
    WHERE visit_date >= current_date - interval '7' day;

## 函数

**approx\_set(x)** -> HyperLogLog

返回 `x` 的输入数据集的 `HyperLogLog` 草图。该数据草图是 `approx_distinct` 的基础，以后可以通过调用 `cardinality()` 来存储和使用该数据草图。

**cardinality(hll)** -> bigint

这将对 `hll` HyperLogLog 数据草图汇总的数据执行 `approx_distinct`。

**empty\_approx\_set()** -> HyperLogLog

返回一个空 `HyperLogLog`。

**merge(HyperLogLog)** -> HyperLogLog

返回各个 `hll` HyperLogLog 结构聚合合并的 `HyperLogLog`。