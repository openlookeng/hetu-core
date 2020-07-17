
# EXPLAIN ANALYZE

## 摘要

``` sql
EXPLAIN ANALYZE [VERBOSE] statement
```

## 说明

执行语句并显示语句的分布式执行计划以及每个操作的开销。

`VERBOSE` 选项可提供更详细的信息和具体的统计数据；了解这些信息可能需要了解 openLooKeng 内部细节和实现细节。

**注意**

*统计数据可能不完全准确，尤其是对于快速完成的查询。*

## 示例

在下面的示例中，您可以看到每个阶段消耗的 CPU 时间，以及该阶段中每个计划节点的相对开销。请注意，计划节点的相对开销基于挂钟时间，挂钟时间可能与 CPU 时间相关，也可能与之不相关。对于每个计划节点，您都可以看到一些额外的统计数据（例如每个节点实例的平均输入以及相关计划节点的平均哈希冲突数）。当您希望检测查询的数据异常（偏斜、异常哈希冲突）时，这些统计数据很有用。

``` sql
lk:sf1> EXPLAIN ANALYZE SELECT count(*), clerk FROM orders WHERE orderdate > date '1995-01-01' GROUP BY clerk;

                                          Query Plan
-----------------------------------------------------------------------------------------------
Fragment 1 [HASH]
    Cost: CPU 88.57ms, Input: 4000 rows (148.44kB), Output: 1000 rows (28.32kB)
    Output layout: [count, clerk]
    Output partitioning: SINGLE []
    - Project[] => [count:bigint, clerk:varchar(15)]
            Cost: 26.24%, Input: 1000 rows (37.11kB), Output: 1000 rows (28.32kB), Filtered: 0.00%
            Input avg.: 62.50 lines, Input std.dev.: 14.77%
        - Aggregate(FINAL)[clerk][$hashvalue] => [clerk:varchar(15), $hashvalue:bigint, count:bigint]
                Cost: 16.83%, Output: 1000 rows (37.11kB)
                Input avg.: 250.00 lines, Input std.dev.: 14.77%
                count := "count"("count_8")
            - LocalExchange[HASH][$hashvalue] ("clerk") => clerk:varchar(15), count_8:bigint, $hashvalue:bigint
                    Cost: 47.28%, Output: 4000 rows (148.44kB)
                    Input avg.: 4000.00 lines, Input std.dev.: 0.00%
                - RemoteSource[2] => [clerk:varchar(15), count_8:bigint, $hashvalue_9:bigint]
                        Cost: 9.65%, Output: 4000 rows (148.44kB)
                        Input avg.: 4000.00 lines, Input std.dev.: 0.00%

Fragment 2 [tpch:orders:1500000]
    Cost: CPU 14.00s, Input: 818058 rows (22.62MB), Output: 4000 rows (148.44kB)
    Output layout: [clerk, count_8, $hashvalue_10]
    Output partitioning: HASH [clerk][$hashvalue_10]
    - Aggregate(PARTIAL)[clerk][$hashvalue_10] => [clerk:varchar(15), $hashvalue_10:bigint, count_8:bigint]
            Cost: 4.47%, Output: 4000 rows (148.44kB)
            Input avg.: 204514.50 lines, Input std.dev.: 0.05%
            Collisions avg.: 5701.28 (17569.93% est.), Collisions std.dev.: 1.12%
            count_8 := "count"(*)
        - ScanFilterProject[table = tpch:tpch:orders:sf1.0, originalConstraint = ("orderdate" > "$literal$date"(BIGINT '9131')), filterPredicate = ("orderdate" > "$literal$date"(BIGINT '9131'))] => [cler
                Cost: 95.53%, Input: 1500000 rows (0B), Output: 818058 rows (22.62MB), Filtered: 45.46%
                Input avg.: 375000.00 lines, Input std.dev.: 0.00%
                $hashvalue_10 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("clerk"), 0))
                orderdate := tpch:orderdate
                clerk := tpch:clerk
```

使用 `VERBOSE` 选项后，某些运算符可能会报告额外的信息。例如，窗口函数运算符将输出以下信息：

``` sql
EXPLAIN ANALYZE VERBOSE SELECT count(clerk) OVER() FROM orders WHERE orderdate > date '1995-01-01';

                                          Query Plan
-----------------------------------------------------------------------------------------------
  ...
         - Window[] => [clerk:varchar(15), count:bigint]
                 Cost: {rows: ?, bytes: ?}
                 CPU fraction: 75.93%, Output: 8130 rows (230.24kB)
                 Input avg.: 8130.00 lines, Input std.dev.: 0.00%
                 Active Drivers: [ 1 / 1 ]
                 Index size: std.dev.: 0.00 bytes , 0.00 rows
                 Index count per driver: std.dev.: 0.00
                 Rows per driver: std.dev.: 0.00
                 Size of partition: std.dev.: 0.00
                 count := count("clerk")
 ...
```

## 另请参见

[EXPLAIN](./explain.md)