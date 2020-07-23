
# EXPLAIN

## 摘要

``` sql
EXPLAIN [ ( option [, ...] ) ] statement

where option can be one of:

    FORMAT { TEXT | GRAPHVIZ | JSON }
    TYPE { LOGICAL | DISTRIBUTED | VALIDATE | IO }
```

## 说明

显示语句的逻辑或分布式执行计划，或者对语句进行验证。使用 `TYPE DISTRIBUTED` 选项可以显示分片计划。每个计划片段由单个或多个 openLooKeng 节点执行。片段之间的间隔表示 openLooKeng 节点之间的数据交换。片段类型指定 openLooKeng 节点如何执行片段以及数据如何在片段之间分布：

`SINGLE`

片段在单个节点上执行。

`HASH`

片段在固定数量的节点上执行，输入数据通过哈希函数进行分布。

`ROUND_ROBIN`

片段在固定数量的节点上执行，输入数据以轮循方式进行分布。

`BROADCAST`

片段在固定数量的节点上执行，输入数据广播到所有节点。

`SOURCE`

片段在访问输入分段的节点上执行。

## 示例

逻辑计划：

``` sql
lk:tiny> EXPLAIN SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                                Query Plan
----------------------------------------------------------------------------------------------------------
 - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
         _col1 := count
     - RemoteExchange[GATHER] => regionkey:bigint, count:bigint
         - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
                count := "count"("count_8")
             - LocalExchange[HASH][$hashvalue] ("regionkey") => regionkey:bigint, count_8:bigint, $hashvalue:bigint
                 - RemoteExchange[REPARTITION][$hashvalue_9] => regionkey:bigint, count_8:bigint, $hashvalue_9:bigint
                     - Project[] => [regionkey:bigint, count_8:bigint, $hashvalue_10:bigint]
                             $hashvalue_10 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("regionkey"), 0))
                         - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                                 count_8 := "count"(*)
                             - TableScan[tpch:tpch:nation:sf0.1, originalConstraint = true] => [regionkey:bigint]
                                     regionkey := tpch:regionkey
```

分布式计划：

``` sql
lk:tiny> EXPLAIN (TYPE DISTRIBUTED) SELECT regionkey, count(*) FROM nation GROUP BY 1;
                                          Query Plan
----------------------------------------------------------------------------------------------
 Fragment 0 [SINGLE]
     Output layout: [regionkey, count]
     Output partitioning: SINGLE []
     - Output[regionkey, _col1] => [regionkey:bigint, count:bigint]
             _col1 := count
         - RemoteSource[1] => [regionkey:bigint, count:bigint]

 Fragment 1 [HASH]
     Output layout: [regionkey, count]
     Output partitioning: SINGLE []
     - Aggregate(FINAL)[regionkey] => [regionkey:bigint, count:bigint]
             count := "count"("count_8")
         - LocalExchange[HASH][$hashvalue] ("regionkey") => regionkey:bigint, count_8:bigint, $hashvalue:bigint
             - RemoteSource[2] => [regionkey:bigint, count_8:bigint, $hashvalue_9:bigint]

 Fragment 2 [SOURCE]
     Output layout: [regionkey, count_8, $hashvalue_10]
     Output partitioning: HASH [regionkey][$hashvalue_10]
     - Project[] => [regionkey:bigint, count_8:bigint, $hashvalue_10:bigint]
             $hashvalue_10 := "combine_hash"(BIGINT '0', COALESCE("$operator$hash_code"("regionkey"), 0))
         - Aggregate(PARTIAL)[regionkey] => [regionkey:bigint, count_8:bigint]
                 count_8 := "count"(*)
             - TableScan[tpch:tpch:nation:sf0.1, originalConstraint = true] => [regionkey:bigint]
                     regionkey := tpch:regionkey
```

验证：

``` sql
lk:tiny> EXPLAIN (TYPE VALIDATE) SELECT regionkey, count(*) FROM nation GROUP BY 1;
 Valid
-------
 true
```

IO：

``` sql
lk:hive> EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_nation SELECT * FROM nation WHERE regionkey = 2;
            Query Plan
-----------------------------------
 {
   "inputTableColumnInfos" : [ {
     "table" : {
       "catalog" : "hive",
       "schemaTable" : {
         "schema" : "tpch",
         "table" : "nation"
       }
     },
     "columns" : [ {
       "columnName" : "regionkey",
       "type" : "bigint",
       "domain" : {
         "nullsAllowed" : false,
         "ranges" : [ {
           "low" : {
             "value" : "2",
             "bound" : "EXACTLY"
           },
           "high" : {
             "value" : "2",
             "bound" : "EXACTLY"
           }
         } ]
       }
     } ]
   } ],
   "outputTable" : {
     "catalog" : "hive",
     "schemaTable" : {
       "schema" : "tpch",
       "table" : "test_nation"
     }
   }
 }
```

## 另请参见

[EXPLAIN ANALYZE](./explain-analyze.md)