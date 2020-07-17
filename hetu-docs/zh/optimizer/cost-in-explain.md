
EXPLAIN成本
===============

在计划期间，与计划的每个节点相关联的开销将根据查询中的表的表统计信息计算。此计算的成本将作为[EXPLAIN](../sql/explain.md)语句的输出的一部分打印出来。

在计划树中，成本信息以`{rows: XX (XX), cpu: XX, memory: XX, network: XX}`格式呈现。`rows`是指每个计划节点在执行过程中期望输出的行数。行数后面括号中的值表示每个计划节点期望输出的数据大小（以字节为单位）。其他参数表示计划节点执行时预计占用的CPU、内存、网络等资源。这些值并不代表任何实际的单位，而是用于比较计划节点之间的相对成本，使优化器能够选择执行查询的最佳计划。如果其中任何一个值未知，则打印`?`。

 

示例：

``` sql
lk:default> EXPLAIN SELECT comment FROM tpch.sf1.nation WHERE nationkey > 3;

- Output[comment] => [[comment]]
        Estimates: {rows: 22 (1.69kB), cpu: 6148.25, memory: 0.00, network: 1734.25}
    - RemoteExchange[GATHER] => [[comment]]
            Estimates: {rows: 22 (1.69kB), cpu: 6148.25, memory: 0.00, network: 1734.25}
        - ScanFilterProject[table = tpch:nation:sf1.0, filterPredicate = ("nationkey" > BIGINT '3')] => [[comment]]
                Estimates: {rows: 25 (1.94kB), cpu: 2207.00, memory: 0.00, network: 0.00}/{rows: 22 (1.69kB), cpu: 4414.00, memory: 0.00, network: 0.00}/{rows: 22 (1.69kB), cpu: 6148.25, memory: 0.00, network: 0.00}
                nationkey := tpch:nationkey
                comment := tpch:comment
```


通常情况下，每个计划节点只打印一份成本。但是，当`Scan`运算符与`Filter`和/或`Project`运算符组合使用时，将打印多个成本结构，每个成本结构对应组合运算符的一个逻辑部分。例如，`ScanFilterProject`运算符将打印三个成本结构，分别对应`Scan`、`Filter`和`Project`部分。

 

除了实际运行时统计数据外，预估成本也在[EXPLAIN ANALYZE](../sql/explain-analyze.md)中打印。
