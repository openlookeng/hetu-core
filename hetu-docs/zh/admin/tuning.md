
# openLooKeng调优

默认openLooKeng设置对于大多数工作负载应该可以正常工作。如果你的集群面临特定的性能问题，以下信息可能会有帮助。

## 配置属性

参见[属性参考](./properties.md)。

## JVM设置

诊断GC问题时，以下信息可能会有帮助:

``` properties
-XX:+PrintGCApplicationConcurrentTime
-XX:+PrintGCApplicationStoppedTime
-XX:+PrintGCCause
-XX:+PrintGCDateStamps
-XX:+PrintGCTimeStamps
-XX:+PrintGCDetails
-XX:+PrintReferenceGC
-XX:+PrintClassHistogramAfterFullGC
-XX:+PrintClassHistogramBeforeFullGC
-XX:PrintFLSStatistics=2
-XX:+PrintAdaptiveSizePolicy
-XX:+PrintSafepointStatistics
-XX:PrintSafepointStatisticsCount=1
```

## 性能调优说明
下面是一些有助于优化查询执行性能的配置说明。 

### 公共表表达式（CTE） 优化
公共表表达式是查询执行计划中被多次用到的公共子执行计划。
openLooKeng分析引擎评估CTE计划节点的使用情况，以确定CTE重用优化的可行性。这个优化在两个场景下被应用： 

> #### 重用执行管道
> 如果配置 [optimizer.cte-reuse-enabled](./properties.md#optimizercte-reuse-enabled)：一个给定查询里的相同CTE计划节点被安排到stage管道中，并且只有一个CTE节点（生产者）被执行，其他的CTE节点（消费者）复用第一个CTE节点的输出结果。
> 当CTE用于自连接（self join）时，那么CTE管道不可用。
> 管道模式执行减少了重复的数据扫描和数据处理，从而减少了查询的执行时间。
 
> #### 物化和重用
> 如果配置 [enable-cte-result-cache](./properties.md#enable-cte-result-cache)：CTE结果将物化到用户指定的存储位置(由[数据缓存配置](./properties.md#hetuexecutiondata-cacheschema-name)指定)。这里缓存了CTE节点的输出结果，物化成功后，可被后续的查询读取和重用。

### 执行计划优化
* **使用精确的分区**：启用后，除非上游阶段的分区精确地匹配到下游阶段的分区，否则强制对数据重新分区（参考: [精准分区](./properties.md#optimizeruse-exact-partitioning)）。
  
* **自适应聚合**: 该特性可以自适应地进行部分聚合； 该特性受到以下三个配置控制：
> 1) [启用自适应部分聚合](./properties.md#adaptive-partial-aggregationenabled)：启用特性。
> 2) [最小行数阈值](./properties.md#adaptive-partial-aggregationmin-rows)：可能自适应关闭部分聚合的最小处理数据行数。
> 3) [唯一行数比值](./properties.md#adaptive-partial-aggregationunique-rows-ratio-threshold)：可能自适应关闭部分聚合的聚合输出、输入数据行数的比值。

* **join数据独立性假设下的选择率估计**:
> 1) [多子句join下数据独立性因子](./properties.md#optimizerjoin-multi-clause-independence-factor)：多子句连接的选择率估计的数据独立性假设因子。
> 2) [多过滤条件下数据独立性因子](./properties.md#optimizerfilter-conjunction-independence-factor)：多过滤条件的选择率估计的数据独立性假设因子。

* **执行策略**：指定调度器实施的执行策略，配置可参考[指定执行策略](./properties.md#queryexecution-policy)：
> 1. _**all-at-once**_：该策略下调度器启动和处理所有的阶段。
> 2. _**phased**_：该策略下调度器调度遵循阶段间生产者源这样的依赖关系，可以将所有独立的阶段一起调度。
> 3. _**prioritize-utilization**_：该策略下调度器调度除了遵循生产者源这样的阶段依赖关系以外，它还查看动态过滤生产者的依赖路径。
