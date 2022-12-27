
# Tuning openLooKeng

The default openLooKeng settings should work well for most workloads. The following information may help you if your cluster is facing a specific performance problem.

## Config Properties

See [Properties Reference](./properties.md).

## JVM Settings


The following can be helpful for diagnosing GC issues:

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

## Performance Tuning Notes:
Below are few notes on configurations which help with optimization and tuning query execution performance. 

### Common Table Expression(CTE) Optimization
Common Table expressions are common sub-plans under the query plan which are used more than ones within the query.
openLooKeng engine assesses the usage of such CTE plan nodes for feasibility of reuse based optimization. This optimization is applied in 2 scenarios: - 

> #### Reuse Execution Pipeline 
> Given the configuration [optimizer.cte-reuse-enabled](./properties.md#optimizercte-reuse-enabled) is enabled: Same CTE nodes in a given query are arranged in the stage pipeline such that only 1 executes and other reuse the output from the first node wiz. deemed as producer and reset as consumers.
> Its important to note that CTE pipeline cannot be used if CTE is used in Self join with the same CTE.
> Pipeline mode execution reduces the duplicate scanning and processing of the operations, thereby reducing the round trip time for the query.
 
> #### Materialize and Reused
> Given the configuration [enable-cte-result-cache](./properties.md#enable-cte-result-cache) is enabled: CTE results are materialized to user's choice of storage(refer [data-cache configuration](./properties.md#hetuexecutiondata-cacheschema-name)). This approach caches the output of the CTE node which is read and reused for subsequent queries after materialization succeeds.

### Plan optimizations
* **Use exact partitioning**: When enabled this forces data repartitioning unless the partitioning of upstream stage matches exactly what downstream stage expects (refer: [exact partitioning](./properties.md#optimizeruse-exact-partitioning)).
  
* **Adaptive aggregations**: This feature engages adaptive partial aggregation; it is governed by following 3 configurations: -
> 1) [Enable Adaptive Partial Aggregation](./properties.md#adaptive-partial-aggregationenabled): enable the feature
> 2) [Minimum rows threshold](./properties.md#adaptive-partial-aggregationmin-rows): Minimum number of processed rows before partial aggregation might be adaptively turned off.
> 3) [Unique rows ratio threshold](./properties.md#adaptive-partial-aggregationunique-rows-ratio-threshold): Ratio between aggregation output and input rows above which partial aggregation might be adaptively turned off.

* **Join Independence assumptions for Selectivity estimates**:
> 1) [Multi-Clause join Independence factor](./properties.md#optimizerjoin-multi-clause-independence-factor): Scales the strength of independence assumption for selectivity estimates of multi-clause joins.
> 2) [Filter Conjunction Independence factor](./properties.md#optimizerfilter-conjunction-independence-factor): Scales the strength of independence assumption for selectivity estimates of the conjunction of multiple filters.

* **Execution policy for better filtering**: Specifies the execution policy enforced by the scheduler. One of following set of execution policies can be configured refer [execution policy specification](./properties.md#queryexecution-policy): -
> 1. _**all-at-once**_: This policy makes available all stages for scheduler to process and start.
> 2. _**phased**_: This policy follows the stage dependency based on the producer source, and schedule all independent stages together.
> 3. _**prioritize-utilization**_: This policy follows the stage dependency in addition to producer source, it also looks at dynamic filters producers for dependent paths.
