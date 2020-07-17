
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
