
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