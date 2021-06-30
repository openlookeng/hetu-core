# Release 1.3.0

## 关键特性

| 分类   | 描述                                                         | PR #s                                                   |
| ------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery                 | Task recovery 功能上没有变化，修复了bug，增强了功能的稳定性。兼容CTE 和spill-to-disk特性。 | 812,813,837,<br/>838,842,843,<br/>847,863,868,<br/>874,875,885,<br/>889,891,901,<br/>906,917,930,<br/>932 |
| CTE (公共表表达式) | 在1.2.0 CTE的基础上进行额外优化。增加了基于成本的决策，以决定是否启用CTE。支持将动态过滤器和谓词下推到CTE节点。 | 722,811,815,<br/>876,921,927                                 |
| DM (数据管理)           | 进一步改进了DM（Data Management）操作的性能。提供性能调优参数： <br/>- metastore -client-service-threads: 通过使用多个客户端发送/接收请求，支持并发操作Hive Metastore。<br/>- metastore -write-bach-size: 打包每次调用包含的多个操作对象，减少Hive metastore之间的数据往来耗时。 | 888                                                          |
| Star Tree 索引                | 1. Star Tree Cube现在支持高达10 Billion基数。 <br/>2. 更新openLooKeng CLI，以改善cube管理体验。用户可以发单个sql语句来创建和填充cube数据集中的数据，而不是多个sql语句。CLI的优化有助于避免查询超出群集内存限制的问题。<br/>3. 问题修复:<br/>    a. 将连续范围合并为单个范围，以便可以利用Cube<br/>    b. Count distinct问题–在Cube插入过程中支持过滤源数据。 | 834,867,890,<br/>902,907                                     |
| CBO                            | 支持排序聚合器（Sorted Source Aggregator）<br/><br/>在输入源为预先排好序的情况下，增加了对排序聚合器的支持。这样相比哈希聚合器能够减少大量内存使用，并可以在部分聚合阶段（partial aggregation stage）确定大部分的计算结果，从而减少下一个计划阶段的最终聚合负载。<br/><br/>openLooKeng优化器会根据给定查询的代价估计值（CBO），在排序聚合器（Sort Aggregator）和哈希聚合器（Hash Aggregator）之间进行选择。 | 855,905,906                                                  |
| Hudi 连接器                 | 支持Hudi COW数据表的快照查询; 支持Hudi MOR数据表的快照查询和读优化查询。 | 881,900                                                      |
| GreenPlum 连接器            | 支持对GreenPlum数据源的基本读和写操作。不支持删除和更新。 | 689                                                          |
| Oracle 连接器               | Oracle连接器支持Update、Delete操作。 | 897                                                          |
| ClickHouse 连接器           | 支持对ClickHouse数据源的基本读和写操作。支持SQL query pushdown 和 external Functions 的注册和下推能力。 | 920                                                          |
| JDBC 连接器                 | 单表查询支持多分片，通过提高并发来提升性能。 | 939                                                          |
| Hive 连接器                 | Hive Connector 的Hive 依赖包从3.0.0升级到3.1.2，并修复升级所带来的timestamp 格式兼容问题。 | 903                                                          |
| Memory 连接器               | 通过hetuMetastore持久化内存表的元数据信息；新的数据布局以支持排序和索引，提高数据查询性能；排序和索引的异步执行；数据下盘管理。 | 914                                                          |
| Resource                       | 增强的资源组，支持根据资源使用情况和用户配置限制来调度或终止查询。 | 779,821,822,<br/>836 
## 已知问题

| 分类   | 描述                                                         | Gitee问题                                                    |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery | 当执行CTAS语句创建事务表并插入数据时，显示错误消息：“Unsuccessful query retry”。 | [I3YF45](https://gitee.com/openlookeng/hetu-core/issues/I3YF45) |
|         | 当节点内存不足时，查询可能会挂起。| [I3YF4O](https://gitee.com/openlookeng/hetu-core/issues/I3YF4O) |
|               | 当开启snapshot，并在执行到stage 1的时候出现异常，会导致计算结果翻倍。 | [I3YF4V](https://gitee.com/openlookeng/hetu-core/issues/I3YF4V) |

## 获取文档

请参考： [https://gitee.com/openlookeng/hetu-core/tree/1.3.0/hetu-docs/zh](https://gitee.com/openlookeng/hetu-core/tree/1.3.0/hetu-docs/zh)