# Release 1.3.0

## Key Features

| Area                           | Feature                                                      | PR #s                                                        |
| ------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery                  | Fixed a few important bugs and further improved the stability of this function. It now can work with CTE and spill-to-disk features. | 812,813,837,<br/>838,842,843,<br/>847,863,868,<br/>874,875,885,<br/>889,891,901,<br/>906,917,930,<br/>932 |
| CTE (Common Table Expressions) | Additional optimization on top of 1.2.0 CTE optimization. Added cost based decision to decide whether to enable CTE or not. Added support for pushdown of dynamic filters and predicates into CTE nodes. | 722,811,815,<br/>876,921,927                                 |
| DM (Data Management)           | Further improved the performance of Data Management Operations. Exposed performance tuning parameters as: <br/>- metastore -client-service-threads: Parallelize operations to Hive metastore by using multiple clients to send/receive requests<br/>- metastore -write-bach-size: Reduce round trip to hive metastore by packing multiple operation objects per call | 888                                                          |
| Star Tree Index                | 1. Star Tree Cube now supports up to 10 billion cardinality. <br/>2. openLooKeng CLI updates to improve cube management experience. User now has to issue a single sql statement to both create and populate data in the cube instead of multiple sql statements. The CLI changes help avoid query exceeded cluster memory limit issue.<br/>3. Bug fixes<br/>    a. Merge continuous ranges into single range so cube can be utilized<br/>    b. Count distinct issue: Filter source data during cube insertion | 834,867,890,<br/>902,907                                     |
| CBO                            | Support Sorted Source Aggregator<br/><br/>Added support for sort based aggregator in cases where input source is pre-ordered. This greatly reduces the amount of memory used for hashes and can finalize the majority of the results at the partial aggregation stage itself, thereby reducing the final aggregation load at a higher plan stage.<br/><br/>The openLooKeng optimizer makes choices between Sort Aggregator and Hash Aggregator based on the estimated cost of operation for the given memory. | 855,905,906                                                  |
| Hudi Connector                 | Snapshot queries for Hudi COW table is supported; snapshot queries and read optimized queries for HUDI MOR table are supported. | 881,900                                                      |
| GreenPlum Connector            | Support read and write operations on the GreenPlum datasource. But update and delete operations are not yet supported. | 689                                                          |
| Oracle Connector               | Add new capability to support update and delete operation within Oracle datasource. | 897                                                          |
| ClickHouse Connector           | Support read and write operations to the ClickHouse datasource.<br/><br/>Also add support for SQL query pushdown, and registration & pushdown of external functions. | 920                                                          |
| JDBC Connector                 | Enhance JDBC to support the multiple splits so that it can improve the performance of high concurrency scenarios. | 939                                                          |
| Hive Connector                 | Upgrade the Hive dependency from 3.0.0 to 3.1.2 and fixed the compatibility issue of timestamp caused by the upgrade. | 903                                                          |
| Memory Connector               | Memory Connector Optimizations<br/><br/>- HetuMetaStore integration to persist table info<br/>- New data formation (LogicalParts) to support sorting and indexing<br/>- Predicate pushdown<br/>- Automatic spill-to-disk management | 914                                                          |
| Resource                       | Enhanced resource group to throttle scheduling or kill query based on resource usage and user configurations. | 779,821,822,<br/>836                                         |

## Known Issues

| Category          | Description                                                  | Gitee issue                                                  |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery | An error message: "Unsuccessful query retry", is shown when CTA creates a transaction table and inserts data. | [I3YF45](https://gitee.com/openlookeng/hetu-core/issues/I3YF45) |
|         | A query can hang when there is insufficient memory for a node. | [I3YF4O](https://gitee.com/openlookeng/hetu-core/issues/I3YF4O) |
|               | When an exception is thrown during stage 1, the value is doubled. | [I3YF4V](https://gitee.com/openlookeng/hetu-core/issues/I3YF4V) |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.3.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.3.0/hetu-docs/en)