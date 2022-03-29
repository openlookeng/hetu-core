# Release 1.6.0

## Key Features

| Area                  | Feature                                                      |
| --------------------- | ------------------------------------------------------------ |
| Star Tree             | Support update cube command to allow admin to easily update an existing cube when the underlying data changes |
| Bloom Index           | Hindex-Optimize Bloom Index Size-Reduce bloom index size by 10X+ times |
| Task Recovery         | 1. Improve failure detection time: It need take 300s to determine a task is failed and resume after that. Improving this would improve the resume & also the overall query time<br/>2. snapshotting speed & size: When sql execute takes a snapshot, now use direct Java serialization which is slow and also takes more size. Using kryo serialization would reduce size and also increase speed there by increasing the overall throughput |
| Spill to Disk         | 1. Spill to disk speed & size improvement: When spill happens during HashAggregation & GroupBy, the data serialized to disk is slow and also size is more. It can improve the overall performance by reducing size and also improving  the writing speed. Using kryo serialization improves both speed and reduces size<br/>2. Support spilling to hdfs: Currently data can spill to multiple disks, now support spill to hdfs to improve throughput<br/>3. Async spill/unspill: When revocable memory crosses threshold and spill is triggered, it blocks accepting the data from the downstream operators. Accepting this and adding to the existing spill would help to complete the pipeline faster<br/>4. Enable spill for right outer & full join for spilling: It don’t spill the build side data when the join type is right outer or full join as it needs the entire data in memory for lookup. This leads to out of  memory when the data size is more. Instead by enable spill and create a Bloom Filter to identify the data spilled and use it during join with probe side |
| Connector Enhancement | Support data update and delete operator for PostgreSQL and openGauss |

## Known Issues

| Category      | Description                                                  | Gitee issue                                               |
| ------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| Task Recovery | When a snapshot is enabled and a CTAS with transaction is executed, an error is reported in the SQL statement. | [I502KF](https://e.gitee.com/open_lookeng/issues/list?issue=I502KF) |
|               | An error occurs occasionally when snapshot is enabled and exchange.is-timeout-failure-detection-enabled is disabled. | [I4Y3TQ](https://e.gitee.com/open_lookeng/issues/list?issue=I4Y3TQ) |
| Star Tree     | In the memory connector, after the star tree is enabled, data inconsistency occurs during query. | [I4QQUB](https://e.gitee.com/open_lookeng/issues/list?issue=I4QQUB) |
|               | When the reload cube command is executed for 10 different cubes at the same time, some cubes fail to be reloaded. | [I4VSVJ](https://e.gitee.com/open_lookeng/issues/list?issue=I4VSVJ) |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.6.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.6.0/hetu-docs/en)

