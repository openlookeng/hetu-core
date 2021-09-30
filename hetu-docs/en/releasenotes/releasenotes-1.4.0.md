# Release 1.4.0

## Key Features

| Area             | Feature                                                      |
| ---------------- | ------------------------------------------------------------ |
| Star Tree        | 1. Update APIs to support creating star tree cube for jdbc connectors<br/>2. Implemented support for Clickhouse connector, support for additional JDBC based connectors will be added in the future<br/>3. Bug fixes |
| Heuristic Index  | 1. Added support for UPDATE INDEX (see docs for details)<br/>2. Added index size, memory usage and disk usage info in SHOW INDEX<br/>3. Add mmap cache support for Bloom Index to reduce memory usage (enabled by default, see docs for details)<br/>4. Dropping a table will now also drop any indexes associated with it<br/>5. Index is now automatically load after creation (enabled by default, see docs for details) |
| Memory Connector | Fixed several important bugs to handle large data sets, and address incorrect results that occasionally occur for specific operators. |
| Task Recovery    | Fixed several important bugs to address data inconsistency issues that occasionally occur during high concurrency, and during worker failures. |
| OLK-on-Yarn      | Support deploying an openLooKeng cluster on yarn. The openLooKeng cluster can be deployed with a single coordinator, and multiple workers. |
| Low Latency      | 1. Optimized the Stats calculation for point queries which don't contain join to speed up low latency queries<br/>2. Added adaptive split grouping to enhance high concurrency query throughput<br/>3. Support non-equality dynamic filters to speed up queries having predicates like <, >, <= & >= |
| Kylin Connector  | Support read operations to the Kylin datasource              |

## Known Issues

| Category      | Description                                                  | Gitee issue                                                  |
| ------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Task Recovery | Concurrent CTAS, some SQL statements report the error "HiveSplitSource is already closed." | [144QYL](https://e.gitee.com/open_lookeng/issues/list?issue=I44QYL) |
|               | An error occurs when the CTA creates a transaction table and inserts data"Unsuccessful query retry" | [I44RQJ](https://e.gitee.com/open_lookeng/issues/list?issue=I44RQJ) |
| OLK-on-Yarn   | Multiple small openLooKeng clusters are started in the Hadoop cluster, but all openLooKeng cluster processes are on the same node. As a result, the openLooKeng cluster fails to be started | [I4BP5A](https://e.gitee.com/open_lookeng/issues/list?issue=I4BP5A) |
|               | When multiple openLooKeng clusters are started, an error is reported when access some cluster CLI | [I4BY6Z](https://e.gitee.com/open_lookeng/issues/list?issue=I4BY6Z) |

## Obtaining the Document 

For details, see [https://gitee.com/openlookeng/hetu-core/tree/1.4.0/hetu-docs/en](https://gitee.com/openlookeng/hetu-core/tree/1.4.0/hetu-docs/en)